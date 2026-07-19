# Spoke Operator Architecture: State, Validation & Execution

The **Spoke Operator** is deployed to every managed Kubernetes / OpenShift Virtualization cluster in the fleet. It acts as the "Dumb Hands" of the architecture, executing commands, collecting telemetry, and grouping alerts locally while remaining stateless and independent of direct central controller REST dependencies.

---

## 1. Technological Stack

* **Core Framework**: Go 1.21+ with `controller-runtime` (v0.16+) and Kubebuilder scaffolding.
* **Local Buffer Database**: BoltDB (`go.etcd.io/bbolt`) — a pure Go, transactional, single-file key-value store (CGO-free and FIPS compliant).
* **Admission Webhooks**: `controller-runtime` webhook server using `cert-manager` for TLS certificate injection.
* **Kafka Consumer Engine**: `segmentio/kafka-go` (pure Go implementation).

---

## 2. Cluster Deployment Topology

The operator split-deployment architecture ensures high availability for validation webhooks while avoiding BoltDB lock contention on the single-mount PVC:

```
                  ┌──────────────────────────────────────────────┐
                  │          Kubernetes API Server (Spoke)        │
                  └──────┬────────────────────────────────┬──────┘
                         │                                │
                 Leader Election                   Webhook Traffic
                         │                                │
                         ▼                                ▼
            ┌────────────────────────┐       ┌────────────────────────┐
            │     Replica 01         │       │     Replica 02          │
            │   (Active Leader)      │       │   (Standby Member)      │
            │ - Spoke Reconcilers    │       │ - Webhook Server ONLY   │
            │ - Kafka Consumers      │       │ - No Informers / Cache  │
            │ - BoltDB PVC Mounted   │       │ - No PVC dependency     │
            └────────────────────────┘       └────────────────────────┘
```

**Note on BoltDB + replica count:** BoltDB's `ReadWriteOnce` PVC can only be mounted by one pod. Do **not** collapse the whole Deployment to `replicas: 1` to satisfy this (as it kills webhook HA). Keep two Deployments:
- `sre-operator-reconciler` — `replicas: 1`, mounts BoltDB PVC, runs all reconcilers + Kafka consumers.
- `sre-operator-webhook` — `replicas: 2+`, stateless, no PVC, no leader election needed, serves ValidatingWebhookConfiguration/MutatingWebhookConfiguration only.

---

## 3. CRD Definition & Reconciler Details

The Spoke Operator registers and manages 7 Custom Resources:

```
            SREGlobalConfig ─────── (Configures credentials / Kafka)
                  │
                  ▼
              SREPolicy ─────────── (Configures health checks / limits)
                  │
                  ▼
         SRECorrelationRule ─────── (Defines temporal grouping of alerts)
                  │
                  ▼
             SREIncident ────────── (Tracks active operational failure state)
                  │
                  ▼
          SRERemediationPlan ────── (Orchestrates a sequence of commands)
                  │
                  ▼
              SRECommand ────────── (Executes discrete actions on the API)

           SREMigrationWave ─────── (Tracks migration wave timelines and window risks)
```

### 3.1 `SREGlobalConfig`
* **Scope**: Cluster-scoped or Single Namespace (`openshift-cnv`).
* **Purpose**: Bootstraps the platform. Configures Kafka bootstrap servers, Vault connection roles, Splunk HEC index details, and proxy parameters.
* **Paradigms**: Listens to update events to dynamically rotate TLS certificates and Kafka connection pools in memory without restarting the operator.

### 3.2 `SREPolicy`
* **Scope**: Namespaced.
* **Purpose**: Coordinates all active checks and defines the **Signal Sources** (e.g., Prometheus metrics, Kubernetes events, Pod log regex patterns, OpenShift ClusterOperators, NTP drifts, CoreDNS error rates, Nexus pull failures, ForkliftPlanCondition, ForkliftMigrationVMStatus, ForkliftHookJobStatus, MigrationPrecheckSignal, WindowsGuestHealthSignal) that stream into the BoltDB signal buffer.
* **Reconciler Flow**: 
  - Tracks configured `SignalSources` in parallel.
  - Tails logs, filters cluster events, queries metric endpoints, and watches Forklift (MTV) CRDs and migration event streams.
  - Converts matched conditions (e.g., matching a forklift image conversion failure or VM guest BSOD signature) into structured `Signal` objects.
  - Writes signals to the BoltDB buffer and publishes them to `sre.telemetry.raw`.
  - **Capability Matrix**: On every heartbeat cycle, the operator publishes a `status.capabilities` object (detailing local CNI, Storage Providers, Hypervisor versions, and migration state details like vCenter connection and VDDK checks) to `sre.telemetry.raw`, ensuring the Hub LLMs have accurate, up-to-date execution boundaries.

### 3.3 `SRECorrelationRule`
* **Scope**: Namespaced.
* **Purpose**: Instructs the local engine on how to correlate transient alerts within a sliding temporal window, and determines if the incident requires Hub escalation or local execution.
* **Reconciler Flow**: Evaluates incoming alerts stored in BoltDB. If the required signals match the rule schema within the specified `timeWindow`, it evaluates execution tiers before emitting to Kafka:
  - **Level 0 (Local Execution)**: If the rule specifies a deterministic, allow-listed action (e.g., `ExpandPVC`) with `autoApprove: true`, the operator triggers `SRECommand` locally and emits only the result to Kafka. The LLM is bypassed entirely.
  - **Level 1 (Historical Match)**: If the alert closely matches a known historical template, it fills the runbook parameters and executes locally.
  - **Level 2 (Hub Escalation)**: For novel or ambiguous events, it transitions to creating an `SREIncident` object and escalates to the Hub.

### 3.4 `SREIncident`
* **Scope**: Namespaced.
* **Purpose**: Represents the single source of truth for an operational incident's lifecycle on the spoke.
* **Reconciler Flow**: 
  - Deduplicates matching incident fingerprints to avoid ticket storms.
  - **Diagnostic Auto-Collection**: When the incident transitions to `Active`, the `SREIncidentReconciler` automatically harvests a diagnostic bundle. Diagnostics are never stored inline in `status.diagnostics` to prevent etcd size limits from being breached. Instead, the full bundle is written to a ConfigMap (`incident-diag-<name>`, owned via `controllerutil.SetControllerReference` for automatic GC), and only `status.diagnosticsRef` (the ConfigMap name) is stored on the CR. The auto-harvest collects:
    1. **Live State**: Crawls VM/Node statuses, pod phases, and controller conditions.
    2. **Events**: Collects active Kubernetes Events in the bounded window `[creationTimestamp - 10m, now()]`.
    3. **Log Excerpts**: Fetches the last 100 stdout/stderr log lines of launcher and agent containers, filtering for matched regex patterns.
    4. **Metrics Snapshot**: Captures current Prometheus/Thanos metric coordinates (e.g., `px_volume_replication_status`, CPU utilization).
    5. **Release Context**: Cross-references against `SRERelease` to identify if the incident occurred within the 30-minute well-being window of a recent ArgoCD application sync.
  - Emits the incident lifecycle event (with a size-capped summary embedded directly inline and the `diagnosticsRef` referencing the ConfigMap) to `sre.incidents.lifecycle` to kick off agent reasoning.
  - SRECommands are reserved strictly for extended/on-demand diagnostics (e.g., broad Splunk log queries).

### 3.5 `SRERemediationPlan`
* **Scope**: Namespaced.
* **Purpose**: Translates the remediation workflow proposed by the central planner into a series of steps.
* **Reconciler Flow**: Orchestrates execution steps sequentially or in parallel. If a step fails, it halts execution and runs the specified rollback commands.

### 3.6 `SRECommand`
* **Scope**: Namespaced.
* **Purpose**: Validates, reviews, executes raw API operations, and verifies post-execution health.
* **Reconciler Flow**: Computes the blast-radius score. If the action exceeds the auto-approval threshold defined in `SREPolicy`, it halts and requests human validation. Upon execution, it enters a **Validating** state, polling the target resource health for `validationWindowSeconds`. It only marks the command as `Resolved` once health is confirmed; otherwise, it triggers rollback or escalation.

### 3.7 `SRERelease`
* **Scope**: Namespaced.
* **Purpose**: Tracks ArgoCD Sync deployment events (mapping to redhat-cop standards configurations) and runs post-deployment health check loops.
* **Reconciler Flow**:
  - Watches ArgoCD `Application` resources for sync status changes (drift detection, sync progress, and completion).
  - If the application state transitions to `Degraded` or `OutOfSync`, the reconciler harvests the drift configuration, namespace events, and operator logs, raising an `SREIncident` with the root cause evidence.
  - Supports executing `HardRefreshArgoCDApp` commands via `SRECommand` to clear transient stuck syncs or validation hooks.
  - On sync completion, creates/updates the `SRERelease` CR, capturing the Git tag, commit SHA, Helm versions, and affected namespaces.
  - Automatically queries the `SREErrataCache` to check if any newly introduced package versions contain active critical/warning CVEs.
  - Begins a 30-minute post-release well-being check. The reconciler queries for the *existence* of any `SREIncident` (including `Suppressed` ones) in the affected namespaces during this window. If found, it tags the incident with the release context and updates `status.phase = Degraded`.
  - Publishes a `ReleaseEvent` signal to `sre.signals.buffer`.

### 3.8 `SREMigrationWave`
* **Scope**: Namespaced.
* **Purpose**: Tracks VMware-to-OpenShift migration waves, execution statuses, and maintenance window risks.
* **Reconciler Flow**:
  - Listens to native forklift operator resources (`Plan` and `Migration` CRDs) on the spoke cluster.
  - Monitors per-VM migration status, hook job runs, and throughput metrics.
  - Dynamically extrapolates `projectedCompletionAt`.
  - If the projected completion time pushes past the scheduled `windowEnd`, sets `status.windowRisk = AtRisk` to trigger proactive alerts.

---

## 4. Local Persistence & Cache Hygiene

To prevent data loss during pod restarts, the local correlator does not rely on transient in-memory maps.

### 4.1 BoltDB SignalStore
* **Hygiene**: The leader reconciler mounts a `1Gi` persistent volume. All incoming alerts (from local checks or the Kafka alert consumer) are dual-written to `sre.signals.buffer` and the local BoltDB bucket.
* **Pruning**: A background goroutine runs every 5 minutes (`EvictExpired`), deleting signals older than 20 minutes to keep the BoltDB database under `50MB` and prevent filesystem bloat.
* **Fault Tolerance**: If the PVC fails to mount, the operator falls back to an in-memory no-op structure and writes a `SignalStoreFailed` alert to the central telemetry.

### 4.2 Cache-First Reads
* The operator forbids direct API calls for reads (`client.Get` or `client.List` only read from Informer caches).
* Custom indexes (e.g., matching parent incidents to active `SRECommands`) are registered via the Field Indexer during manager startup to ensure `O(1)` query efficiency and protect the Kubernetes API server from performance degradation.

---

## 5. Webhook Validation & Emergency Bypass

To maintain safety constraints without slowing down critical mitigations, the operator runs admission webhooks:

### 5.1 Validating Webhook (JWT Authorization)
* When the central planner triggers an emergency bypass (e.g., to quickly cordon a node during an active P1 incident without waiting for GitOps sync or human approval), it bakes an `emergencyBypassToken` into the `SRECommand` spec.
* The spoke validation webhook intercepts this:
  1. It reads the local cryptographic secret containing the Hub's public key (synchronized via ACM policies).
  2. It validates the RS256 signature of the JWT.
  3. It verifies claims: `iss == "sre-hub-authority"`, `target_cluster == local_cluster`, `action == command_spec.action`, and checks that the expiration timestamp has not passed (typically <5 minutes).
* If validation succeeds, the bypass is allowed, and a high-priority audit log is emitted to `sre.audit`.

### 5.2 Mutating Webhook (Defaulting & Idempotency)
* Ensures all created resources have default values automatically injected (e.g., default timeout constraints, retry budgets, and tracing tags).
* Enforces strict idempotency on modifications to avoid append loops on finalizers or labels during duplicate admission reviews.

---

## 6. Maintenance Windows & Alert Suppression

* **Definition**: Spoke clusters reference maintenance window intervals in their local configuration or the ACM cluster registration metadata.
* **Evaluation**: In the `Detecting` phase of `SREIncidentReconciler`, the operator checks the current time against `spec.maintenanceWindows`.
* **Behavior**: If the timestamp falls within a window:
  - The incident is successfully created, and diagnostic auto-collection proceeds normally.
  - The incident phase is immediately set to `Suppressed`.
  - `spec.suppressUntil` is set to the end of the window.
  - The incident IS sent to Kafka (so it is visible for release well-being checks and Hub reporting), but the `Suppressed` phase explicitly prevents downstream ServiceNow incident creation and paging alerts.
  - A background loop transitions the incident back to `Active` (enabling paging) if the failure persists after the window expires.
