# Cortex SRE Platform — Architecture Plan (v2)
## Hub-and-Spoke, Kafka-Native, Tiered-Intelligence SRE Automation for OpenShift Virtualization

> Supersedes: sre_operator_whitepaper.md, unified_sre_platform_architecture.md,
> spoke_operator_architecture.md, central_agents_architecture.md,
> agent_lifecycle_and_context_windows.md, rag_domain_knowledge_sourcing.md

---

## CHANGELOG — What Changed From v1 And Why

| # | Change | Reason |
|---|---|---|
| 1 | Added **Tiered Decision Architecture** (Level 0/1/2) | v1 sent every signal through the full Hub→LLM round trip. Bottleneck at scale, wasteful even at 3 regions. |
| 2 | Added **Capability Model** (`SREClusterRegistration.status.capabilities`) | v1 had a single `storageProvider` string. Agents had no reliable way to know if an action was even executable on a given cluster. |
| 3 | Added **Validation phase** to `SRECommand` state machine | v1's `Succeeded` meant "API call didn't error," not "the fix actually worked." |
| 4 | Fixed **maintenance-window / SRERelease interaction bug** | v1 suppressed incidents from Kafka entirely during maintenance windows, which silently broke `SRERelease` well-being checks running concurrently. |
| 5 | Fixed **approval-flow topic inconsistency** | v1's sequence diagram had ServiceNow/Teams producing onto `sre.command-results`, conflating human decisions with operator execution results. |
| 6 | Added **`approvedByVerified`** distinct from spoofable `spec.approvedBy` | Anyone with patch RBAC could previously write any string into the approval field. |
| 7 | Added **FleetCorrelationAgent** | v1 had no cross-cluster pattern detection — N clusters hitting the same bug looked like N independent incidents. |
| 8 | Added **agent-platform observability metrics** | v1 only measured operator health (reconcile latency, Kafka lag), never LLM cost, confidence calibration, or override rate. |
| 9 | Documented **DAG execution as Argo Workflows/Tekton delegation**, not a hand-rolled engine | `SRERemediationPlan`'s sequential/parallel model doesn't need to become a bespoke DAG engine. |
| 10 | Added **Workflow Catalog / Intent layer** as a roadmap item | Reduces LLM free-form action synthesis to catalog selection over time. |
| 11 | Added **Stateful-system DR/backup requirement** as an explicit gate before GA | Neo4j, Postgres, Redis, BoltDB, Kafka all currently undocumented for backup/restore. |
| 12 | Added new `SignalSource` types: `PacketDropSignal`, `MachineConfigInspection`, `RegistryProbe`, `SimulatorQuery`, `PodStatusInspection` | Closed real diagnostic gaps (packet drops, NTP false positives, Nexus tag-typo vs. registry-down, NetworkPolicy denial, app-vs-platform CrashLoop). |
| 13 | Added `MarkAsExpected` / `MarkAsDuplicate` outcomes to `SRECorrelationRule` | v1 could only escalate. No way to encode "this is expected, suppress" (e.g. NTP jitter during live migration). |

Everything else below is the full plan, not a diff — read it standalone.

---

## 1. Philosophy: "Smart Brain, Dumb Hands" — With A Local Reflex Arc

The Spoke Operator executes. The Hub reasons. That principle is unchanged. What's new in v2 is a third layer underneath both:

```
Level 0 — Reflex (Spoke, no Kafka, no LLM, sub-second)
Level 1 — Template Match (Hub, Kafka round-trip, no fresh LLM call)
Level 2 — Full Reasoning (Hub, Kafka round-trip, full OODA + LLM)
```

Most real-world alert volume is Level 0 or Level 1. Reserve Level 2 — the expensive, slow, token-consuming path — for genuinely novel or ambiguous failures. This isn't a scale optimization bolted on later; it's core to the design because it changes what the Spoke Operator is allowed to decide on its own.

### 1.1 Level 0 — Local Reflex (Spoke-only, zero Hub involvement)

Defined per-policy, evaluated entirely inside `SREPolicyReconciler`/`SRECorrelationRuleReconciler` on the spoke. No Kafka `sre.commands` round trip. The action executes, then a result is emitted to Kafka **for visibility only**.

```yaml
# SREPolicy.spec.localAutonomy
localAutonomy:
  enabled: true
  rules:
    - triggerCheck: "PVCCapacityWarning"       # existing check/signal name
      maxRiskLevel: "Low"
      allowedActions: ["ExpandPVC"]
      conditions:
        - "resource.utilizationPct < 90"        # still bounded — never blind expansion
    - triggerCheck: "SingleNodeNotReadyNoImpact"
      maxRiskLevel: "Low"
      allowedActions: ["CordonNode"]
      conditions:
        - "affectedVMCount == 0"
```

### 1.2 Level 1 — Template Match (Hub, no fresh LLM call)

When an incident's signature (triggering alerts + affected resource type + cluster capability fingerprint) cosine-matches an existing `historical_incidents` record at similarity > 0.95 **and** that record has a human rating ≥ 4, `RemediationPlannerAgent` fills the template directly and skips a fresh `RCAAgent` LLM invocation entirely. This is the single largest cost/latency lever in the platform — a cluster that has seen the same NTP-drift-after-migration pattern 40 times this month should not burn an LLM call on the 41st.

### 1.3 Level 2 — Full OODA (unchanged from v1, described in full in §10)

---

## 2. Capability Model (NEW)

Every spoke publishes a structured capability object on each heartbeat cycle — read from the informer cache, zero extra API load, same pattern as the existing heartbeat.

```yaml
# SREClusterRegistration.status.capabilities
status:
  capabilities:
    storage:
      provider: "portworx"          # portworx | dell-csm | ocs | mixed
      liveExpansion: true
      snapshotSupport: true
      replicationFactor: 3
    network:
      cni: "ovn-kubernetes"
      sriov: true
      multus: true
      loadBalancerType: "metallb"
    hypervisor:
      liveMigration: true
      gpuPassthrough: false
      dedicatedCPUSupport: true
    versions:
      ocp: "4.16.8"
      kubevirt: "1.2.1"
      acm: "2.11.2"
      cnv: "4.16.2"
    installedOperators: ["cnv", "portworx-operator", "vault-secrets-operator", "openshift-gitops"]
    gitopsEnabled: true
    lastCapabilityRefresh: "2026-07-18T10:00:00Z"
```

**Enforcement points:**
- `SRECommand` admission webhook rejects any action whose required capability isn't present on the target cluster (e.g. `MigrateVolume` on a cluster reporting `storage.provider: ocs`).
- `SRECorrelationRule.requiresCapability` gates rule evaluation — a Portworx-specific correlation rule never even evaluates on a Dell CSM cluster.
- `RemediationPlannerAgent` filters its candidate action list by capability before invoking the LLM, reducing hallucinated-but-inexecutable proposals.

---

## 3. Hub-Spoke Topology & HA

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

**Note on BoltDB + replica count:** BoltDB's `ReadWriteOnce` PVC can only be mounted by one pod. Do **not** collapse the whole Deployment to `replicas: 1` to satisfy this (that was a real regression risk raised in review — it kills webhook HA). Keep two Deployments:
- `sre-operator-reconciler` — `replicas: 1`, mounts BoltDB PVC, runs all reconcilers + Kafka consumers.
- `sre-operator-webhook` — `replicas: 2+`, stateless, no PVC, no leader election needed, serves ValidatingWebhookConfiguration/MutatingWebhookConfiguration only.

Hub runs the equivalent split for its own reconcilers vs. any hub-side admission surface.

---

## 4. CRD Catalog — Spoke

```
SREGlobalConfig ─────── Kafka/Vault/Splunk bootstrap config
        │
        ▼
    SREPolicy ─────────── Health checks + all SignalSources + localAutonomy rules
        │
        ▼
 SRECorrelationRule ─────── Temporal multi-signal grouping (BoltDB-backed buffer)
        │
        ▼
    SREIncident ────────── Lifecycle state machine (§7)
        │
        ▼
 SRERemediationPlan ────── Sequential/parallel step orchestration (delegates DAG-heavy
        │                   cases to Argo Workflows — see §9)
        ▼
    SRECommand ────────── Discrete action execution + Validation phase (§7)

    SRERelease ─────────── ArgoCD sync tracking + post-release well-being window
```

### 4.1 SREGlobalConfig
Bootstraps Kafka bootstrap servers, Vault role, Splunk HEC, proxy config. Watches its own updates to rotate TLS/Kafka connections in memory without restart.

### 4.2 SREPolicy
Defines `checks[]`, `prometheus.alertTriggers[]`, `signalSources[]` (full taxonomy in §14), `storageChecks`, `vaultChecks`, `errataChecks`, `gitOpsAwareness` (observation only — **never** creates a PR from the operator), `diagnosticCollection`, `remediation` (approval thresholds, maintenance windows), `sloDefinitions[]`, and the new `localAutonomy` block (§1.1).

### 4.3 SRECorrelationRule
Full grammar in §14. Evaluated against the BoltDB-backed `SignalStore` (persistent, crash-resilient, PVC-mounted — see §12.1) via an inverted-index engine (`signalToRules` map) so evaluation is O(signals-matching-this-rule), not O(all-rules) per signal.

### 4.4 SREIncident
Single source of truth for incident lifecycle. State machine in §7. Auto-harvests diagnostics on `Active` transition — **diagnostics collection is independent of notification suppression** (fix #4 — see §7.1).

### 4.4 SREIncident (addendum — diagnostics storage & deduplication)

Diagnostics are never stored inline in `status.diagnostics`. On harvest, the full
bundle is written to a ConfigMap (`incident-diag-<name>`, owned via
`controllerutil.SetControllerReference` for automatic GC), and only
`status.diagnosticsRef` (the ConfigMap name) is stored on the CR. A size-capped
summary (not the full bundle) is embedded directly in the `sre.incidents.lifecycle`
Kafka payload so RCAAgent's Observe phase needs no extra round trip for the common
case; the ConfigMap is the on-demand escalation path for full-detail forensics.

`status.auditTrail` and `status.remediationLog` are bounded ring buffers (last 20
entries) updated via `retry.RetryOnConflict` + re-fetch + `Status().Patch()` — never
a blind `MergeFrom()` against a stale snapshot, since JSON Merge Patch replaces
arrays wholesale rather than appending, and concurrent writers (correlation engine,
SRECommandReconciler, SREIncidentReconciler) can silently clobber each other's
entries otherwise. The durable, unbounded history is Kafka `sre.audit` (90d) —
status fields are a recent-activity cache for `kubectl get`/`describe`, not the
system of record.

**Deduplication Sorting:** To prevent race conditions and non-deterministic behavior during parallel reconciles, the deduplication logic in `SREIncidentReconciler` must explicitly sort matching incidents by `metadata.creationTimestamp` before picking the oldest instance (`existing[0]`) as the merge survivor.

### 4.8 SREJob (addendum — results capacity & routing)

To prevent unbounded status growth on recurring cron schedules, `status.results` is managed as a bounded ring buffer capped at the value of `spec.historyLimit` (updated via `retry.RetryOnConflict` + re-fetch + `Status().Patch()`). The full details of each run are emitted to the `sre.job-results` Kafka topic. Additionally, to route execution feedback without collision risks, jobs are referenced and routed by their unique `namespace/name` rather than the non-unique text `job_label`.

### 4.5 SRERemediationPlan
Orchestrates `SRECommand` steps. For genuinely complex multi-branch/fan-out workflows, this CRD should create an Argo `Workflow` object rather than reimplement DAG semantics — see §9.

### 4.6 SRECommand
Executes one discrete action. Full state machine with the new **Validation** phase in §7.2.

### 4.7 SRERelease
Watches ArgoCD `Application` sync completion. Cross-references `SREErrataCache` for newly-introduced vulnerable versions. Runs a 30-minute post-release well-being window — **now correctly queries incident *existence*, not incident *notification state*** (fix #4).

---

## 5. CRD Catalog — Hub

```
SREClusterRegistration ── One per ACM ManagedCluster. Now includes status.capabilities (§2).
SRECrossClusterRead ────── Read-only fleet inspection (ConfigMaps/RBAC/CRDs). Secret kind hard-blocked.
SREErrataCache ─────────── Daily Red Hat/NVD sync, broadcast to spokes via Kafka.
SREGlobalPolicy ────────── Fleet-wide policy pushed to spokes via ACM ManifestWork.
```

---

## 6. State Machines

### 6.1 SREIncident (with suppression fix)

```
Detecting ──(correlation window elapses)──> Active
                                                │
                          ┌─────────────────────┤
                          │                     │
              [in maintenance window]     [not in window]
                          │                     │
                          ▼                     │
                    Suppressed                  │
              (spec.suppressUntil set)          │
                          │                     │
              ★ FIX: diagnostics STILL          │
              collected here. Only the          │
              paging/notification path is       │
              suppressed — SREIncident.status    │
              still populates fully so           │
              SRERelease well-being checks        │
              and the RCAAgent (once window       │
              ends) have complete data.           │
                          │                     │
              [window ends, still failing]      │
                          └────────────►────────┘
                                                │
                                                ▼
                                          Remediating
                                                │
                          ┌─────────────────────┤
                          ▼                     ▼
                      Resolved            FalsePositive
```

### 6.2 SRECommand (with Validation phase added)

```
Pending → PendingApproval → Approved → Executing
                                            │
                                            ▼
                                       (API call returns)
                                            │
                              ┌─────────────┴─────────────┐
                              ▼                            ▼
                          Succeeded*                     Failed
                              │
                              ▼
                    ★ NEW: Validating
              Poll target resource health for
              spec.validationWindowSeconds
              (default 60s, action-specific override)
                              │
                  ┌───────────┴───────────┐
                  ▼                       ▼
              Healthy                StillDegraded
                  │                       │
                  ▼                       ▼
       Incident may Resolve      Auto-trigger rollback
                                  OR escalate — incident
                                  stays Remediating

*"Succeeded" now means "API call accepted." It is no longer
 sufficient on its own to resolve the parent SREIncident.
 Only Validating→Healthy resolves it.
```

Example: `LiveMigrateVM` returns `Succeeded` the instant the migration API call is accepted. `Validating` then polls `VirtualMachineInstance.status.phase == Running` **and** guest-agent heartbeat for 60s before the incident is allowed to close. This closes the "API succeeded but guest is still hung" gap.

### 6.3 Approval Flow (fixed — no longer conflicts with sre.command-results semantics)

```
SRECommand.status.phase = PendingApproval
        │
        ▼
Human clicks "Approve" in Teams / ServiceNow
        │
        ▼
Admission webhook intercepts the resulting PATCH request directly against
the SRECommand object (via kubectl, Hub API, or a thin approval-service proxy)
        │
        ▼
Webhook captures request.userInfo from the AUTHENTICATED API request itself
        │
        ▼
Sets status.approvedByVerified = request.userInfo.username   (server-derived, non-spoofable)
spec.approvedBy remains a free-text field for display only — NEVER trusted for authorization
        │
        ▼
SRECommandReconciler checks status.approvedByVerified != "", not spec.approvedBy
```

`sre.command-results` remains exclusively operator-produced, representing execution outcomes only. Human decisions never appear on that topic.

---

## 7. GitOps Exclusion Protocol (unchanged from v1 — this part was already correct)

```
Is the action in the runtime-operations allowlist?
  (CordonNode, DrainNode, LiveMigrateVM, ExpandPVC*, transient pod deletion)
    YES → execute directly. GitOps never manages these.
    NO  → is the target resource GitOps-tracked (ArgoCD label / in an Application's
          managed resource list)?
            YES → operator BLOCKS the command (status = Failed, reason = GitOpsManagedExclusion)
                  → RCAAgent generates the exact YAML diff
                  → posted as a copy-pasteable block to ServiceNow + Teams Adaptive Card
                  → SRE merges manually, ArgoCD auto-syncs, well-being window confirms fix
            NO  → execute directly

* ExpandPVC only if storage class supports online expansion without a manifest change.
```

The operator **never** creates a Git commit, branch, or PR. This is a hard boundary, not a policy toggle.

---

## 8. Kafka Topic Registry

| Topic | Key | Producer(s) | Consumer(s) | Retention |
|---|---|---|---|---|
| `sre.telemetry.raw` | `{cluster-id}` | Spoke `SREPolicyReconciler` | `TriageAgent`, `TopologyAgent`, Splunk Connect | 24h |
| `sre.signals.buffer` | `{cluster-id}:{ns}:{signal}` | Spoke (dual-write with BoltDB) | Spoke only (startup replay) | 20m |
| `sre.incidents.lifecycle` | `{fingerprint}` | Spoke `SREIncidentReconciler`, Hub (synthetic `ClusterUnreachable` on heartbeat loss) | `TriageAgent`, `RCAAgent`, `FleetCorrelationAgent` (NEW), `SREClusterRegistrationReconciler`, `PolicyLearnerAgent`, Splunk Connect | 7d, compacted |
| `sre.commands` | `{cluster-id}:{cmd}` | `RemediationPlannerAgent`, `CapacityAgent`, `ErrataCorrelatorAgent`, `PolicyLearnerAgent` | Hub `SREClusterRegistrationReconciler` → creates CR on spoke | 24h |
| `sre.command-results` | `{cmd-name}` | Spoke `SRECommandReconciler` **only** — never ServiceNow/Teams (fix #5) | `RemediationPlannerAgent`, `HumanLoopAgent`, Splunk Connect | 7d |
| `sre.errata.cache` | `{cve-id}` | Hub `SREErrataCacheReconciler` | Spoke consumer, `ErrataCorrelatorAgent`, Splunk Connect | 30d, compacted |
| `sre.crosscluster.reads` | `{cluster}:{kind}:{name}` | Hub `SRECrossClusterReadReconciler` | RAG ingestion pipeline, `TopologyAgent`, Splunk Connect | 1h |
| `sre.audit` | `{cluster-id}` | Spoke reconcilers (all mutations, approvals, bypasses) | `PolicyLearnerAgent`, Splunk Connect (90d compliance) | 90d |
| `sre.dead-letter` | `{topic}:{key}` | Any producer on retry exhaustion | Manual review, DLQ alert | 7d |

### 8.1 Multi-Region Kafka Federation

To support global deployments (e.g., US, EU, APAC) without experiencing WAN latency bottlenecks:
- Regional Kafka clusters are deployed in each geographic region.
- High-frequency, latency-sensitive traffic (like heartbeats and raw telemetry logs) stays entirely local to the region's Kafka cluster.
- Low-frequency orchestration traffic (`sre.commands`, `sre.command-results`, and `sre.incidents.lifecycle`) is federated globally to the central primary Kafka cluster via **Strimzi / Apache Kafka MirrorMaker 2 (MM2)**.
- Hub agents connect directly to the global primary Kafka cluster, allowing centralized control without regional latency overhead.

---

## 9. Execution Model — Sequential Steps vs. True DAGs

Keep `SRERemediationPlan` for the common case (2-5 sequential or parallel steps with simple `dependsOnStep`). For genuinely complex multi-branch recovery workflows (e.g. "Recover OVN": cordon → drain in parallel across 3 nodes → wait-for-quorum → conditionally reboot → validate → close), **do not build a bespoke DAG engine inside the reconciler.** Delegate to Argo Workflows:

```
SRERemediationPlanReconciler:
  if plan.spec.complexity == "simple":
    → orchestrate directly (existing sequential/parallel logic)
  if plan.spec.complexity == "dag":
    → translate plan.spec.steps into an Argo Workflow object
    → create it on the spoke (Argo Workflows already installed alongside ArgoCD in most
      OpenShift GitOps deployments — reuse, don't duplicate)
    → watch Workflow.status, mirror back into SRERemediationPlan.status
```

This keeps your differentiator (correlation + RCA intelligence) as the thing you build, and borrows a battle-tested execution engine for the part that isn't your differentiator.

---

## 10. Agent Registry — Hub

```
                  ┌──────────────────────────────────────────────┐
                  │                 TriageAgent                  │
                  │   Consumes incidents.lifecycle, routes        │
                  └──────────────────────┬───────────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                     ▼
            ┌───────────────┐   ┌────────────────┐   ┌──────────────────┐
            │  Level 1       │   │    RCAAgent    │   │ FleetCorrelation  │
            │  Template      │   │  (Level 2 OODA)│   │      Agent (NEW)  │
            │  Match (NEW)   │   └────────┬───────┘   └──────────────────┘
            └───────┬────────┘            │
                    │                     ▼
                    │           RemediationPlannerAgent
                    │           (capability-filtered actions)
                    └──────────┬──────────┘
                               ▼
                    SRECommand → Validation phase → Resolved
```

### 10.1 TriageAgent
Routes incidents. Checks Level 1 template match (§1.2) before dispatching to `RCAAgent`. Creates baseline ServiceNow ticket for P1.

### 10.2 RCAAgent (Level 2, OODA loop — unchanged core logic)
Observe (Splunk query window `[triggerTime - 15m, now()]` + diagnostics bundle + topology) → Orient (RAG retrieval, pgvector) → Decide (schema-enforced LLM call, **now filtered by capability model**) → Act (emit `SRECommand`, or halt on `insufficient_data`).

### 10.3 RemediationPlannerAgent
Translates RCA conclusion into ordered `SRECommand` steps. **Now filters candidate actions against `SREClusterRegistration.status.capabilities` before proposing them.**

### 10.4 CapacityAgent
Linear-regression forecasting on Prometheus range queries. Proactively issues `ExpandPVC`/scale commands 7 days ahead of exhaustion.

### 10.5 ErrataCorrelatorAgent
Daily RHSA/CVE audit against running kernel/package versions. Schedules patching within maintenance windows.

### 10.6 PolicyLearnerAgent
Mines resolved incidents for MTTR trends and false-positive patterns. Proposes new `SRECorrelationRule`s **including `MarkAsExpected` suppression rules**, always `approvalRequired: true`.

### 10.7 HumanLoopAgent
Manages approval timeouts. On timeout: `SafeAbort` + escalate to P1, never leaves state ambiguous.

### 10.8 FleetCorrelationAgent (NEW)
```python
def detect_fleet_pattern(self):
    # Every 5 minutes, group Active mirrored incidents by
    # (triggering_alerts signature, ocp_version, kubevirt_version, storage_provider)
    groups = group_active_incidents_by_signature()
    for signature, affected in groups.items():
        if len(affected) >= 3:
            primary = affected[0]
            for dup in affected[1:]:
                mark_as_duplicate(dup, primary_ref=primary)   # suppresses N-1 redundant RCA runs
            create_fleet_incident(
                title=f"Fleet pattern: {signature} across {len(affected)} clusters",
                severity="P1Critical" if len(affected) > 10 else "P2High"
            )
```
Directly reduces LLM cost (fewer duplicate `RCAAgent` runs) and gives humans immediate "this is a platform bug, not my cluster" signal.

### 10.9 LangGraph Orchestration State Machine (StateGraph Skeleton)

To prevent lost context on Hub restarts and enable first-class interrupts for human loops, the Central Agent Platform avoids procedural python function-chaining. Instead, it relies on a typed `IncidentState` managed by a LangGraph `StateGraph` with a supervisor node and durable PostgreSQL checkpoints:

```python
from typing import TypedDict, Literal, Optional
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

class IncidentState(TypedDict):
    incident_id: str
    cluster_id: str
    incident_data: dict
    diagnostics: Optional[dict]          # from operator's ConfigMap/Kafka summary
    rag_context: Optional[dict]          # runbooks + historical_incidents + errata
    topology_context: Optional[dict]     # Neo4j blast radius
    rca_decision: Optional[dict]         # enforced schema output
    remediation_plan: Optional[dict]
    command_results: list
    route: Optional[str]                 # supervisor's routing decision
    human_feedback: Optional[dict]

def supervisor_node(state: IncidentState) -> IncidentState:
    """The central router node. Determines the next node based on current state."""
    if state.get("rca_decision") is None:
        state["route"] = "rca_agent"
    elif state["rca_decision"].get("action") == "ESCALATE":
        state["route"] = "human_loop_agent"
    elif state.get("remediation_plan") is None:
        state["route"] = "remediation_planner_agent"
    elif not all(c["phase"] in ("Succeeded", "Failed") for c in state.get("command_results", [])):
        state["route"] = "wait_for_commands"
    else:
        state["route"] = END
    return state

def route_decision(state: IncidentState) -> str:
    return state["route"]

def rca_node(state: IncidentState) -> IncidentState:
    # Observe -> Orient -> Decide. Interacts with Postgres / Neo4j via ToolRegistry
    obs = ToolRegistry.invoke("splunk_tool.query", incident_id=state["incident_id"])
    context = ToolRegistry.invoke("vector_db.search", query=state["incident_data"]["title"])
    state["diagnostics"] = obs
    state["rag_context"] = context
    state["rca_decision"] = decide_cause(obs, context)
    return state

def remediation_planner_node(state: IncidentState) -> IncidentState:
    state["remediation_plan"] = build_plan(state["rca_decision"], state["cluster_id"])
    emit_commands_to_kafka(state["remediation_plan"])
    return state

def human_loop_node(state: IncidentState) -> IncidentState:
    # StateGraph interrupt point. Graph state is persisted to Postgres;
    # Execution halts until Teams/ServiceNow callback triggers resume.
    state["human_feedback"] = None
    return state

# Graph assembly
graph = StateGraph(IncidentState)
graph.add_node("supervisor", supervisor_node)
graph.add_node("rca_agent", rca_node)
graph.add_node("remediation_planner_agent", remediation_planner_node)
graph.add_node("human_loop_agent", human_loop_node)
graph.set_entry_point("supervisor")

graph.add_conditional_edges("supervisor", route_decision, {
    "rca_agent": "rca_agent",
    "remediation_planner_agent": "remediation_planner_agent",
    "human_loop_agent": "human_loop_agent",
    "wait_for_commands": END, # wait for external Kafka trigger to resume
    END: END
})
graph.add_edge("rca_agent", "supervisor")
graph.add_edge("remediation_planner_agent", "supervisor")
graph.add_edge("human_loop_agent", "supervisor")

compiled = graph.compile(checkpointer=PostgresSaver(conn_pool))
```

---

## 11. RAG / Knowledge Architecture (unchanged core, unchanged and correct)

PostgreSQL + pgvector, three tables (`runbooks`, `platform_configs`, `historical_incidents`), HNSW indexes, relational pre-filter + vector similarity. Sources: Red Hat KCS/errata, `openshift/runbooks`, KubeVirt closed-issue mining, internal runbooks, and — highest long-term value — your own `historical_incidents` corpus once populated. Ingestion via `SRECrossClusterRead` events (real-time config) and weekly cron (static docs). No changes recommended here; this section of v1 was already well-designed.

**One addition:** feed `SREClusterRegistration.status.capabilities` into `platform_configs` retrieval filters so the RCAAgent's runbook search is capability-scoped (`WHERE storage_provider = cluster.capabilities.storage.provider`), not just version-scoped as in v1.

---

## 12. Security Model

### 12.1 Signal Buffer Persistence
BoltDB (`go.etcd.io/bbolt`), pure Go, FIPS-compatible, mounted via 1Gi `ReadWriteOnce` PVC on the reconciler-only Deployment (see §3 HA correction). `sync.RWMutex`-protected. Falls back to in-memory no-op with a degraded heartbeat if the PVC fails to mount.

### 12.2 Emergency Bypass (JWT, not boolean)
Short-lived RS256 JWT issued by Hub, validated by a spoke admission webhook. Claims checked: `iss`, `exp` (<5min), `target_cluster`, `action`. Hub public key loaded **once at operator startup** via a watched Secret and cached — never fetched inline inside the webhook handler (that was a real anti-pattern risk flagged in review: don't reintroduce an external call inside an O(1)-budget webhook). Include a `jti` claim plus a 5-minute in-memory used-token set to prevent replay.

### 12.3 Approval Non-Spoofability
`status.approvedByVerified` derived from `request.userInfo` at admission time (§6.3), never trusted from the free-text `spec.approvedBy` field alone.

### 12.4 Allow-Listed Actions Only
No raw shell/kubectl passthrough from any agent. Every action maps to a compiled Go handler behind the `AllowedSREActions` enum, validated at admission.

### 12.5 Cross-Cluster Read Boundaries
`Secret` and `ServiceAccount` kinds hard-blocked at both the webhook and the reconciler (defense in depth). All other read results sanitized for `password`/`token`/`credential` substrings before storage.

### 12.6 local LLM vLLM Sizing Requirement

For air-gapped security compliance, serving `Llama-3-70B-Instruct` locally in FP16 format requires a minimum of **140GB of VRAM**. This necessitates multi-GPU tensor parallelism (typically 2× A100-80GB or equivalent GPU footprint) on the Hub cluster's GPU node pool, which must be factored in during the initial platform infrastructure sizing.

### 12.7 Microsoft Teams Callback Gateway

To enable interactive buttons (`Approve`, `Reject`, `Escalate`) on Microsoft Teams Adaptive Cards, the platform cannot rely on one-way Incoming Webhooks. SRE Teams operations require one of the following interactive callback routes back into the Hub API approval endpoint:
1. **Azure Bot Service + Teams App Manifest:** Registering a Bot Framework messaging endpoint.
2. **Power Automate Gateway:** A flow triggered by card execution ("When a Teams adaptive card action is performed") that parses the callback payload and updates the Hub SRE API.

### 12.8 Tool Registry & MCP Readiness

To future-proof the platform for Model Context Protocol (MCP) server integration, all specialist agent tools (e.g. `prom_tool`, `splunk_tool`, etc.) are wrapped within a unified `ToolRegistry` abstraction layer. Agents never import tools as direct in-process Python modules. Swapping to stdio or HTTP+SSE-based MCP transports later is completed purely by modifying the `ToolRegistry.invoke()` handler.

New query-level tools are introduced to the registry:
* `servicenow_tool.get_ticket_status(ticket_id)` -> Queries active status and assignment groups.
* `maintenance_tool.check_active_window(cluster_id, resource_ref)` -> Checks if the resource is currently in a scheduled maintenance window.
* `suppression_tool.check_suppression(fingerprint)` -> Checks for active local or fleet-wide suppressions.

---

## 13. Observability

### 13.1 Operator Self-Monitoring (already correct in v1)
`sre_check_result`, `sre_incident_total`, `sre_command_duration_seconds`, `sre_kafka_producer_lag`, reconcile latency, webhook latency (p99 budget: 2s, hard timeout 3s).

### 13.2 Agent Platform Observability (NEW — was entirely missing in v1)
```
sre_agent_llm_tokens_total{agent, incident_type}
sre_agent_llm_cost_usd_total{agent}
sre_agent_confidence_vs_outcome{confidence_bucket}     # calibration check — is a stated
                                                        # 90% confidence actually right 90% of the time?
sre_agent_human_override_rate{incident_type}
sre_agent_rollback_frequency{action}
sre_agent_reasoning_latency_seconds{agent, phase}
sre_agent_level0_reflex_rate                            # % of signals resolved without Kafka/LLM
sre_agent_level1_template_match_rate                    # % resolved without a fresh LLM call
sre_fleet_duplicate_suppression_rate                     # FleetCorrelationAgent effectiveness
```

---

## 14. Correlation & Policy Rule Grammar (reference)

### SRECorrelationRule dimensions

| Dimension | Field | Values |
|---|---|---|
| Signal type | `signals[].type` | `Alert`, `Event`, `MetricThreshold`, `LogPattern`, `ClusterOperator`, `NodeCondition`, `NTPCheck`, `DNSCheck`, `NexusPullCheck`, `RBACEvent`, `HardwareEvent`, `VaultCondition`, `PortworxCondition`, `DellCSMCondition`, `ArgoCDSyncEvent`, `ReleaseEvent`, `ErrataMatchSignal`, `PacketDropSignal`, `CapabilityGate` |
| Requirement | `signals[].required` | `true` / `false` |
| Comparison | `signals[].valueExpr` | `>`, `<`, `>=`, `<=`, `==`, `!=`, regex |
| Match count | `minSignalCount` | int |
| Window | `correlationWindow` | duration, capped at buffer TTL (20m) |
| Scope | `scopeConstraint` | `same-node`, `same-namespace`, `same-cluster`, `cross-cluster`, `fleet-wide` |
| Priority | `priority` | int ascending |
| Outcome | `remediationAction` | action name, or `MarkAsExpected` (suppress), or `MarkAsDuplicate` (fleet dedupe) |
| Capability gate | `requiresCapability` | e.g. `storage.provider == portworx` |
| Provenance | `autoGenerated` | `true` forces `approvalRequired: true` |

### SREPolicy.signalSources catalog

```
Existing:      Alert, HealthCheck, KubernetesEvent, PodLog, ClusterOperator,
               NodeCondition, MetricQuery, NTPCheck, DNSCheck, NexusPullCheck,
               RBACEvent, HardwareEvent, VaultCondition, PortworxCondition,
               DellCSMCondition, ArgoCDSyncEvent, ReleaseEvent

New this pass: MachineConfigInspection  — reads rendered chrony.conf etc., not live drift only
               RegistryProbe            — GET /v2/{repo}/manifests/{tag}, distinguishes
                                           typo-vs-registry-down for Nexus pull failures
               SimulatorQuery           — calls NetworkPolicySimulator tool for policy denial
               PodStatusInspection      — classifies CrashLoop as Platform/App/Ambiguous from
                                           exitCode + terminated.reason
```

---

## 15. Operational Maturity Gate Before GA

Do not go to production without backup/restore runbooks for:
- **Neo4j** (topology graph — rebuildable from `sre.crosscluster.reads` replay, but document the procedure)
- **PostgreSQL/pgvector** (runbooks + historical_incidents — this is learned data, not rebuildable, needs real backups)
- **Redis** (working memory — ephemeral by design, but document what breaks if it's lost mid-incident)
- **BoltDB per spoke** (already has a documented degraded fallback — lowest risk of the five)
- **Kafka** (AMQ Streams — standard Strimzi backup patterns, but confirm retention + replication factor covers your actual RPO)

---

## 16. Roadmap Priority

```
1. Fix suppression/well-being interaction (§6.1)              — correctness bug, ship first
2. Fix approval-flow topic inconsistency (§6.3)                 — doc + small code fix
3. Capability Model (§2)                                        — unlocks correct action gating
4. Tiered decision architecture, Level 0 first (§1.1)            — biggest cost/latency win
5. FleetCorrelationAgent (§10.8)                                 — pays for itself in LLM savings
6. Validation phase on SRECommand (§6.2)                         — closes "succeeded but not healthy" gap
7. Agent-platform observability (§13.2)                          — you can't tune what you can't measure
8. Level 1 template-match path (§1.2)                            — depends on historical_incidents volume
9. Argo Workflows delegation for complex plans (§9)               — do when first DAG-shaped need appears
10. Workflow catalog / intent layer                              — lower urgency, do after capability model
11. DR runbooks for stateful systems (§15)                        — required gate before GA, not before dev
```
