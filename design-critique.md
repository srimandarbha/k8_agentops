# SRE Operator — Design Critique & Issue Backlog

Issues are grouped by severity. Work through them top-down.
Mark each issue `[x]` when resolved and note the fix applied.

---

## 🔴 CRITICAL — Will break in production

### Issue 1: Signal Buffer is In-Memory (Not Crash-Resilient)
- **Status:** `[x]`
- **Location:** `SRECorrelationRuleReconciler` (line 1285), `SREPolicyReconciler` (line 744)
- **Problem:** The signal ring buffer is an in-memory map with a 15-min TTL. If the operator pod restarts mid-outage (OOM kill during node pressure is the most common scenario), all buffered signals are lost and the incident never gets created. The Executive Summary explicitly called for SQLite/RocksDB persistent buffering, but the pseudo-code was never updated to reflect this.
- **Fix Applied:** Replaced `getOrCreateSignalBuffer()` with a `SignalStore` backed by BoltDB (`go.etcd.io/bbolt`) — a pure Go, CGO-free, FIPS-compatible embedded key-value store (same engine as etcd). Operator Deployment now mounts a 1Gi `ReadWriteOnce` PVC. Store is injected into both `SREPolicyReconciler` and `SRECorrelationRuleReconciler` at startup. Protected by `sync.RWMutex` for goroutine safety. Falls back to in-memory mode with a degraded heartbeat if the PVC is unavailable. Full implementation in **Part 14** of sre-agent-operator.txt.

---

### Issue 2: Stray `alertAPI.POST` Still Inside the Operator
- **Status:** `[x]`
- **Location:** `SRECommandReconciler` execution switch, `case NotifyAlertAPI` (line 1232)
- **Problem:** `case NotifyAlertAPI: alertAPI.POST(buildAlertFromParameters(spec.parameters))` — this is an allowed `SREAction` that still calls the Alert API directly from inside the operator. Contradicts the architectural principle that the Operator never calls the Alert API.
- **Fix Applied:** Removed the `case NotifyAlertAPI` block from the operator action switch entirely. Added an explicit guard to the `AllowedSREActions` check: `if spec.action not in AllowedSREActions or spec.action == "NotifyAlertAPI"` — so even if the action name arrives in a `SRECommand`, it is immediately rejected with a `Failed` status. Comment added explaining that `alert_api.post()` is exclusively the Central Agent's responsibility.

---

### Issue 3: Hub Has No Kafka Emit on Cluster Disconnect
- **Status:** `[x]`
- **Location:** `SREClusterRegistrationReconciler` heartbeat monitoring (line 1552)
- **Problem:** When the hub detects `lastSeen > 3x heartbeatIntervalSecs`, it sets `connected = false` with a comment `// (Cluster disconnect alert handled by Central Agent via Kafka)`. But no Kafka message is actually produced. The Central Agent has nothing to consume.
- **Fix Applied:** Added full Kafka emit to `sre.incidents.lifecycle` when the hub detects a missed heartbeat. Emits a synthetic `ClusterUnreachable` incident with `current_phase: Active` on the `connected → disconnected` transition only (guarded by `wasConnected` to prevent flooding). Also emits a `Resolved` phase when the cluster reconnects. Hub is now listed as a producer for `sre.incidents.lifecycle` in both topic tables. `ClusterUnreachable` type added to the TriageAgent routing map so the agent picks it up with zero new wiring.

---

### Issue 4: One Kafka Consumer Group Per Command in `RemediationPlannerAgent`
- **Status:** `[x]`
- **Location:** `RemediationPlannerAgent._monitor_command_result()` (line 2114)
- **Problem:** `group_id=f"agent-monitor-{command_name}"` — creates a new consumer group per command. On a 500-cluster fleet with 100 concurrent incidents each spawning 5 commands, this generates 500 live consumer groups simultaneously. Kafka consumer groups are expensive broker-side state. This will eventually overwhelm the broker.
- **Fix Applied:** Replaced `_monitor_command_result` with two methods: a background daemon `_central_result_consumer` that uses a single shared consumer group (`agent-remediation-monitor`) to consume all command results, and `_wait_for_command_result` which blocks on a Redis pub/sub channel. The central daemon dispatches each incoming Kafka message to the correct waiting thread via Redis pub/sub (`cmd-result:<command_name>`). This limits the agent to exactly 1 consumer group regardless of concurrent command volume.

---

## 🟠 SERIOUS GAPS — Not thought through fully

### Issue 5: `EmergencyBypass` is a Plain Boolean, Not Cryptographically Verified
- **Status:** `[x]`
- **Location:** `SRECommand` CRD (line 450), Reconciler Checks (line 1030)
- **Problem:** The design claims "Cryptographic validation via Central Agent signature" for emergency bypasses, but the CRD just uses `emergencyBypass: true`. Anyone with RBAC to create the CR can set this boolean to `true` and bypass GitOps redirection for high-risk actions.
- **Fix Applied:** Replaced `emergencyBypass` boolean with `emergencyBypassToken` across the CRD, Kafka schemas, and Reconciler checks. Added `PART 15: EMERGENCY BYPASS VALIDATING WEBHOOK` which introduces a spoke-side Admission Webhook that intercepts `SRECommand` creation. If a token is provided, the webhook decodes and validates the RS256 signature against the Hub's public key, and verifies claims (`exp` expiration, `target_cluster`, `action`). The `RemediationPlannerAgent` now generates this JWT instead of setting a boolean.

---

### Issue 6: CrossClusterRead Routing Split-Brain
- **Status:** `[x]`
- **Location:** `TopologyAgent._full_refresh_all_clusters` (line 3173) and `Diagnostic` mode validations (line 2619).
- **Problem:** The `TopologyAgent` produces a `CrossClusterRead` command to `sre.commands`, which flows to the spoke operator. But there is also an `SRECrossClusterRead` CRD managed strictly by the Hub via ACM `ManagedClusterView` (Part 6). Both the Hub and Spoke Operator think they own cross-cluster reads. This is a massive split-brain.
- **Fix Applied:** Removed `CrossClusterRead` from the operator entirely (`DiagnosticOnlyActions`, all Secret checks removed since they are no longer handled by the spoke). The operator now rejects `CrossClusterRead` as an unknown action. Updated `TopologyAgent` to bypass Kafka and create `SRECrossClusterRead` CRs directly on the Hub API, delegating read-only discovery strictly to the Hub.

---

### Issue 7: `SREDriftReportReconciler` Directly Mutates Incident Phase
- **Status:** `[x]`
- **Location:** `SREDriftReportReconciler` Reconcile (line 1428)
- **Problem:** `incident.status.phase = Resolved` — this bypasses the `SREIncidentReconciler` lifecycle entirely. No MTTR is calculated, no audit entry written, no Kafka lifecycle event emitted.
- **Fix Applied:** Removed the direct mutation from `SREDriftReportReconciler`. Instead, when the drift report creates the incident, it adds `resolveWhenDriftClears = true` to the incident spec. The `SREIncidentReconciler` now detects this flag during the `Active` phase, checks the `SREDriftReport`, and if `driftDetected == false`, handles the full `Resolved` transition natively (MTTR calculation, Kafka event emission, and Audit logging).

---

### Issue 8: `CapacityAgent` Listed in Registry But Has No Implementation
- **Status:** `[x]`
- **Location:** Agent Registry (line 3937)
- **Problem:** The agent registry lists a `CapacityAgent` running every 30 minutes. No pseudo-code, no design, no Kafka schema defined for it anywhere in the document.
- **Fix Applied:** Designed the `CapacityAgent` as a cron-driven hub agent. It aggregates node/PVC capacity from `sre.telemetry.raw` and intelligently emits commands to `sre.commands` to automatically expand PVCs or provision new nodes before exhaustion hits 100%.

---

## 🟡 DESIGN WEAKNESSES — Will cause operational pain

### Issue 9: `SREJobReconciler` Creates One Consumer Group Per Job Run
- **Status:** `[x]`
- **Location:** `SREJobReconciler.monitorJobCommandResults()` (line 2804)
- **Problem:** `groupID="srejob-monitor-"+jobName` — same Kafka consumer group explosion as Issue 4. A nightly snapshot job across 500 clusters spawns 500 consumer group registrations.
- **Fix Applied:** Implemented a single shared consumer group (`srejob-monitor-group`) for the operator. Job results are read by a background daemon and routed via an in-memory channel to the correct reconciler loop.

---

### Issue 10: Deduplication Fingerprint is Not Canonical
- **Status:** `[x]`
- **Location:** `createSREIncidentFromRule()` (line 849)
- **Problem:** If the same VM appears as `db-vm-01` in one signal and `production-vms/db-vm-01` in another, the fingerprint hash differs and a duplicate incident is created.
- **Fix Applied:** Updated `SREPolicyReconciler` and `SRECorrelationRuleReconciler` pseudo-code to explicitly enforce a `namespace/name` canonical format for resource references before the SHA256 fingerprint is computed.

---

### Issue 11: Signal Buffer Has No Concurrency Protection
- **Status:** `[x]`
- **Location:** `SRECorrelationRuleReconciler.EvaluateAllRules()` (line 1294)
- **Problem:** Controller-runtime runs multiple reconciler goroutines concurrently. Both `SREPolicyReconciler` and the correlation engine read/write the shared signal buffer without any mutex or channel protection. This causes race conditions and duplicate incident creation under load.
- **Fix Applied:** Implemented the `CorrelationEngine` with an inverted index (`signalToRules`) and time-bucketed `SignalRingBuffer`. The entire buffer interaction is now protected by a `sync.RWMutex`, ensuring safe concurrent reads/writes and O(1) rule evaluation.

---

### Issue 12: No Fallback Path When Human Approval Times Out
- **Status:** `[x]`
- **Location:** `HumanLoopAgent._watch_for_approval()` (line 2410-2415)
- **Problem:** When a command times out in `PendingApproval`, the incident stays in `Remediating` indefinitely. No fallback path is defined.
- **Fix Applied:** When `TimedOut`, the agent now emits a `SafeAbort` command to clean up the state and explicitly escalates the incident severity to P1 via the ServiceNow webhook, paging a human immediately.

---

### Issue 13: Knowledge Graph Has No Stale Node Eviction
- **Status:** `[x]`
- **Location:** `TopologyAgent._upsert_graph_node()` (line 3020)
- **Problem:** Deleted VMs, Nodes, and PVCs are never removed from the graph. `MERGE` only creates or updates. Ghost nodes accumulate and corrupt blast-radius calculations.
- **Fix Applied:** Added a `last_seen` timestamp during the `MERGE` operation. Introduced a cleanup cron job inside the `TopologyAgent` to execute `MATCH (n) WHERE n.last_seen < datetime() - duration({minutes: 30}) DETACH DELETE n`.

---

## 🔵 MISSING PIECES — Not designed at all

### Issue 14: No Schema Versioning in Kafka Message Payloads
- **Status:** `[x]`
- **Location:** All Kafka topic schemas (Topics 1–8)
- **Problem:** No `schema_version` field in the `data` payload. When the schema evolves, older consumers reading from Kafka's 7-day retention window will fail to parse new messages.
- **Fix Applied:** Adopted the CloudEvents specification for all Kafka messages. Every payload now includes `specversion: "1.0"`, providing native schema versioning and standardized metadata (type, source, id, time).

---

### Issue 15: No Mechanism to Enforce Maintenance Window Suppression
- **Status:** `[x]`
- **Location:** `SREIncident` `Suppressed` phase, `SREClusterRegistration.spec.maintenanceWindows`
- **Problem:** The `Suppressed` phase exists but nothing sets `spec.suppressUntil` based on the maintenance windows in `SREClusterRegistration`. The suppression logic exists but is never triggered.
- **Fix Applied:** Updated the `SREIncidentReconciler` inside the `Detecting` phase. It now checks the maintenance window from the cluster config. If active, it transitions directly to `Suppressed` and sets `spec.suppressUntil`.

---

### Issue 16: No Multi-Region Kafka Federation Design
- **Status:** `[x]`
- **Location:** Architecture-wide assumption
- **Problem:** The document assumes a single Kafka cluster. On a 500-cluster fleet spanning US, EU, APAC, a single cluster creates WAN latency bottlenecks (150–200ms from APAC), defeating the sub-second Dead Man's Switch design.
- **Fix Applied:** Defined a regional Kafka federation strategy. A dedicated AMQ Streams (Strimzi) cluster runs in each major region. Global topics (`sre.incidents.lifecycle`, `sre.commands`, `sre.errata.cache`) are synchronized across regions using Confluent MirrorMaker 2 (MM2), while high-throughput telemetry remains strictly regional.

---

### Issue 17: No Admission Webhooks for SRE Operator's Own CRDs
- **Status:** `[x]`
- **Location:** Architecture-wide
- **Problem:** Admission webhooks exist for `VirtualMachine` enforcement but none for the operator's own CRDs. A malformed `SRECorrelationRule` (zero signals) crashes the correlator. An `SREJob` with invalid cron panics the scheduler.
- **Fix Applied:** Documented `ValidatingWebhookConfiguration` implementations for `SRECommand` (block bypass without valid signed JWT), `SRECorrelationRule` (reject empty `spec.signals`), and `SREJob` (validate cron expression syntax at admission).

---

## Resolution Log

| Issue | Severity | Status | Fix Applied | Date |
|-------|----------|--------|-------------|------|
| 1 — In-memory signal buffer | 🔴 Critical | `[x]` | BoltDB on PVC via Part 14 | 2026-07-16 |
| 2 — Stray alertAPI.POST | 🔴 Critical | `[x]` | Removed case + blocked in AllowedSREActions guard | 2026-07-16 |
| 3 — No Kafka emit on disconnect | 🔴 Critical | `[x]` | Synthetic ClusterUnreachable incident emitted to sre.incidents.lifecycle | 2026-07-16 |
| 4 — Consumer group per command | 🔴 Critical | `[x]` | Central daemon consumer + Redis pub/sub dispatch | 2026-07-16 |
| 5 — EmergencyBypass not cryptographic | 🟠 Serious | `[x]` | Replaced with emergencyBypassToken + Admission Webhook (Part 15) | 2026-07-16 |
| 6 — CrossClusterRead routing split | 🟠 Serious | `[x]` | Removed from Operator; TopologyAgent uses Hub API CR | 2026-07-16 |
| 7 — DriftReport direct phase mutation | 🟠 Serious | `[x]` | Delegated to SREIncidentReconciler via resolveWhenDriftClears | 2026-07-16 |
| 8 — CapacityAgent not designed | 🟠 Serious | `[x]` | CapacityAgent cron and topic logic added | 2026-07-17 |
| 9 — Consumer group per job | 🟡 Weakness | `[x]` | Shared consumer group with in-memory dispatch | 2026-07-17 |
| 10 — Non-canonical fingerprint | 🟡 Weakness | `[x]` | Enforced canonical resource ref before hash | 2026-07-17 |
| 11 — Signal buffer race condition | 🟡 Weakness | `[x]` | sync.RWMutex + Inverted Index Engine | 2026-07-17 |
| 12 — No timeout fallback path | 🟡 Weakness | `[x]` | Fallback emits SafeAbort and escalates to P1 | 2026-07-17 |
| 13 — Graph stale node eviction | 🟡 Weakness | `[x]` | MERGE with last_seen + Periodic cleanup cron | 2026-07-17 |
| 14 — No Kafka schema versioning | 🔵 Missing | `[x]` | CloudEvents specversion="1.0" adopted | 2026-07-17 |
| 15 — Maintenance window suppression | 🔵 Missing | `[x]` | Logic added to SREIncidentReconciler detecting phase | 2026-07-17 |
| 16 — Multi-region Kafka federation | 🔵 Missing | `[x]` | Regional Strimzi + MirrorMaker 2 | 2026-07-17 |
| 17 — No webhooks for SRE CRDs | 🔵 Missing | `[x]` | Webhook validation rules implemented | 2026-07-17 |
