# SRE Agent Platform: End-to-End Lifecycle & Context Windows

This document maps out the precise operational lifecycle of an alert from the moment a correlation rule fires to the resolution phase, detailing the exact context windows, inputs, outputs, tool permissions, and behavior profiles of each central agent.

---

## 1. Step-by-Step Operational Lifecycle

```
[Spoke: Correlation Fires] ──> [SREIncident Created] ──> [State Harvested] ──> [Lifecycle Event to Kafka]
                                                                                      │
[Teams Card & SNOW Ticket] <── [TriageAgent Routes] <── [Hub: Ingested] <─────────────┘
          │
          ▼
   [RCAAgent OODA] ───────────> [High Confidence?]
          │                              │
          │                              ├──> Yes ──> [RemediationPlanner] ──> [Exec Plan]
          │                              │
          └───────────────────────────> No ───> [Escalation to Teams/SNOW]
```

### Step 1: Spoke Correlation Matches
1. The spoke `SRECorrelationRuleReconciler` detects a match in the BoltDB `SignalStore`.
2. It creates an `SREIncident` Custom Resource (CR) with a unique SHA256 fingerprint.

### Step 2: Live Diagnostic State Harvesting
The `SREIncidentReconciler` intercepts the new CR. Before emitting any events, it harvests:
- The last 50 error-filtered stdout/stderr lines of the target Pod: `[triggerTime - 10m, now()]`.
- Active Kubernetes Event logs (warning/critical messages) for the target resource UIDs: `[triggerTime - 10m, now()]`.
- Live Pod statuses and status conditions.

### Step 3: Lifecycle Event Published
The operator publishes a CloudEvent payload to the `sre.incidents.lifecycle` topic.

---

## 2. Inbound Lifecycle Event Payload Schema

This payload is consumed by the Hub, serving as the baseline context for all central operations.

```json
{
  "specversion": "1.0",
  "type": "io.sre.kubevirt.incident.phase-changed",
  "source": "sre-operator/prod-east-01",
  "id": "inc-uuid-9876",
  "time": "2026-07-17T23:00:00Z",
  "data": {
    "incident_id": "sre-incident-storage-deg-20260717",
    "cluster_id": "prod-east-01",
    "region": "us-east-1",
    "finger_print": "sha256:d8b2e3...",
    "previous_phase": "Detecting",
    "current_phase": "Active",
    "severity": "P1Critical",
    "type": "StorageDegradation",
    "title": "KubeVirt VM evicted due to Portworx degradation",
    "affected_resources": [
      { "kind": "VirtualMachine", "name": "db-vm-01", "namespace": "production-vms" },
      { "kind": "Node", "name": "worker-04", "namespace": "" }
    ],
    "triggering_alerts": ["KubeVirtVMEvicted", "PortworxVolumeReplicationDegraded"],
    "correlation_rule_ref": "vm-eviction-plus-portworx-replication",
    "observed_pod_state": {
      "virt-launcher-db-vm-01": "Failed (ContainerEvicted) | Ready=False"
    },
    "log_excerpt_summary": {
      "virt-launcher-db-vm-01": "[WARN] guest agent heartbeat missed\n[ERROR] I/O write error on dev/vda\n[FATAL] launcher process killed: evict signal received"
    },
    "observed_events": [
      "2026-07-17T22:58:10Z: Pod/virt-launcher-db-vm-01 evicted due to Node worker-04 disk pressure"
    ],
    "diagnostics_ref": "incident-diag-sre-incident-storage-deg-20260717",
    "timestamp": "2026-07-17T23:00:00Z"
  }
}
```

---

## 3. Agent Specifications & Context Windows

Below is the execution detail for each agent running in the Hub LangGraph framework:

### 3.1 TriageAgent
* **Primary Role**: Processes incoming raw incidents and routes them.
* **Inbound Context Window**:
  - The raw Kafka CloudEvent payload (Section 2).
* **Outbound Context Window**:
  - Sets the initial state parameters: `incident_id`, `cluster_id`, and `specialist_agent` (e.g., `rca_agent`).
  - Registers the incident session log in Redis.
* **Accessible Tools**:
  - `alert_api.post()`: Triggers the ServiceNow ticket creation and posts the initial triage alert card to Teams.
* **Data Sources Referenced**: None (stateless routing based on `incident_type`).

---

### 3.2 RCAAgent (Diagnostic Engine)
* **Primary Role**: Investigates the failure vector using the OODA loop.
* **Inbound Context Window**:
  - The initial telemetry payload from the operator.
  - Custom system prompts explaining KubeVirt CNI, OVN, and storage paradigms.
* **Observe Phase (Data Gathering)**:
  - Runs a Splunk REST API query for the time range `[triggerTime - 15m, now()]` targeting the VM launcher node or Windows Guest health events.
  - Queries Neo4j for the topological path: `VM -> Pod -> Node -> Storage Pool` (including migration lineage edges).
* **Orient Phase (RAG Search)**:
  - Queries the PostgreSQL `runbooks` and `historical_incidents` tables using the embedding of `triggering_alerts` (similarity match).
* **Decide Phase (LLM Reasoning)**:
  - Injects the logs, events, topology, runbook text, and past historical cases (few-shot context) into the prompt.
* **Outbound Context Window**:
  - Returns a structured JSON schema:
    ```json
    {
      "rca_conclusion": "Portworx replication pool metadata corrupted on worker-04.",
      "confidence_score": 95,
      "proposed_action": {
        "action": "LiveMigrateVM",
        "target": { "kind": "VirtualMachine", "name": "db-vm-01", "namespace": "production-vms" },
        "parameters": { "target_node": "worker-06" }
      },
      "insufficient_data": false
    }
    ```
* **Accessible Tools**:
  - `splunk_tool.query()`, `topology_tool.query()` (Neo4j), `vector_db.search()` (Postgres pgvector).
* **Data Sources Referenced**: Splunk, Neo4j, PostgreSQL.

---

### 3.3 RemediationPlannerAgent
* **Primary Role**: Designs and schedules execution command steps.
* **Inbound Context Window**:
  - The RCA conclusion, proposed action, and topological blast-radius score.
* **Outbound Context Window**:
  - Outputs a complete step-by-step execution plan containing sequential or parallel commands.
  - Publishes the plan payload to `sre.commands`.
* **Accessible Tools**:
  - `k8s_tool.create_command_cr()` (via Kafka messaging).
* **Data Sources Referenced**: None.

---

### 3.4 CapacityAgent (Proactive Scheduler)
* **Primary Role**: Preemptively checks node/volume exhaustions.
* **Inbound Context Window**:
  - Prometheus metric range queries (utilization patterns over 24 hours).
* **Outbound Context Window**:
  - Outputs a forecast rating. If utilization is projected to exceed 95% within 7 days, it issues a preemptive storage expansion `SRECommand` block.
* **Accessible Tools**:
  - `prom_tool.query_range()`, `k8s_tool.create_command_cr()`.
* **Data Sources Referenced**: Prometheus / Thanos.

---

### 3.5 ErrataCorrelatorAgent
* **Primary Role**: Proactively audits VMs against CVE caches.
* **Inbound Context Window**:
  - Daily lists of new RHSAs/CVEs from the Red Hat Errata API.
  - Active spoke cluster registration software logs.
* **Outbound Context Window**:
  - Outputs targeted compliance patching plans.
* **Accessible Tools**:
  - `k8s_tool.read_spoke_versions()`, `errata_cache.query()`.
* **Data Sources Referenced**: Red Hat Errata API, Spoke Cluster Registry.

---

### 3.6 PolicyLearnerAgent
* **Primary Role**: Evaluates MTTR and tunes rules.
* **Inbound Context Window**:
  - Historical closed incident lists and MTTR metrics.
  - Human rating scores (1-5 stars) and comments.
* **Outbound Context Window**:
  - Outputs updated threshold configurations for `SRECorrelationRule` resources on the spokes.
* **Accessible Tools**:
  - `k8s_tool.update_correlation_rule()`.
* **Data Sources Referenced**: PostgreSQL `historical_incidents` table.

---

### 3.7 HumanLoopAgent
* **Primary Role**: Coordinates manual SRE approvals and timeouts.
* **Inbound Context Window**:
  - `SRECommand` status notifications showing `PendingApproval`.
* **Outbound Context Window**:
  - Updates command status to `TimedOut` or `Approved` based on Teams/ServiceNow user clicks.
* **Accessible Tools**:
  - `alert_api.post_approval_link()`.
* **Data Sources Referenced**: Redis session store.

---

## 4. Agent Behavior Profiles: Sure vs. Unsure

To prevent automated scripts from taking unguided or dangerous actions, the platform implements a dual-path behavior matrix:

### 4.1 Sure Scenario (Confidence >= 80%)
* **Criteria**: The LLM successfully matches the alerts/logs to a known runbook or high-rated historical incident.
* **Path**:
  1. The agent compiles the `SRECommand` plan.
  2. If the blast-radius score is `Low` or `Medium`, the plan is marked `AutoApproved` and immediately written to `sre.commands`.
  3. The Spoke Operator executes the changes automatically.

### 4.2 Unsure / Low Confidence Scenario (Confidence < 80% OR unknown error)
* **Criteria**: The LLM cannot match the logs to a runbook, or the extracted log signatures conflict.
* **Path**:
  1. The LLM sets the schema flag `insufficient_data = true` and details the diagnostic gap (e.g., *"Cannot distinguish if packet drops are caused by CNI config or hardware NIC issues on worker-04"*).
  2. The agent **halts automated execution**. No commands are sent to the cluster.
  3. The `HumanLoopAgent` generates a **Teams Chat Card** and ServiceNow alert containing the diagnosis logs, the low-confidence explanation, and buttons for SREs to select manual diagnostics:
     - `[Run Ping Test]`
     - `[Cordon worker-04]`
     - `[Escalate to Human]`
  4. The issue is frozen until an engineer makes a decision.

---

## 5. Summary of Agent Tool Permissions & Access

The table below defines the strict RBAC / network access boundaries for the agent containers:

| Agent Name | Prometheus | Splunk REST | Neo4j Graph | Postgres Vector | Kafka Produce | ServiceNow API | MCP Tools |
| :--- | :---: | :---: | :---: | :---: | :--- | :---: | :--- |
| **TriageAgent** | ❌ | ❌ | ❌ | ❌ | ❌ |  (Webhook) | ❌ |
| **RCAAgent** |  |  |  |  | `sre.commands`¹ | ❌ | `redhat_support.create_case`, `servicenow_tool.get_ticket_status`, `maintenance_tool.check_active_window`, `suppression_tool.check_suppression` |
| **PlannerAgent** | ❌ | ❌ | ❌ | ❌ | `sre.commands` | ❌ | `migration_app.trigger_retry`, `migration_app.trigger_rollback` |
| **CapacityAgent**|  | ❌ |  | ❌ | `sre.commands` | ❌ | `migration_app.trigger_retry` |
| **ErrataAgent** | ❌ | ❌ | ❌ |  | `sre.commands` | ❌ | ❌ |
| **LearnerAgent** | ❌ | ❌ | ❌ |  | `sre.commands` | ❌ | ❌ |
| **HumanLoop** | ❌ | ❌ | ❌ | ❌ | `sre.commands`² |  | ❌ |

¹ RCAAgent is authorized to produce to `sre.commands` specifically to trigger high-priority diagnostic collection escalation commands (never writes to execution/remediation topics).
² HumanLoopAgent is strictly authorized to produce to `sre.commands` only to trigger the `SafeAbort` cleanup command in timeout events. It is strictly blocked from producing to the spoke-exclusive `sre.command-results` topic.

