# Central Agent Platform: Intelligence, Reasoning & RAG

The **Central Agent Platform** represents the "Smart Brain" of the architecture. Deployed centrally on the Hub, it is responsible for analyzing telemetry, executing root cause analysis (RCA) loops, scheduling proactive maintenance, and coordinating remediation strategies across all spoke clusters.

---

## 1. Technological Stack

* **Agent Orchestration**: **LangGraph (Python)** — a stateful, cyclical graph-based framework for constructing multi-agent architectures with human-in-the-loop validation checkpoints. Uses a **PostgresSaver checkpointer** to persist execution graphs across hub pod restarts.
* **Tool Abstraction Layer**: **ToolRegistry** — a thin Python wrapper that maps agent actions to dynamic methods, abstracting in-process imports from transport layers. This ensures stdio/HTTP+SSE-based Model Context Protocol (MCP) servers can be swapped in without modifying agent logic.
* **Vector Database**: **PostgreSQL (with pgvector)** — standard relational engine with pgvector extension supporting cosine similarity vector lookup, relational SQL filters, and HNSW indexing.
* **Graph Database**: **Neo4j** — hosts cluster topology relationships and executes Cypher queries to compute blast-radius scopes.
* **Short-Term Storage & Cache**: **Redis** — maintains agent working memory, session locks, and temporary log buffers.
* **LLM Serving Engine**: **vLLM** — running open-source models (Llama-3-70B-Instruct or Mistral Large) locally on Hub cluster GPU pools for air-gapped security compliance.
* **Embedding Model**: `text-embedding-3-small` or `nomic-embed-text`.

---

## 2. Specialized Agent Roles

The Hub runs eight specialized agents, each managing a specific domain of operations:

```
                  ┌──────────────────────────────────────────────┐
                  │                 TriageAgent                  │
                  │         - Consumes Incident Lifecycle        │
                  │         - Routes to Specialist Agents        │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │                  RCAAgent                    │
                  │         - Runs OODA Loop                     │
                  │         - Evaluates Logs & Metrics           │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │           RemediationPlannerAgent            │
                  │         - Synthesizes Command Steps          │
                  │         - Checks Risk & Blast Radius         │
                  └──────────────────────┬───────────────────────┘
```

### 2.1 TriageAgent (The Router)
* **Ingestion**: Consumes `sre.incidents.lifecycle` messages.
* **Function**: Registers the incident in Redis, determines initial routing to specialist agents, and creates the baseline ServiceNow incident (via AlertManager webhook mappings) for P1 Critical failures.

### 2.2 RCAAgent (The Investigator)
* **Function**: Runs a strict **OODA** (Observe-Orient-Decide-Act) troubleshooting loop.
* **Process**: 
  - **Observe Phase**: 
    1. Reads the size-capped diagnostic summary directly from the incoming `sre.incidents.lifecycle` event. If a deeper forensic analysis is required, it dynamically fetches the full, un-truncated diagnostic logs/events from the ConfigMap referenced in `status.diagnosticsRef` via `ToolRegistry`.
    2. Inspects `status.diagnostics.releaseContext` to check if the failure is correlated with a recent manual GitOps sync (`SRERelease`).
    3. Runs an extended log range query via the **Splunk REST API** using a query window bounded to `[triggerTime - 15m, now()]` (ending at `now()` to prevent querying log events that have not occurred yet) targeting the VM launcher node.
  - **Orient Phase**: Pulls topology graphs (Neo4j), queries runbooks and historical case studies (PostgreSQL), formulates a root-cause hypothesis, and passes the output to the planner.

### 2.3 RemediationPlannerAgent (The Architect)
* **Function**: Receives the root-cause hypothesis and designs an orchestration plan via intent-based indirection.
* **Process**: Instead of directly synthesizing raw YAML actions, the LLM selects intents from a versioned **`WorkflowCatalog`** (e.g., "intent: recover kubevirt-handler"). A deterministic policy table then maps the intent and cluster capabilities to the correct underlying `SRECommand` CRD instructions and writes the plan to `sre.commands`. This reduces hallucination risk and standardizes blast-radius scoring.

### 2.4 CapacityAgent (Proactive Forecaster)
* **Function**: Proactively checks cluster capacity constraints using Prometheus range queries.
* **Process**: Runs linear regression models on node memory, CPU, and storage. If utilization is projected to exceed thresholds (95% Critical / 85% Warning) within **7 days**, it automatically issues a PVC expansion command.

### 2.5 ErrataCorrelatorAgent (Compliance Audit)
* **Function**: Periodically audit node and VM configurations against Red Hat security errata.
* **Process**: Scrapes new advisories, maps kernel version dependencies against live virtualization nodes, and proactively schedules rolling package updates during maintenance windows.

### 2.6 PolicyLearnerAgent (Optimizing Engine)
* **Function**: Analyzes historical operational efficiency.
* **Process**: Calculates MTTR metrics from closed incidents. If a correlation rule generates high numbers of false alerts, it automatically increases trigger thresholds to prevent paging fatigue.

### 2.7 HumanLoopAgent (Escalator)
* **Function**: Manages human-in-the-loop approvals.
* **Process**: Watches commands pending manual confirmation. If an approval times out (based on severity thresholds), it cancels the action, triggers a `SafeAbort` rollback command, and escalates the ServiceNow incident to page on-call SREs.

### 2.8 FleetCorrelationAgent (Pattern Detector)
* **Function**: Detects platform-level bugs by identifying correlated incidents across multiple clusters.
* **Process**: Groups active mirror incidents by signature (e.g., matching triggering alerts, OCP version, storage provider). Marks redundant incidents as duplicates (suppressing redundant RCAAgent LLM invocations) and creates unified fleet-level incident tickets.

---

## 3. Agent Toolset Integration

Agents cannot modify resources directly. They reason and interact with Kubernetes and telemetry stacks using a defined list of Python-based tools:

| Tool Name | Scope | Primary Function |
| :--- | :--- | :--- |
| `prom_tool.query` | Telemetry | Queries Prometheus metrics on spoke clusters via federated Thanos. |
| `splunk_tool.query` | Logs | Gathers guest OS, CNI, and virtual machine logs during RCA. |
| `topology_tool.query`| Graph | Runs Cypher queries on Neo4j to build blast-radius dependencies. |
| `vector_db.search` | Vector DB | Searches PostgreSQL using pgvector for semantic runbook matches and errata workarounds. |
| `k8s_tool.create_cr` | Kubernetes| Creates SRECrossClusterRead or mirror incident CRDs on the Hub. |

---

## 4. LangGraph Stateful Orchestration Graph

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

### 4.1 Schema Enforcement (Structured Output)
Every LLM call within the agent graph is validated against a strict JSON schema:
- **`hypotheses`**: Array of ranked potential causes with confidence scores, source citations, and evidence lines.
- **`proposed_action`**: Action name, target resource coordinates (kind/name/namespace), and parameter block.
- **`rollback`**: Action and parameters to revert the mitigation if post-execution health checks fail.
- **`insufficient_data`**: Boolean flag. If `true`, the LLM explains the data gap and halts remediation to prevent unguided mutations.

### 4.2 Proactive vs. Reactive Execution
- **Reactive Workflow**: Triggered by a Kafka event -> TriageAgent -> RCAAgent -> Execution Plan -> Operator acting on cluster.
- **Proactive Workflow**: Triggered by Cron -> CapacityAgent -> Linear Regression -> Kafka `sre.commands` -> Operator expanding storage or scaling nodes.

---

## 5. Agent Long-Term Memory & Human Feedback Loop

To learn over time, the Central Agent Platform leverages a **Long-Term Memory** database (the `historical_incidents` PostgreSQL table) and a structured **Human Feedback Loop**.

### 5.1 The Feedback Loop Lifecycle

```
[Incident Resolved] ──> [SRE inputs rating (1-5) & feedback] ──> [RCA summary embedded] ──> [Stored in Long-Term Memory]
                                                                                                      │
[New Incident Fired] <── [Agent queries Memory (Few-Shot Prompt)] <───────────────────────────────────┘
```

1. **Inputting Feedback**:
   - When an `SREIncident` is resolved, engineers can write feedback notes and rate the automated resolution (1 to 5 stars) directly via their ServiceNow ticket or within the `SREIncident.spec.humanFeedback` fields on the Hub cluster.
2. **Memory Ingestion**:
   - The `PolicyLearnerAgent` captures this feedback. It generates an embedding of the incident context (`title + alert_names + rca_conclusion`) and stores the record in the `historical_incidents` table.
3. **Retrieval during Triage**:
   - When a new active incident is processed, the `RCAAgent` performs a cosine-similarity query on the `historical_incidents` vector database to retrieve similar past failures.

### 5.2 Dynamic Few-Shot Prompt Injection

The retrieved historical data is injected directly into the LLM context window using two distinct categories:

#### A. Positive Few-Shot Examples (Rating >= 4)
If past similar incidents were resolved successfully, the agent incorporates them as recommended templates:
> *"Here is how a similar failure was successfully resolved on cluster prod-west-02: [RCA: Node Worker-02 had stale Portworx replicas. Fix: Cordon Node followed by VM LiveMigration]."*

#### B. Negative Few-Shot Examples (Rating <= 2)
If past incident actions were flagged as bad or dangerous by human engineers, the agent injects them as warnings:
> *"WARNING: On 2026-06-01, an agent attempted to restart CNI pods for a similar alert on prod-east-01. The human rated this 1 star, commenting: 'Do NOT restart CNI during active VM live migrations—it causes virtual networks to disconnect'. Do NOT propose restarting CNI."*

This feedback mechanism guarantees that the central planner learns from mistakes and respects specific environment constraints.

---

## 6. User Interfaces & Integration

The platform provides dedicated interfaces tailored for SRE engineers and platform maintainers:

### 6.1 SRE ChatOps (Microsoft Teams Integration)
Teams Adaptive Cards serve as the operational hub during incidents, providing interactive buttons mapping to API operations:
- **`Approve`**: Sends an approval event to `sre.command-results`, authorizing the Spoke Operator to execute high-risk commands.
- **`Reject`**: Cancels the command step and instructs `RemediationPlannerAgent` to execute rollback commands.
- **`Escalate`**: Places the plan in manual mode, halts automated agent actions, and pages primary SRE on-call.
- **`Re-evaluate`**: Clears the current diagnostic cache and triggers `RCAAgent` to run a fresh OODA loop.

### 6.2 Agent Maintainer UI (Hub Admin Panel)
Designed for developers tuning the agent platform:
- **LangGraph Observability**: Utilizes LangSmith or LangFuse to log node latency, trace agent token costs, and track LLM inputs/outputs.
- **Prompt Registry Manager**: Dynamically manages LLM system prompts without rebuilding Python container images.
- **pgvector Runbook Portal**: Web portal to upload markdown runbooks, trigger semantic embeddings, and check retrieval accuracy scores.

### 6.3 Agent Platform Observability
The Hub exports specific Prometheus metrics to track the LLM platform's cost, performance, and calibration:
- `sre_agent_llm_tokens_total{agent, incident_type}`: Tracks token consumption.
- `sre_agent_llm_cost_usd_total{agent}`: Tracks financial spend per agent.
- `sre_agent_confidence_vs_outcome{bucket}`: Calibrates the agent's self-reported confidence against actual resolution success.
- `sre_agent_human_override_rate{incident_type}`: Tracks how often engineers reject the plan.
- `sre_agent_rollback_frequency{action}`: Tracks validation check failures.
- `sre_agent_reasoning_latency_seconds{agent, phase}`: Tracks time spent in Observe vs Orient vs Decide phases.

---

## 7. Structured Context Processing (Prompt Engineering)

To make correct decisions, the agent prompt is built dynamically using retrieved context. The agent is provided with:
1. **The Telemetry Payload**: The raw Prometheus alert and the exact guest OS error message.
2. **The Runbook Excerpt**: The matching step-by-step resolution from the vector DB.
3. **The Topology Chain**: The Neo4j dependency chain showing what else runs on the same physical node.

This structure allows the agent to reason securely:
> *"The VM has disk I/O errors. Topology shows worker-02 has a degraded Portworx storage pool. The runbook instructs to migrate the VM to worker-03. I will issue a LiveMigrateVM command targeting worker-03."*

---

## 8. LLM Gateway: Dynamic Token Authentication

For air-gapped security compliance, the central agents connect to the local models via an **Internal LLM Gateway**. Rather than utilizing static API keys, the gateway enforces a short-lived token rotation schema:

### 8.1 Authentication Protocol
1. **Dynamic Token Ingestion**: The agent is configured with gateway client credentials (`LLM_CLIENT_ID` and `LLM_CLIENT_SECRET`) mounted via Kubernetes secrets.
2. **Token Fetching**: Before sending any completion request, the Python agent queries the internal authentication endpoint `/auth/token`.
3. **30-Second Lifetime**: The returned Bearer token has a strict **30-second expiry**.
4. **Auto-Refreshing Middleware**: The HTTP client implements a dynamic wrapper to handle caching and refresh requests.

### 8.2 Client Implementation Pattern (Python)
```python
import time
import requests
from datetime import datetime, timedelta

class GatewayTokenRefresher:
    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.cached_token = None
        self.expiry_time = datetime.min

    def get_token(self) -> str:
        # Refresh if token is close to expiring (within 5 seconds of the 30-second window)
        if not self.cached_token or datetime.utcnow() >= self.expiry_time - timedelta(seconds=5):
            self._refresh_token()
        return self.cached_token

    def _refresh_token(self):
        response = requests.post(
            self.token_url,
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        self.cached_token = data["access_token"]
        # Set expiry (30 seconds from now)
        expires_in = int(data.get("expires_in", 30))
        self.expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)
```
This refresher is registered as a custom authentication middleware in the agent's LangGraph model client context.

---

## 9. Stateful Systems & Disaster Recovery

The central Hub relies on several stateful databases that require strict backup, DR runbooks, and HA configurations to prevent the SRE platform from becoming a single point of failure during a critical outage:
- **PostgreSQL / pgvector**: Stores `historical_incidents` and runbook embeddings. Requires continuous WAL archiving and daily snapshots.
- **Neo4j**: Stores the cluster topology graph. Requires regular backups, although the graph can be rebuilt by re-ingesting ACM hub data if lost.
- **Redis**: Caches LLM context windows and session states. Should be configured with persistence (AOF/RDB) or treated as volatile with the understanding that active RCA loops will restart on failure.
- **Kafka**: The central event backbone (and regional federated clusters) requires strict replication (min. ISR) and topic retention policies.
