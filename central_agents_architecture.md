# Central Agent Platform: Intelligence, Reasoning & RAG

The **Central Agent Platform** represents the "Smart Brain" of the architecture. Deployed centrally on the Hub, it is responsible for analyzing telemetry, executing root cause analysis (RCA) loops, scheduling proactive maintenance, and coordinating remediation strategies across all spoke clusters.

---

## 1. Technological Stack

* **Agent Orchestration**: **LangGraph (Python)** — a stateful, cyclical graph-based framework for constructing multi-agent architectures with human-in-the-loop validation checkpoints.
* **Vector Database**: **PostgreSQL (with pgvector)** — standard relational engine with pgvector extension supporting cosine similarity vector lookup, relational SQL filters, and HNSW indexing.
* **Graph Database**: **Neo4j** — hosts cluster topology relationships and executes Cypher queries to compute blast-radius scopes.
* **Short-Term Storage & Cache**: **Redis** — maintains agent working memory, session locks, and temporary log buffers.
* **LLM Serving Engine**: **vLLM** — running open-source models (Llama-3-70B-Instruct or Mistral Large) locally on Hub cluster GPU pools for air-gapped security compliance.
* **Embedding Model**: `text-embedding-3-small` or `nomic-embed-text`.

---

## 2. Specialized Agent Roles

The Hub runs seven specialized agents, each managing a specific domain of operations:

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
                  └──────────────────────────────────────────────┘
```

### 2.1 TriageAgent (The Router)
* **Ingestion**: Consumes `sre.incidents.lifecycle` messages.
* **Function**: Registers the incident in Redis, determines initial routing to specialist agents, and creates the baseline ServiceNow incident (via AlertManager webhook mappings) for P1 Critical failures.

### 2.2 RCAAgent (The Investigator)
* **Function**: Runs a strict **OODA** (Observe-Orient-Decide-Act) troubleshooting loop.
* **Process**: 
  - **Observe Phase**: 
    1. Reads the **`status.diagnostics`** block directly from the incoming `SREIncident` CR (containing auto-collected Pod logs, K8s Events, and Prometheus metrics snapshotted at trigger time).
    2. Inspects `status.diagnostics.releaseContext` to check if the failure is correlated with a recent manual GitOps sync (`SRERelease`).
    3. Runs an extended log range query via the **Splunk REST API** (`[triggerTime - 15m, triggerTime + 1h]`) to fetch hypervisor-level details, Multus CNI statuses, or Portworx OSD logs.
  - **Orient Phase**: Pulls topology graphs (Neo4j), queries runbooks and historical case studies (PostgreSQL), formulates a root-cause hypothesis, and passes the output to the planner.

### 2.3 RemediationPlannerAgent (The Architect)
* **Function**: Receives the root-cause hypothesis and designs an orchestration plan.
* **Process**: Translates the proposed mitigation into sequential or parallel `SRECommand` CRD instructions and writes the plan to `sre.commands`.

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

## 4. The OODA Loop Execution Engine

The `RCAAgent` implements a strict structural layout for its diagnostic process:

```
Observe ──── (Gathers logs, commits, metrics, and configurations in parallel)
  │
  ▼
Orient ───── (Queries Neo4j for topology maps and PostgreSQL pgvector for relevant runbooks)
  │
  ▼
Decide ───── (Invokes the LLM using the structured JSON output schema)
  │
  ▼
Act ──────── (Emits SRECommands to Kafka and logs audit data to sre.audit)
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
