# Cortex SRE Operator: Next-Generation Automated Remediation

## Executive Summary
The **Cortex SRE Operator** is an event-driven, Hub-and-Spoke Kubernetes operator designed to automate incident resolution across hundreds of OpenShift Virtualization clusters. Moving away from traditional synchronous webhook architectures, it leverages Kafka for complete decoupling between the Spoke clusters and Central Platform Agents. 

The core philosophy of the SRE Operator is **"Smart Brain, Dumb Hands."** The Spoke Operator is entirely stateless and declarative, focusing strictly on GitOps safety, blast-radius calculation, and execution. All complex reasoning, policy learning, and RCA generation are offloaded to Central AI-assisted Agents running on the Hub.

## 1. Architectural Philosophy

### 1.1 GitOps-Awareness & Exclusion Protocol
Directly modifying declarative resources managed by GitOps controllers (like ArgoCD) leads to immediate state drift, which GitOps aggressively reverts. To prevent drift fights and configuration corruption, the SRE Operator enforces a strict **GitOps-Exclusion Protocol**:
- The operator never attempts to modify declarative resources (Deployments, replicas, config maps) that are managed by GitOps.
- If a required remediation targets a GitOps-managed resource, the operator blocks the action, flags the command as `RequiresManualIntervention`, and delegates diagnosis to the Central Agents.
- The Central Agents analyze the config error and generate the **exact suggested YAML diff / patch block** needed to correct the issue.
- This copy-pasteable configuration fix is posted directly to **Microsoft Teams** (via webhook integration) and attached to the ServiceNow ticket, allowing SREs to quickly apply the patch to Git.
- The operator focuses automated remediation **strictly** on runtime/infrastructure-level resources that are NOT managed by GitOps (e.g., CordonNode, LiveMigrateVM, deleting transient pods, Portworx storage expansions), ensuring zero conflict with GitOps controllers.

### 1.2 Event-Driven OODA Loop via Kafka
To handle thousands of alerts without overwhelming Kubernetes control planes or creating complex inter-operator REST dependencies, the architecture utilizes Kafka.
- **Observe**: Alerts are ingested into the `sre.telemetry.raw` topic.
- **Orient**: Correlation rules group signals into `SREIncident` CRDs.
- **Decide**: Hub Agents analyze the incident and propose `SRECommand` payloads into `sre.commands`.
- **Act**: The Spoke Operator picks up the command, verifies safety rails, and executes it, emitting results back to `sre.command-results`.

## 2. Core Components (The Spoke Operator)

The Spoke operator is deployed to every managed cluster and executes the following critical reconcilers:

### 2.1 SREIncidentReconciler
Manages the lifecycle of an incident. It handles deduplication, phase transitions (`Active` -> `Remediating` -> `Resolved`), MTTR calculation, and SLA tracking. It listens to command results from Kafka to auto-resolve incidents once all remediation steps succeed.

### 2.2 SRECorrelationRuleReconciler & BoltDB Signal Store
To correlate transient alerts (e.g., CPU spikes followed by Node NotReady events), the operator maintains a persistent temporal buffer using a local **BoltDB** instance mounted via PVC. The correlator continuously evaluates incoming alerts against declarative `SRECorrelationRule` definitions and fires an incident if conditions are met within a rolling time window.

### 2.3 SRECommandReconciler
The workhorse of the operator. It translates high-level commands proposed by the Hub (e.g., `CordonNode`, `ExpandPVC`) into concrete Kubernetes API actions. 
Before execution, it enforces:
- **Blast Radius Computation**: Calculates how many workloads are impacted.
- **Approval Workflows**: Halts execution for High/Critical risk commands until a human approves via the API.
- **Dependency Graphs**: Ensures sequential commands execute in the correct order.
- **Zero-Trust Emergency Bypass**: In P1 Critical scenarios, it validates a short-lived, cryptographically signed JWT (RS256) issued by the Hub to bypass GitOps and approval rails.


## 3. The Central Hub

The Hub acts as the aggregation point and "Smart Brain." It runs various specialized agents:
- **RemediationPlannerAgent**: Breaks down complex incidents into a sequence of safe `SRECommand` steps.
- **HumanLoopAgent**: Monitors commands pending approval and escalates timeouts via PagerDuty/ServiceNow.
- **TopologyAgent**: Maintains a real-time Neo4j graph of all clusters, VMs, and PVCs, cleaning up stale nodes to ensure accurate blast-radius estimates.
- **CapacityAgent**: Analyzes telemetry trends to proactively expand PVCs or propose Node additions before exhaustion occurs.

## 4. User Interfaces

The platform splits user interaction into two interfaces based on user roles:

### 4.1 SRE ChatOps (Microsoft Teams Integration)
The primary operational interface for SRE engineers during an active incident. Teams Adaptive Cards deliver:
- **Incident Summaries**: Aggregated root cause analysis and metric evidence.
- **Interactive Action Buttons**: `Approve`, `Reject`, `Escalate`, or `Trigger Rollback` directly within the chat window.
- **GitOps Diff Cards**: Copy-pasteable YAML blocks when declarative modifications are required.

### 4.2 Agent Maintainer UI (Central Hub Dashboard)
A specialized administrative interface for SRE platform maintainers to monitor and tune the AI engine:
- **LangGraph Tracing (LangSmith/LangFuse)**: Visualizes the execution steps of the agent graphs to debug prompt latencies and tool usage.
- **Neo4j Explorer**: Visual interface to traverse the active cluster topology.
- **PostgreSQL pgvector Configurator**: Allows uploading new runbooks, editing embeddings, and auditing human feedback ratings.

## 5. Security & Compliance

- **Admission Webhooks**: Validating webhooks intercept `SRECommand` creation, ensuring that emergency bypass tokens are cryptographically valid, correctly scoped, and not expired.
- **Audit Logging**: Every command execution, approval, and bypass is immutably written to the `sre.audit` Kafka topic, retained for 90 days for compliance reviews.
- **Maintenance Windows**: Defined at the cluster level; active maintenance windows automatically suppress new incident creation to prevent alert fatigue during scheduled downtime.
- **Multi-Region Federation**: To ensure sub-second Dead Man's Switch heartbeat latency across the globe, Kafka clusters are federated regionally via MirrorMaker2.

## Conclusion
The Cortex SRE Operator bridges the gap between AI-driven Operations and GitOps strictness. By strictly decoupling decision-making from execution and treating Kafka as the central nervous system, it achieves massive scale, absolute auditability, and impenetrable safety rails for autonomous remediation.
