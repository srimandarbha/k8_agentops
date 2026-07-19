# Real-Time Domain RAG & Knowledge Sourcing Architecture
## For OpenShift Virtualization, KubeVirt, and Platform Infrastructure

To enable the Central SRE Agents to make accurate, production-grade decisions on VM, Pod, and Platform failures, they must access a combination of static upstream documentation and real-time environment configurations. This document defines the architecture for a **Real-Time Domain RAG (Retrieval-Augmented Generation)** pipeline.

---

## 1. Architectural Overview

The RAG system decouples static platform manuals (e.g., KubeVirt node evacuation runbooks) from real-time operational context (e.g., current cluster configurations). The Central Agents query this system during their OODA loop's **Orient** phase.

```
                  ┌──────────────────────────────────────────────┐
                  │           Central Platform Agents            │
                  └──────────────────────┬───────────────────────┘
                                         │
                                   Orient Phase
                        (Semantic Vector + Metadata Query)
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │    PostgreSQL Database (with pgvector)       │
                  └──────────────────────┬───────────────────────┘
                                         │
       ┌─────────────────────────────────┼────────────────────────────────┐
       ▼                                 ▼                                ▼
┌──────────────┐                  ┌──────────────┐                 ┌──────────────┐
│  Runbooks    │                  │  Platform    │                 │  Telemetry   │
│  Collection  │                  │  Errata      │                 │  & Topology  │
└──────▲───────┘                  └──────▲───────┘                 └──────▲───────┘
       │                                 │                                │
 (Static Sync)                     (Daily Sync)                    (Real-Time Sync)
       │                                 │                                │
 ┌─────┴──────────┐               ┌──────┴─────────┐               ┌──────┴───────┐
 │ RH Portal, KB, │               │ Red Hat Errata │               │ Kafka:       │
 │ KubeVirt Docs, │               │ API, CVE feeds │               │ telemetry.raw│
 │ Internal Wikis │               │                │               │ crosscluster │
 └────────────────┘               └────────────────┘               └──────────────┘
```

---

## 2. Ingested Data Sources

To support deep diagnosis, the vector database maps three distinct scopes of data:

### 2.1 Upstream & Platform Domain Knowledge (Static / Semi-Static)
- **OpenShift Virtualization / KubeVirt Manuals**: Official documentation for live migrations, storage bindings, guest agent configurations, and network attachments (Multus/SR-IOV).
- **Red Hat KCS (Knowledgebase) Articles**: Known issues, bug descriptions, and workarounds for OpenShift virtualization stacks (e.g., OVN network interface failures under heavy VM loads).
- **Internal Runbooks & Playbooks**: Curated incident response plans mapping out internal registry configurations (Nexus), proxy exceptions, and approved cluster architectures.

### 2.2 Security & Component Errata (Daily Sync)
- **Red Hat Advisories (RHSA/RHBA)**: Critical updates affecting RHEL CoreOS nodes and OCP operators.
- **NVD CVE Feed**: Vulnerabilities mapped directly to active container images and node kernels.

### 2.3 Live Configuration & Topology (Real-Time / Event-Driven)
- **Cross-Cluster Configurations**: Sanitized `MachineConfigs`, `NetworkPolicies`, CNI configurations, and deployment specs scraped continuously by the `SRECrossClusterReadReconciler`.
- **Knowledge Graph Nodes**: VM-to-Node topological mapping managed in real-time by the `TopologyAgent`.

---

## 3. Real-Time Ingestion & Sync Pipeline

To keep the agent's context window current, updates are processed continuously rather than in batches.

```
┌─────────────────────────────────┐
│     RH Portal / CVE Feeds       │─────┐
└─────────────────────────────────┘     │
                                        │  (Daily)
┌─────────────────────────────────┐     ▼      ┌─────────────────────────┐
│   Internal Git / Markdown Docs  │───────────>│   Semantic Embedding    │
└─────────────────────────────────┘            │    Transformer Service  │
                                        ┌─────>│  (text-embedding-3-sm)  │
┌─────────────────────────────────┐     │      └────────────┬────────────┘
│    SRECrossClusterRead CRs      │─────┘                   │
│   (Kafka: sre.crosscluster.reads)     │  (Real-Time)      │  (Upsert)
└─────────────────────────────────┘     │                   ▼
                                        │      ┌─────────────────────────┐
                                        │      │  PostgreSQL DB          │
                                        │      │  (Tables: runbooks,     │
                                        │      │   errata, configs)      │
                                               └─────────────────────────┘
```

### 3.1 Static Runbook & Docs Sync
* **Trigger**: Git commit webhooks on internal runbook repositories, or weekly cron jobs scraping Red Hat customer portals.
* **Process**: A lightweight doc-parser splits documents using markdown section headers (MarkdownHeaderTextSplitter) rather than strict character counts, preserving tabular data and terminal command parameters.

### 3.2 Real-Time Configuration Ingestion (The Ooda Loop Bridge)
* **Trigger**: Spoke changes trigger a Kafka message on the `sre.crosscluster.reads` topic.
* **Process**:
  - The Hub operator serializes the raw YAML resource (e.g., an updated `NetworkPolicy` or `MachineConfig`) and strips private metadata (secrets, system annotations).
  - The resource is embedded as a structured string: `Resource: NetworkPolicy/deny-all | Namespace: production | Rules: ...`
  - Upserted into PostgreSQL with a record containing `source_cluster`, `namespace`, and `resource_version`. Old configurations under the same resource reference are instantly evicted using their unique key constraint on `(cluster_id, namespace, resource_name)`.

## 4. PostgreSQL Database Schema (pgvector)

To keep context windows small and queries fast, the PostgreSQL schema structures the vector data into three dedicated tables, indexing them with HNSW (Hierarchical Navigable Small World) indexes:

### 4.1 Schema Definition
```sql
-- Enable vector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Table 1: Runbooks
CREATE TABLE runbooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    components TEXT[] NOT NULL,
    min_ocp_version TEXT,
    required_capabilities JSONB, -- capabilities requirements mapping (e.g. {"storage": {"provider": "portworx"}})
    is_internal BOOLEAN DEFAULT true,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    embedding vector(1536) -- openai text-embedding-3-small dimension
);

-- Table 2: Platform Configs
CREATE TABLE platform_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    namespace TEXT,
    yaml_content TEXT NOT NULL,
    last_sync_time TIMESTAMPTZ DEFAULT NOW(),
    embedding vector(1536),
    UNIQUE (cluster_id, resource_kind, resource_name, namespace)
);

-- Table 3: Historical Incidents (Agent Long-Term Memory & Feedback)
CREATE TABLE historical_incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    incident_type TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    triggering_alerts TEXT[] NOT NULL,
    rca_conclusion TEXT NOT NULL,
    resolution_actions TEXT[] NOT NULL,
    mttr_minutes INTEGER NOT NULL,
    resolved_at TIMESTAMPTZ NOT NULL,
    human_feedback TEXT,
    human_rating INTEGER CHECK (human_rating BETWEEN 1 AND 5),
    embedding vector(1536) -- embedding of: title + triggering_alerts + rca_conclusion
);

-- Index vector columns for rapid cosine distance search
CREATE INDEX ON runbooks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON platform_configs USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON historical_incidents USING hnsw (embedding vector_cosine_ops);
```

### 4.2 Query Execution Pattern
The Agent performs relational-filtered vector similarity queries directly in SQL:
```sql
SELECT title, content, 1 - (embedding <=> %(query_vector)s) AS similarity
FROM runbooks
WHERE %(ocp_version)s >= min_ocp_version
  AND %(component)s = ANY(components)
  AND (required_capabilities IS NULL OR required_capabilities @> %(cluster_capabilities)s)
ORDER BY embedding <=> %(query_vector)s
LIMIT 3;
```

---

## 5. Retrieval & Synthesis Flow for Agents

When an incident transitions to **Active**, the `RCAAgent` performs the following automated search loop:

```
                  ┌──────────────────────────────────────────────┐
                  │         Active SREIncident Fired             │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │       Keyword extraction from Splunk logs     │
                  │             and triggering alerts            │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │          Query 1: Runbook Match              │
                  │      Filter: target_components in payload    │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │         Query 2: Real-time Config            │
                  │      Filter: cluster_id == local_cluster     │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │      Inject both inputs into Agent Prompt    │
                  │       "Identify drift & propose action"      │
                  └──────────────────────────────────────────────┘
```

### 5.1 Prompt Injections Strategy (Example: Nexus ImagePullBackOff issue)
During retrieval, the agent prompt is dynamically compiled:

```markdown
You are diagnosing an SRE Incident on cluster [prod-east-01].
Triggering Alert: KubeVirtPodStuckInImagePullBackOff (Pod: virt-launcher-db01-xxxx)
Spoke Event Error: "failed to pull image nexus.dmz.corp.internal/kubevirt/virt-launcher: connection refused"

---
RETRIEVED DOMAIN KNOWLEDGE (Semantic Similarity: 0.89):
Document: "Nexus Registry Networking Guidelines (Internal Runbook)"
Content: "Nexus registry is only exposed on the Internal Management Network segment. Pods trying to pull from DMZ namespaces must route requests through the squid-proxy-dmz.corp.internal:3128 egress proxy. Workloads must define the HTTPS_PROXY environment variable."

---
RETRIEVED REAL-TIME CONFIGURATION (Cluster: prod-east-01):
Resource: Deployment/virt-launcher-db01 (Namespace: production-vms)
Active env vars: [NO HTTPS_PROXY env var defined]

---
Task: Suggest the remediation step.
```

The agent resolves this instantly: **Emit SRECommand (GitOps PR) adding the HTTPS_PROXY environment variable to the workload config.**

---

## 6. Actionable Implementation Recommendations

To deploy this in production, implement these three key integrations:

1. **Deploy PostgreSQL on the Central Hub**:
   - Run PostgreSQL as a StatefulSet with a Persistent Volume in the `sre-hub` namespace.
   - Install the `pgvector` extension and configure memory bounds (shared_buffers, work_mem) optimized for vector indexes.
2. **Wire Git Webhooks & Incident Resolution Feed**:
   - Establish a CI pipeline in your GitLab/GitHub runbook repository. Every merge to `main` splits changed markdown files and runs a script to upsert embeddings into the PostgreSQL database.
   - Wire a database trigger or listener on `SREIncident` phase resolution: when an incident goes to `Resolved`, capture its metadata, metrics, and any feedback comments left by the SRE team. Compute the semantic embedding of the resolution summary and upsert it into the `historical_incidents` table. This continually updates the agent's long-term memory.
3. **Configure the `SRECrossClusterRead` Sync Interval**:
   - Set the `SRECrossClusterRead` controller to sync critical config kinds (`MachineConfigs`, `NetworkPolicies`, `IngressConfigs`) every **15 minutes** to keep the `platform_configs` table fresh.
