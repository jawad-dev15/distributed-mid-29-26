# Distributed Object-Tolerant Storage Framework
**Course:** 2601_2653 Distributed Systems | **Institution:** Université Antonine (UA)  
**Student:** Jawad Choukair (ID: 202211402) | **Workstation:** Mid-29

---

## Project Overview
An enterprise-grade, S3-compatible, Content-Addressed Distributed Storage System that stores files split into SHA-256 addressed chunks across independent storage nodes, providing high availability, fault tolerance, deduplication, and real-time observability.

---

## Task 1 Implementation: Storage Node Service & Multi-Node Cluster

### 1. Storage Node Microservice (`storage_node/app.py`)
Each storage node is a standalone FastAPI service exposing:
- **`PUT /chunks/{chunk_hash}`**: Stores chunk data on disk. Verifies `sha256(data) == chunk_hash` before committing to disk; rejects mismatched payloads with `HTTP 400 Bad Request`. Uses atomic write patterns (`.tmp` file and rename) to prevent partial chunk corruption during crashes.
- **`GET /chunks/{chunk_hash}`**: Reads chunk bytes from local disk. Re-computes SHA-256 hash on **every read** to detect bit-rot or tampering. If hash check fails, rejects request with `HTTP 500 Internal Server Error (Bit-rot detected)`.
- **`HEAD /chunks/{chunk_hash}`**: Fast existence and file size verification (`HTTP 200` with `Content-Length` header or `HTTP 404`).
- **`DELETE /chunks/{chunk_hash}`**: Removes chunk file from local disk.
- **`GET /status`**: Reports node ID, absolute storage path, total disk space, used space, free space, chunk count, and uptime.
- **`GET /health`**: Fast liveness probe.

### 2. Periodic Node Heartbeat
Every storage node runs an asynchronous background loop every 5 seconds, collecting disk metrics via `shutil.disk_usage` and reporting health to the central Metadata Registry Service.

### 3. Multi-Node Container Architecture (`docker-compose.yml`)
The cluster is containerized with isolated data volumes for each node:
- **`metadata_service`**: Port `8000` (Cluster topology & heartbeat tracker)
- **`storage_node_1`**: Port `8001` (Volume: `./data/node_1:/data`)
- **`storage_node_2`**: Port `8002` (Volume: `./data/node_2:/data`)
- **`storage_node_3`**: Port `8003` (Volume: `./data/node_3:/data`)
- **`storage_node_4`**: Port `8004` (Volume: `./data/node_4:/data`)
- **`rabbitmq`**: Ports `5672` (AMQP) & `15672` (Management UI)
- **`prometheus`**: Port `9090` (Metrics Collector)
- **`grafana`**: Port `3000` (Dashboard UI)

---

## Task 2 Implementation: Chunking, Metadata & Consistent Hashing

### 1. Chunking Engines (`gateway/chunker.py`)
- **Fixed-Size Chunking**: Splits incoming streams into uniform blocks (default 4 KB). Fast but vulnerable to the *boundary shift problem* (inserting 1 byte invalidates all subsequent chunk hashes).
- **Fast Content-Defined Chunking (FastCDC)**: Employs a rolling Gear hash matrix with normalized masks (`mask_s` and `mask_l`) to establish boundaries based on data content.
- **Boundary Shift Benchmark**: Inserting 44 bytes into a 50 KB file results in 0.0% reusable chunks for fixed-size chunking, but achieves **92.31% deduplication reuse** under FastCDC.

### 2. Consistent Hashing Ring with Virtual Nodes (`gateway/consistent_hash.py`)
- **Virtual Nodes**: Configured with 150 virtual nodes per physical storage node (600 tokens total for 4 nodes) mapped across the SHA-256 ring space.
- **Uniform Distribution**: Distributes 2,000 chunks evenly across `storage_node_1` (27.8%), `storage_node_2` (25.0%), `storage_node_3` (25.4%), and `storage_node_4` (21.9%).
- **Minimal Migration on Scale-Out**: Adding a 5th node (`storage_node_5`) moves only **17.9%** of chunks onto the new node (close to the theoretical optimum $1/N = 20.0\%$), whereas traditional modulo hashing invalidates **80.5%** of chunks.
- **Node Removal**: Removing a node gracefully reassigns its chunks to clockwise neighbors on the ring with minimal migration (**20.05%**).

### 3. PostgreSQL Metadata Catalog & Chunk Deduplication (`metadata_service/metadata_db.py`)
- **Relational Schema**:
  - `objects`: `bucket`, `key`, `version`, `size`, `chunk_hashes` (ordered JSON array), `created_at`.
  - `chunks`: `chunk_hash` (primary key), `size`, `ref_count`, `target_nodes` (JSON array), `created_at`.
- **Reference-Counted Deduplication**:
  - Identical chunk payloads are stored once on physical disks; identical hashes increment `ref_count`.
  - Storing a modified version of a document saved 8,192 bytes (66.67% deduplication ratio).
  - Deleting an object decrements the reference counts of its constituent chunks. Only orphaned chunks (`ref_count == 0`) are purged from disk and catalog.

---

## Task 3 Implementation: Replication, Erasure Coding & Self-Healing

### 1. Quorum-Based Replication (`gateway/replication.py`)
- **Configurable Quorum**: Configured with Replication Factor $N=3$, Write Quorum $W=2$, and Read Quorum $R=2$, guaranteeing strong consistency ($W + R = 4 > 3 = N$).
- **Concurrent Dispatch**: Concurrently dispatches writes and reads to target nodes selected by the Consistent Hash Ring.

### 2. Reed-Solomon Erasure Coding (`gateway/erasure_coding.py`)
- **$4+2$ Codec via `zfec`**: Splits objects into $k=4$ data fragments and $m=2$ parity fragments (6 total fragments).
- **Storage Multiplier**: Only $1.5\times$ (50% storage overhead), compared to $3.0\times$ (200% overhead) for 3-way replication.
- **Catastrophic Failure Recovery**: Successfully recovers 100% of the original data when ANY 2 fragments/nodes are lost.

### 3. Automated Node Failure Detector & Repair Worker (`gateway/repair.py`)
- Monitors node heartbeats. Once a node is marked `OFFLINE`, triggers an automated repair job.
- Reads surviving replicas from healthy nodes, computes replacement nodes via the consistent hash ring, and restores redundancy.

### 4. Background Bit-Rot Scrubber (`gateway/scrubber.py`)
- Proactively scans on-disk chunks, detecting corrupted or tampered bytes.
- Automatically fetches pristine copies from healthy peer nodes and self-heals corrupted replicas on disk.

---

## Task 4 Implementation: S3 FastAPI Gateway, JWT Auth, Range & Multipart

### 1. FastAPI Gateway Microservice (`gateway/app.py`)
Exposes an enterprise S3-compatible REST API:
- **`POST /api/auth/token`**: Generates signed JWT tokens with 24-hour expiration.
- **Sliding-Window Rate Limiter**: Limits clients to 50 requests/minute, responding with `HTTP 429 Too Many Requests` and `Retry-After` headers.
- **Bucket Management**: `POST /api/buckets` and `GET /api/buckets` with configurable storage policies (`replication` or `erasure_coding`).
- **Streaming Upload**: `POST /api/objects/{bucket}/{key}` streams data, executes FastCDC chunking, performs deduplication checks, writes via Quorum, and attaches user tags (`X-Tags`).
- **Byte-Range Partial Downloads**: `GET /api/objects/{bucket}/{key}` evaluates `Range: bytes=start-end`, slices the payload, and returns `HTTP 206 Partial Content` with `Content-Range` headers.
- **Multipart Uploads**: Full lifecycle for large files (`/multipart/initiate` $\to$ `/multipart/.../{part_num}` $\to$ `/multipart/.../complete`).
- **Object Search & Filtering**: `GET /api/search` queries by key prefix, file size range, and user tags (`tag_key` / `tag_value`).
- **Cluster Status**: `GET /api/cluster/status` exposes cluster topology, hash ring tokens, and deduplication statistics.

### 2. Postman Collection Export (`gateway/postman_collection.json`)
Pre-configured collection covering authentication, streaming uploads, multipart parts, byte-range queries, and cluster status.

---

## Task 5 Implementation: React Web Dashboard & Elasticsearch Engine

### 1. React Web Application (`storage-ui/`)
- Single-page dashboard built with React and Vite:
  - **Object Browser & Uploader**: Bucket selector, FastCDC vs Fixed Chunking toggle, metadata tagging, and streaming upload commit.
  - **RFC 7233 Byte-Range Partial Content**: In-browser range slice requests rendering HTTP 206 Partial Content previews.
  - **Cluster Telemetry**: Live cards for all 4 storage nodes (`storage_node_1` to `storage_node_4` on ports 8001–8004) showing uptime, heartbeat, capacity, stored chunks, 150 virtual tokens/node, and deduplication catalog metrics.
  - **Elasticsearch Query Engine View**: Interactive Query DSL tester displaying query syntax and document hits.

### 2. Elasticsearch-Compatible Search Engine (`gateway/search_engine.py`)
- Inverted index over key tokens, path prefixes, and user-defined metadata tags (`tags.department:finance`).
- Exposes `POST /api/search/es` and `GET /api/search/es` with sub-millisecond query execution (<0.1ms).
- Supports Elasticsearch Query DSL: `match`, `prefix`, `term`, and `range`.

---

## Task 6 Implementation: Nginx Load Balancer, RabbitMQ Workers & Observability

### 1. Nginx Load Balancing (`nginx/nginx.conf`)
- Runs on port `8088`, using `least_conn` to distribute incoming S3 REST traffic across dual gateway replicas (`gateway_1:8080` and `gateway_2:8081`).
- Eliminates single points of failure with active upstream health probes and zero-downtime failover.

### 2. RabbitMQ Task Queues & Asynchronous Workers (`workers/tasks.py` & `workers/worker.py`)
- Durable AMQP queues (`storage_repair_queue`, `storage_rebalance_queue`, `storage_gc_queue`).
- Background worker pool consumes chunk rebuild jobs, executes ring token rebalancing, and safely purges orphaned chunk blocks.

### 3. Kubernetes StatefulSet & HA Architecture (`k8s/`)
- **`k8s/storage-statefulset.yaml`**: 4 storage node replicas with 10Gi PersistentVolumeClaims, `/health` liveness and `/status` readiness probes.
- **`k8s/metadata-statefulset.yaml`**: 3-member replicated HA PostgreSQL metadata store.
- **`k8s/gateway-deployment.yaml`**: Gateway deployment with LoadBalancer Service & HorizontalPodAutoscaler (HPA).

### 4. Prometheus & Grafana Observability
- Prometheus scrapes Gateway 1 and Gateway 2 `/metrics` every 5 seconds (Status: UP).
- Pre-configured Grafana Dashboard (`Distributed Storage Cluster Telemetry - Mid-29`) displaying live throughput, request status code distributions, storage node health, chunk distribution, and background repair rates.

---

## How to Run & Verify

### A. Run Automated Unit & Integration Tests (24/24 Passing)
```powershell
.\.venv\Scripts\python -m pytest tests/ -v
```

### B. Run Task 1 Demonstration (Storage Nodes & Bit-Rot)
```powershell
.\.venv\Scripts\python verify_task1_demo.py
```

### C. Run Task 2 Demonstration (Chunking, Ring Migration & Deduplication)
```powershell
.\.venv\Scripts\python verify_task2_demo.py
```

### D. Run Task 3 Demonstration (Quorum, Erasure Coding & Self-Healing)
```powershell
.\.venv\Scripts\python verify_task3_demo.py
```

### E. Run Task 4 Demonstration (S3 Gateway, JWT Auth, Range & Multipart)
```powershell
.\.venv\Scripts\python verify_task4_demo.py
```

### F. Run Task 5 Demonstration (React UI & Elasticsearch Engine)
```powershell
.\.venv\Scripts\python verify_task5_demo.py
```
Open the live dashboard in your browser at: `http://localhost:5173/`

### G. Run Task 6 Demonstration (Nginx LB, RabbitMQ, K8s & Monitoring)
```powershell
.\.venv\Scripts\python verify_task6_demo.py
```
View Prometheus at: `http://localhost:9090` | View Grafana at: `http://localhost:3000`
