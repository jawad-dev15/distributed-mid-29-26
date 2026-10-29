import time
import os
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from metadata_service.metadata_db import MetadataDatabase

app = FastAPI(
    title="Distributed Storage - Metadata & Cluster Registry Service",
    description="Maintains cluster topology, tracks storage node heartbeats, object metadata & chunk deduplication.",
    version="2.0.0"
)

# Registry of active storage nodes: {node_id: NodeInfo}
active_nodes: Dict[str, Dict[str, Any]] = {}

# Initialize Database Manager (PostgreSQL with SQLite fallback)
db = MetadataDatabase()

class HeartbeatPayload(BaseModel):
    node_id: str
    host: str
    port: int
    url: str
    total_space_bytes: int
    used_space_bytes: int
    free_space_bytes: int
    chunk_count: int
    uptime_seconds: float
    timestamp: float

class ChunkSpec(BaseModel):
    hash: str
    size: int
    target_nodes: List[str] = Field(default_factory=list)

class RegisterObjectRequest(BaseModel):
    bucket: str
    key: str
    size: int
    chunks: List[ChunkSpec]
    version: Optional[int] = None

@app.post("/api/heartbeat", status_code=status.HTTP_200_OK)
def receive_heartbeat(payload: HeartbeatPayload):
    """Receive periodic health telemetry from storage nodes."""
    active_nodes[payload.node_id] = {
        **payload.dict(),
        "last_seen": time.time(),
        "status": "ONLINE"
    }
    return {
        "status": "acknowledged",
        "node_id": payload.node_id,
        "active_cluster_nodes": len(active_nodes)
    }

@app.get("/api/nodes")
def list_nodes():
    """List all storage nodes and evaluate live vs dead nodes based on heartbeat timeouts."""
    now = time.time()
    result = []
    for node_id, info in active_nodes.items():
        is_alive = (now - info["last_seen"]) < 15.0 # Mark offline after 15s of silence
        node_data = {
            **info,
            "status": "ONLINE" if is_alive else "OFFLINE",
            "seconds_since_heartbeat": round(now - info["last_seen"], 1)
        }
        result.append(node_data)
    return {"total_nodes": len(result), "nodes": result}

@app.get("/api/cluster/summary")
def cluster_summary():
    """Cluster-wide capacity, storage metrics and deduplication stats."""
    now = time.time()
    total_free = sum(n["free_space_bytes"] for n in active_nodes.values())
    total_chunks = sum(n["chunk_count"] for n in active_nodes.values())
    online_count = sum(1 for n in active_nodes.values() if (now - n["last_seen"]) < 15.0)

    dedup_stats = db.get_dedup_statistics()

    return {
        "online_nodes": online_count,
        "total_nodes": len(active_nodes),
        "total_free_space_gb": round(total_free / (1024**3), 2),
        "total_chunks_stored": total_chunks,
        "deduplication": dedup_stats
    }

# --- Task 2: Object Metadata & Deduplication Endpoints ---

@app.post("/api/objects", status_code=status.HTTP_201_CREATED)
def register_object(req: RegisterObjectRequest):
    """Registers object metadata and performs chunk deduplication with reference counting."""
    chunks_data = [c.dict() for c in req.chunks]
    result = db.register_object(
        bucket=req.bucket,
        key=req.key,
        size=req.size,
        chunks=chunks_data,
        version=req.version
    )
    return result

@app.get("/api/objects/{bucket}/{key}")
def get_object(bucket: str, key: str, version: Optional[int] = None):
    """Retrieve object metadata and ordered list of chunk hashes."""
    meta = db.get_object_metadata(bucket=bucket, key=key, version=version)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Object {bucket}/{key} not found")
    return meta

@app.delete("/api/objects/{bucket}/{key}")
def delete_object(bucket: str, key: str, version: Optional[int] = None):
    """Deletes object and decrements chunk reference counts."""
    res = db.delete_object(bucket=bucket, key=key, version=version)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=res["message"])
    return res

@app.get("/api/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    """Retrieves chunk reference count and assigned target storage nodes."""
    c = db.get_chunk(chunk_hash)
    if not c:
        raise HTTPException(status_code=404, detail="Chunk not found in metadata catalog")
    return c

@app.get("/api/dedup/stats")
def get_dedup_stats():
    """Returns cluster deduplication efficiency and space savings."""
    return db.get_dedup_statistics()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
