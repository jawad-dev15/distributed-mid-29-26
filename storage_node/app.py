import os
import sys
import time
import shutil
import hashlib
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response, status
from pydantic import BaseModel

# Configuration from Environment
NODE_ID = os.environ.get("NODE_ID", "storage_node_1")
STORAGE_DIR = os.environ.get("STORAGE_DIR", f"./data/{NODE_ID}/chunks")
METADATA_URL = os.environ.get("METADATA_URL", "http://localhost:8000/api/heartbeat")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))
NODE_PORT = int(os.environ.get("PORT", "8001"))
NODE_HOST = os.environ.get("HOST", "127.0.0.1")

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)
START_TIME = time.time()

def get_chunk_path(chunk_hash: str) -> str:
    # Safe filename sanitization
    clean_hash = "".join(c for c in chunk_hash.lower() if c in "0123456789abcdef")
    return os.path.join(STORAGE_DIR, f"{clean_hash}.chunk")

def calculate_disk_stats():
    total, used, free = shutil.disk_usage(STORAGE_DIR)
    chunk_count = len([f for f in os.listdir(STORAGE_DIR) if f.endswith(".chunk")])
    return total, used, free, chunk_count

async def heartbeat_loop():
    """Background task to periodically report health to metadata registry."""
    print(f"[*] [{NODE_ID}] Starting heartbeat service -> {METADATA_URL} every {HEARTBEAT_INTERVAL}s")
    while True:
        try:
            total, used, free, chunk_count = calculate_disk_stats()
            payload = {
                "node_id": NODE_ID,
                "host": NODE_HOST,
                "port": NODE_PORT,
                "url": f"http://{NODE_HOST}:{NODE_PORT}",
                "total_space_bytes": total,
                "used_space_bytes": used,
                "free_space_bytes": free,
                "chunk_count": chunk_count,
                "uptime_seconds": round(time.time() - START_TIME, 2),
                "timestamp": time.time()
            }
            async with httpx.AsyncClient(timeout=3.0) as client:
                res = await client.post(METADATA_URL, json=payload)
                if res.status_code == 200:
                    pass # Heartbeat acknowledged
        except Exception as e:
            # Metadata service might be starting up or temporarily offline
            pass
        await asyncio.sleep(HEARTBEAT_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: spawn heartbeat loop
    task = asyncio.create_task(heartbeat_loop())
    yield
    # Shutdown: cancel task
    task.cancel()

app = FastAPI(
    title=f"Distributed Storage Node Service [{NODE_ID}]",
    description="Content-Addressed Storage Node exposing chunk-level PUT, GET, DELETE, and HEAD.",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
def health():
    return {"status": "healthy", "node_id": NODE_ID}

@app.get("/status")
def status_endpoint():
    total, used, free, chunk_count = calculate_disk_stats()
    return {
        "node_id": NODE_ID,
        "storage_dir": os.path.abspath(STORAGE_DIR),
        "total_space_bytes": total,
        "used_space_bytes": used,
        "free_space_bytes": free,
        "chunk_count": chunk_count,
        "uptime_seconds": round(time.time() - START_TIME, 2)
    }

@app.put("/chunks/{chunk_hash}", status_code=status.HTTP_201_CREATED)
async def put_chunk(chunk_hash: str, request: Request):
    """
    Store chunk addressed by its SHA-256 hash.
    Verifies hash before writing; rejects mismatched chunks with 400 Bad Request.
    """
    chunk_hash = chunk_hash.lower()
    data = await request.body()
    
    if not data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty chunk payload")

    # Verify SHA-256 content address
    computed_hash = hashlib.sha256(data).hexdigest().lower()
    if computed_hash != chunk_hash:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Hash mismatch: Provided {chunk_hash} but content evaluates to {computed_hash}"
        )

    chunk_path = get_chunk_path(chunk_hash)
    temp_path = f"{chunk_path}.tmp.{time.time()}"

    # Atomic write pattern to avoid incomplete chunks during crash
    with open(temp_path, "wb") as f:
        f.write(data)
    os.replace(temp_path, chunk_path)

    return {
        "node_id": NODE_ID,
        "chunk_hash": chunk_hash,
        "size_bytes": len(data),
        "status": "stored"
    }

@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    """
    Retrieve chunk from disk.
    Verifies cryptographic hash on EVERY read to detect bit-rot and tampering.
    """
    chunk_hash = chunk_hash.lower()
    chunk_path = get_chunk_path(chunk_hash)

    if not os.path.exists(chunk_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Chunk {chunk_hash} not found")

    with open(chunk_path, "rb") as f:
        data = f.read()

    # Integrity verification on read (Bit-rot detection)
    computed_hash = hashlib.sha256(data).hexdigest().lower()
    if computed_hash != chunk_hash:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Bit-rot detected! Chunk data corrupted: expected {chunk_hash}, found {computed_hash}"
        )

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Chunk-Hash": chunk_hash,
            "X-Node-ID": NODE_ID,
            "Content-Length": str(len(data))
        }
    )

@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    """
    Inspect chunk existence and size without downloading bytes.
    """
    chunk_hash = chunk_hash.lower()
    chunk_path = get_chunk_path(chunk_hash)

    if not os.path.exists(chunk_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chunk not found")

    file_size = os.path.getsize(chunk_path)
    return Response(
        status_code=status.HTTP_200_OK,
        headers={
            "X-Chunk-Hash": chunk_hash,
            "X-Node-ID": NODE_ID,
            "Content-Length": str(file_size)
        }
    )

@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    """
    Delete a chunk from local node disk.
    """
    chunk_hash = chunk_hash.lower()
    chunk_path = get_chunk_path(chunk_hash)

    if not os.path.exists(chunk_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chunk not found")

    try:
        os.remove(chunk_path)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    return {
        "node_id": NODE_ID,
        "chunk_hash": chunk_hash,
        "status": "deleted"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
