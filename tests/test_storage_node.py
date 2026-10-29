import os
import time
import hashlib
import requests
import pytest

NODE_PORTS = [8001, 8002, 8003, 8004]
NODE_URLS = [f"http://localhost:{port}" for port in NODE_PORTS]
METADATA_URL = "http://localhost:8000"

def test_node_health():
    """Verify all 4 storage nodes and metadata service respond to health check."""
    for url in NODE_URLS:
        res = requests.get(f"{url}/health", timeout=3.0)
        assert res.status_code == 200, f"Node {url} failed health check: {res.text}"
        data = res.json()
        assert data["status"] == "healthy"

def test_chunk_put_get_head_delete():
    """Test full chunk lifecycle on Storage Node 1."""
    node_url = NODE_URLS[0]
    payload = b"Distributed Object-Tolerant Storage - Test Chunk Data 123456789"
    chunk_hash = hashlib.sha256(payload).hexdigest()

    # 1. PUT chunk
    put_res = requests.put(f"{node_url}/chunks/{chunk_hash}", data=payload)
    assert put_res.status_code == 201
    assert put_res.json()["chunk_hash"] == chunk_hash

    # 2. HEAD chunk
    head_res = requests.head(f"{node_url}/chunks/{chunk_hash}")
    assert head_res.status_code == 200
    assert head_res.headers.get("X-Chunk-Hash") == chunk_hash
    assert int(head_res.headers.get("Content-Length")) == len(payload)

    # 3. GET chunk
    get_res = requests.get(f"{node_url}/chunks/{chunk_hash}")
    assert get_res.status_code == 200
    assert get_res.content == payload

    # 4. DELETE chunk
    del_res = requests.delete(f"{node_url}/chunks/{chunk_hash}")
    assert del_res.status_code == 200

    # 5. Verify 404 after delete
    get_after_del = requests.get(f"{node_url}/chunks/{chunk_hash}")
    assert get_after_del.status_code == 404

def test_chunk_hash_mismatch_rejection():
    """Verify node strictly rejects chunks if SHA-256 does not match."""
    node_url = NODE_URLS[1]
    payload = b"Genuine Payload"
    fake_hash = "0000000000000000000000000000000000000000000000000000000000000000"

    res = requests.put(f"{node_url}/chunks/{fake_hash}", data=payload)
    assert res.status_code == 400
    assert "Hash mismatch" in res.text

def test_bit_rot_detection():
    """Simulate disk bit-rot and verify node detects corruption upon read."""
    node_url = NODE_URLS[2]
    payload = b"Critical data block subject to potential magnetic bit-rot"
    chunk_hash = hashlib.sha256(payload).hexdigest()

    # Store genuine chunk
    put_res = requests.put(f"{node_url}/chunks/{chunk_hash}", data=payload)
    assert put_res.status_code == 201

    # Tamper with chunk file on disk directly (simulate bit-rot)
    chunk_file = f"./data/storage_node_3/chunks/{chunk_hash}.chunk"
    if not os.path.exists(chunk_file):
        chunk_file = f"./data/node_3/chunks/{chunk_hash}.chunk"

    if os.path.exists(chunk_file):
        with open(chunk_file, "r+b") as f:
            f.seek(5)
            f.write(b"CORRUPTED_BYTE")

        # GET request must detect hash mismatch and return error
        get_res = requests.get(f"{node_url}/chunks/{chunk_hash}")
        assert get_res.status_code == 500
        assert "Bit-rot detected" in get_res.text

        # Cleanup
        os.remove(chunk_file)

def test_cluster_heartbeats():
    """Verify all 4 storage nodes send periodic heartbeats to metadata registry."""
    # Allow heartbeats to register
    time.sleep(3)
    res = requests.get(f"{METADATA_URL}/api/nodes", timeout=3.0)
    assert res.status_code == 200
    nodes = res.json()["nodes"]
    assert len(nodes) >= 4, f"Expected 4 nodes registered, got {len(nodes)}"
    for n in nodes:
        assert n["status"] == "ONLINE"
        assert n["free_space_bytes"] > 0

if __name__ == "__main__":
    pytest.main(["-v", __file__])
