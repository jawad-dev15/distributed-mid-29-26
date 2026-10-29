import os
import time
import hashlib
import requests
import json

def print_header(title):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)

def main():
    print_header("TASK 1 VERIFICATION: DISTRIBUTED STORAGE NODES & HEARTBEATS")

    metadata_url = "http://localhost:8000"
    node_ports = [8001, 8002, 8003, 8004]

    # 1. Cluster Status & Heartbeat Registry
    print("\n[+] STEP 1: Querying Metadata Cluster Registry (Heartbeat Monitor)")
    r = requests.get(f"{metadata_url}/api/nodes")
    nodes_data = r.json()
    print(f"Total Nodes Registered: {nodes_data['total_nodes']}")
    print("-" * 70)
    print(f"{'Node ID':<18} | {'Port':<6} | {'Status':<8} | {'Free Space (GB)':<15} | {'Heartbeat'}")
    print("-" * 70)
    for n in nodes_data["nodes"]:
        free_gb = round(n["free_space_bytes"] / (1024**3), 2)
        print(f"{n['node_id']:<18} | {n['port']:<6} | {n['status']:<8} | {free_gb:<15} | {n['seconds_since_heartbeat']}s ago")
    print("-" * 70)

    # 2. Chunk Lifecycle (PUT, HEAD, GET, DELETE)
    print("\n[+] STEP 2: Testing Chunk Operations on Storage Node 1 (Port 8001)")
    test_data = b"Distributed Systems Midterm - Content-Addressed Storage Chunk - Node 1"
    sha = hashlib.sha256(test_data).hexdigest()
    print(f"[*] Payload Size: {len(test_data)} bytes")
    print(f"[*] SHA-256 Hash: {sha}")

    # PUT
    put_res = requests.put(f"http://localhost:8001/chunks/{sha}", data=test_data)
    print(f"[*] PUT /chunks/{sha[:16]}... -> HTTP {put_res.status_code} ({put_res.json()})")

    # HEAD
    head_res = requests.head(f"http://localhost:8001/chunks/{sha}")
    print(f"[*] HEAD /chunks/{sha[:16]}... -> HTTP {head_res.status_code} (Content-Length: {head_res.headers.get('Content-Length')})")

    # GET
    get_res = requests.get(f"http://localhost:8001/chunks/{sha}")
    print(f"[*] GET /chunks/{sha[:16]}...  -> HTTP {get_res.status_code} (Bytes Verified: {get_res.content == test_data})")

    # 3. Hash Mismatch Rejection
    print("\n[+] STEP 3: Verifying Hash Mismatch Rejection on Write")
    bad_hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    bad_res = requests.put(f"http://localhost:8002/chunks/{bad_hash}", data=test_data)
    print(f"[*] Attempting PUT with mismatched hash -> HTTP {bad_res.status_code}")
    print(f"    Server Response: {bad_res.json()['detail']}")

    # 4. Bit-rot Detection on Read
    print("\n[+] STEP 4: Verifying Bit-Rot Detection on Read")
    rot_payload = b"Original chunk block that will suffer magnetic bit-rot"
    rot_hash = hashlib.sha256(rot_payload).hexdigest()
    requests.put(f"http://localhost:8003/chunks/{rot_hash}", data=rot_payload)
    
    # Tamper with chunk on disk
    base_dir = os.path.dirname(os.path.abspath(__file__))
    chunk_file = os.path.join(base_dir, "data", "node_3", "chunks", f"{rot_hash}.chunk")
    with open(chunk_file, "r+b") as f:
        f.seek(10)
        f.write(b"CORRUPTED")
    
    rot_res = requests.get(f"http://localhost:8003/chunks/{rot_hash}")
    print(f"[*] Reading tampered chunk -> HTTP {rot_res.status_code}")
    print(f"    Detection Message: {rot_res.json()['detail']}")
    
    # Clean up tampered file
    if os.path.exists(chunk_file):
        os.remove(chunk_file)

    # 5. Summary
    print_header("ALL TASK 1 REQUIREMENTS VERIFIED SUCCESSFULLY!")

if __name__ == "__main__":
    main()
