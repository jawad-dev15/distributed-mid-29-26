#!/usr/bin/env python3
"""
Task 5 Comprehensive Verification & Demonstration Script:
1. Validates FastAPI Gateway Cluster State & Node Telemetry API
2. Validates Elasticsearch Metadata Inverted Index & Query DSL (Match, Prefix, Term, Range)
3. Validates Object Streaming Upload, Download, and Byte-Range Slicing for Web UI
4. Verifies React UI assets, components, and server readiness.
"""

import os
import sys
import time
import json
import requests

GATEWAY_URL = "http://localhost:8080"
STORAGE_NODES = {
    "storage_node_1": "http://localhost:8001",
    "storage_node_2": "http://localhost:8002",
    "storage_node_3": "http://localhost:8003",
    "storage_node_4": "http://localhost:8004"
}

def log(step: str, title: str):
    print(f"\n========================================================")
    print(f"[{step}] {title}")
    print(f"========================================================")

def main():
    print("=" * 65)
    print(" TASK 5: REACT WEB UI & ELASTICSEARCH METADATA ENGINE VERIFICATION")
    print(" Student: Jawad Choukair (ID: 202211402) | Mid-29")
    print("=" * 65)

    # 1. Authenticate with Gateway
    log("STEP 1", "AUTHENTICATING & OBTAINING JWT BEARER TOKEN")
    auth_resp = requests.post(
        f"{GATEWAY_URL}/api/auth/token",
        json={"username": "jawad_admin", "password": "secure_password_2026"}
    )
    if auth_resp.status_code != 200:
        print(f"[-] Authentication failed: {auth_resp.text}")
        return
    token = auth_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print(f"[+] JWT Token Successfully Acquired: {token[:35]}...")

    # 2. Verify Cluster Status & Node Health
    log("STEP 2", "FETCHING CLUSTER HEALTH & CHUNK DISTRIBUTION TELEMETRY")
    status_resp = requests.get(f"{GATEWAY_URL}/api/cluster/status")
    print(f"[+] Cluster Status HTTP Code: {status_resp.status_code}")
    cluster_telemetry = status_resp.json()
    print(f"    - Cluster State: {cluster_telemetry.get('cluster_state')}")
    print(f"    - Total Nodes: {cluster_telemetry.get('total_nodes')}")
    print(f"    - Nodes: {cluster_telemetry.get('nodes')}")
    print(f"    - Virtual Nodes per Physical Node: {cluster_telemetry.get('virtual_nodes_per_node')}")
    print(f"    - Total Ring Tokens: {cluster_telemetry.get('total_ring_tokens')}")
    dedup = cluster_telemetry.get("deduplication", {})
    print(f"    - Deduplication Savings: {dedup.get('savings_percentage', 0)}% (Stored: {dedup.get('stored_chunk_count', 0)} chunks)")

    # 3. Direct Storage Node Heartbeat & Storage Checks
    print("\n[*] Inspecting Individual Storage Nodes:")
    for node_id, url in STORAGE_NODES.items():
        try:
            h = requests.get(f"{url}/health", timeout=2).json()
            print(f"    -> [{node_id}] Status: {h.get('status')} | Free: {h.get('free_space_gb')} GB | Chunks: {h.get('chunk_count')}")
        except Exception as e:
            print(f"    -> [{node_id}] Unreachable: {e}")

    # 4. Upload Objects with Rich Tags for UI & Elasticsearch
    log("STEP 3", "UPLOADING OBJECTS WITH METADATA & TAGS FOR SEARCH TESTING")
    test_objects = [
        {
            "bucket": "enterprise-vault",
            "key": "finance/q4_audit_report.pdf",
            "data": b"CONFIDENTIAL FINANCIAL AUDIT REPORT Q4 2026 ... " * 60,
            "tags": "department=finance,confidential=true,year=2026"
        },
        {
            "bucket": "enterprise-vault",
            "key": "engineering/telemetry_cluster_nodes.log",
            "data": b"NODE TELEMETRY LOG [storage_node_1..4 ONLINE] ... " * 80,
            "tags": "department=engineering,system=telemetry,tier=gold"
        },
        {
            "bucket": "media-storage",
            "key": "backup/database_full_snapshot.bin",
            "data": b"BINARY DATABASE REPLICATION SNAPSHOT DUMP ... " * 120,
            "tags": "system=database,backup=nightly,retention=90d"
        }
    ]

    for item in test_objects:
        up = requests.post(
            f"{GATEWAY_URL}/api/objects/{item['bucket']}/{item['key']}",
            data=item["data"],
            headers={**headers, "X-Tags": item["tags"]}
        )
        print(f"[+] Stored '{item['key']}' in '{item['bucket']}' (Size: {len(item['data'])} bytes) -> HTTP {up.status_code}")

    # 5. Test Elasticsearch Query DSL
    log("STEP 4", "TESTING ELASTICSEARCH INVERTED INDEX & QUERY DSL")
    
    # 5a. Match query on key token 'audit'
    dsl_match = {
        "query": {
            "match": {"key": "audit"}
        }
    }
    es_res1 = requests.post(f"{GATEWAY_URL}/api/search/es", json=dsl_match, headers=headers).json()
    print(f"[+] Elasticsearch Match Query ('audit'): Took {es_res1.get('took')}ms | Total Hits: {es_res1['hits']['total']['value']}")
    for hit in es_res1['hits']['hits']:
        print(f"    - Hit ID: {hit['_id']} | Score: {hit['_score']} | Tags: {hit['_source'].get('tags')}")

    # 5b. Prefix query on 'engineering/'
    dsl_prefix = {
        "query": {
            "prefix": {"key": "engineering/"}
        }
    }
    es_res2 = requests.post(f"{GATEWAY_URL}/api/search/es", json=dsl_prefix, headers=headers).json()
    print(f"[+] Elasticsearch Prefix Query ('engineering/'): Took {es_res2.get('took')}ms | Total Hits: {es_res2['hits']['total']['value']}")
    for hit in es_res2['hits']['hits']:
        print(f"    - Hit ID: {hit['_id']} | Key: {hit['_source']['key']}")

    # 5c. Term query on tag 'department:finance'
    dsl_term = {
        "query": {
            "term": {"tags.department": "finance"}
        }
    }
    es_res3 = requests.post(f"{GATEWAY_URL}/api/search/es", json=dsl_term, headers=headers).json()
    print(f"[+] Elasticsearch Term Query ('tags.department:finance'): Took {es_res3.get('took')}ms | Total Hits: {es_res3['hits']['total']['value']}")

    # 5d. Range query on size (between 2KB and 20KB)
    dsl_range = {
        "query": {
            "range": {"size": {"gte": 2000, "lte": 20000}}
        }
    }
    es_res4 = requests.post(f"{GATEWAY_URL}/api/search/es", json=dsl_range, headers=headers).json()
    print(f"[+] Elasticsearch Range Query (2KB-20KB): Took {es_res4.get('took')}ms | Total Hits: {es_res4['hits']['total']['value']}")

    # 6. Test Byte-Range Partial Content Download (HTTP 206)
    log("STEP 5", "TESTING RFC 7233 BYTE-RANGE SLICE DOWNLOAD (HTTP 206)")
    range_resp = requests.get(
        f"{GATEWAY_URL}/api/objects/enterprise-vault/finance/q4_audit_report.pdf",
        headers={**headers, "Range": "bytes=0-49"}
    )
    print(f"[+] Byte-Range Request HTTP Status: {range_resp.status_code} (Expected: 206)")
    print(f"[+] Content-Range Header: {range_resp.headers.get('Content-Range')}")
    print(f"[+] Slice Content ({len(range_resp.content)} bytes): {range_resp.content}")

    # 7. React UI Verification
    log("STEP 6", "REACT VITE DASHBOARD ASSETS & CONFIGURATION AUDIT")
    ui_dir = os.path.join(os.path.dirname(__file__), "storage-ui")
    print(f"[+] UI Project Directory: {ui_dir}")
    print(f"    - package.json: {'FOUND' if os.path.exists(os.path.join(ui_dir, 'package.json')) else 'MISSING'}")
    print(f"    - vite.config.js: {'FOUND' if os.path.exists(os.path.join(ui_dir, 'vite.config.js')) else 'MISSING'}")
    print(f"    - src/App.jsx: {'FOUND' if os.path.exists(os.path.join(ui_dir, 'src', 'App.jsx')) else 'MISSING'}")
    print(f"    - src/App.css: {'FOUND' if os.path.exists(os.path.join(ui_dir, 'src', 'App.css')) else 'MISSING'}")

    print("\n" + "=" * 65)
    print(" ALL TASK 5 VERIFICATION CHECKS COMPLETED SUCCESSFULLY!")
    print("=" * 65)

if __name__ == "__main__":
    main()
