import os
import sys
import time
import subprocess

def main():
    python_exe = sys.executable
    base_dir = os.path.abspath(os.path.dirname(__file__))

    nodes = [
        {"id": "storage_node_1", "port": "8001"},
        {"id": "storage_node_2", "port": "8002"},
        {"id": "storage_node_3", "port": "8003"},
        {"id": "storage_node_4", "port": "8004"},
    ]

    print("[*] Starting Metadata Registry Service on port 8000...")
    meta_env = os.environ.copy()
    meta_proc = subprocess.Popen([python_exe, "metadata_service/app.py"], cwd=base_dir, env=meta_env)

    time.sleep(1.5)

    node_procs = []
    for n in nodes:
        node_env = os.environ.copy()
        node_env["NODE_ID"] = n["id"]
        node_env["PORT"] = n["port"]
        node_env["STORAGE_DIR"] = f"./data/{n['id']}/chunks"
        node_env["METADATA_URL"] = "http://localhost:8000/api/heartbeat"
        node_env["HEARTBEAT_INTERVAL"] = "5"
        
        print(f"[*] Starting {n['id']} on port {n['port']}...")
        p = subprocess.Popen([python_exe, "storage_node/app.py"], cwd=base_dir, env=node_env)
        node_procs.append(p)

    print("\n[+] All 4 storage nodes + metadata registry service running locally!")
    print("Press CTRL+C to terminate all services.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[-] Stopping all cluster processes...")
        meta_proc.terminate()
        for p in node_procs:
            p.terminate()
        print("[+] Cluster stopped.")

if __name__ == "__main__":
    main()
