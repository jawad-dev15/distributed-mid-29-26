import time
import httpx
from typing import List, Dict, Any, Optional
from gateway.consistent_hash import ConsistentHashRing
from metadata_service.metadata_db import MetadataDatabase

class NodeFailureDetector:
    """
    Monitors cluster nodes and flags nodes as FAILED after missing heartbeats.
    """
    def __init__(self, metadata_url: str = "http://localhost:8000", heartbeat_timeout: float = 15.0):
        self.metadata_url = metadata_url
        self.heartbeat_timeout = heartbeat_timeout

    def check_node_health(self) -> Dict[str, Any]:
        """Queries metadata registry and classifies nodes as ONLINE or FAILED."""
        try:
            with httpx.Client(timeout=3.0) as client:
                resp = client.get(f"{self.metadata_url}/api/nodes")
                if resp.status_code == 200:
                    data = resp.json()
                    nodes = data.get("nodes", [])
                    online = []
                    failed = []
                    for n in nodes:
                        if n.get("status") == "ONLINE" and n.get("seconds_since_heartbeat", 0) <= self.heartbeat_timeout:
                            online.append(n["node_id"])
                        else:
                            failed.append(n["node_id"])
                    return {"online_nodes": online, "failed_nodes": failed, "total": len(nodes)}
        except Exception:
            pass
        return {"online_nodes": [], "failed_nodes": [], "total": 0}

class RepairWorker:
    """
    Executes automated repair jobs when a storage node fails or loses heartbeats.
    Reads surviving replicas from remaining healthy nodes, picks replacement nodes,
    and restores full redundancy.
    """
    def __init__(
        self,
        db: MetadataDatabase,
        ring: ConsistentHashRing,
        node_urls: Optional[Dict[str, str]] = None
    ):
        self.db = db
        self.ring = ring
        self.node_urls = node_urls or {
            "storage_node_1": "http://localhost:8001",
            "storage_node_2": "http://localhost:8002",
            "storage_node_3": "http://localhost:8003",
            "storage_node_4": "http://localhost:8004",
            "storage_node_5": "http://localhost:8005"
        }

    def repair_failed_node(self, failed_node_id: str) -> Dict[str, Any]:
        """
        Rebuilds all chunks that were housed on the failed node onto surviving nodes.
        """
        # 1. Remove failed node from consistent hash ring so it won't be chosen
        self.ring.remove_node(failed_node_id)

        # 2. Query all chunks in catalog that reside on failed_node_id
        session = self.db.SessionLocal()
        from metadata_service.metadata_db import StoredChunk
        import json

        chunks_to_repair = []
        try:
            all_chunks = session.query(StoredChunk).all()
            for c in all_chunks:
                nodes = json.loads(c.target_nodes)
                if failed_node_id in nodes:
                    chunks_to_repair.append((c.chunk_hash, c.size, nodes))
        finally:
            session.close()

        repaired_details = []
        total_traffic_bytes = 0

        with httpx.Client(timeout=4.0) as client:
            for c_hash, c_size, current_nodes in chunks_to_repair:
                surviving_nodes = [n for n in current_nodes if n != failed_node_id and n in self.ring.physical_nodes]
                
                # Fetch chunk payload from first available surviving replica
                payload = None
                source_node = None
                for s_node in surviving_nodes:
                    base_url = self.node_urls.get(s_node)
                    if base_url:
                        try:
                            r = client.get(f"{base_url}/chunks/{c_hash}")
                            if r.status_code == 200:
                                payload = r.content
                                source_node = s_node
                                total_traffic_bytes += len(payload)
                                break
                        except Exception:
                            continue

                if payload is None:
                    continue # Cannot repair if no healthy replicas survive

                # Pick a new replacement node from the ring not already holding this chunk
                all_candidates = self.ring.get_nodes(c_hash, count=len(self.ring.physical_nodes))
                replacement_node = next((n for n in all_candidates if n not in current_nodes and n != failed_node_id), None)

                if replacement_node:
                    rep_url = self.node_urls.get(replacement_node)
                    if rep_url:
                        put_resp = client.put(f"{rep_url}/chunks/{c_hash}", content=payload)
                        if put_resp.status_code == 201:
                            # Update target nodes in metadata catalog
                            new_node_list = [n for n in current_nodes if n != failed_node_id] + [replacement_node]
                            session = self.db.SessionLocal()
                            try:
                                chunk_rec = session.query(StoredChunk).filter_by(chunk_hash=c_hash).first()
                                if chunk_rec:
                                    chunk_rec.target_nodes = json.dumps(new_node_list)
                                    session.commit()
                            finally:
                                session.close()

                            total_traffic_bytes += len(payload)
                            repaired_details.append({
                                "chunk_hash": c_hash,
                                "source_node": source_node,
                                "replacement_node": replacement_node,
                                "size_bytes": len(payload)
                            })

        return {
            "failed_node": failed_node_id,
            "chunks_inspected": len(chunks_to_repair),
            "chunks_repaired": len(repaired_details),
            "network_traffic_bytes": total_traffic_bytes,
            "repaired_chunks": repaired_details
        }
