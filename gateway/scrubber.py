import json
import hashlib
import httpx
from typing import List, Dict, Any, Optional
from metadata_service.metadata_db import MetadataDatabase, StoredChunk

class BitRotScrubber:
    """
    Background Bit-Rot Scrubber.
    Periodically re-reads chunks across all storage nodes, detects silent disk corruption
    (bit-rot or tampering), and heals damaged replicas from healthy peer nodes.
    """
    def __init__(self, db: MetadataDatabase, node_urls: Optional[Dict[str, str]] = None):
        self.db = db
        self.node_urls = node_urls or {
            "storage_node_1": "http://localhost:8001",
            "storage_node_2": "http://localhost:8002",
            "storage_node_3": "http://localhost:8003",
            "storage_node_4": "http://localhost:8004",
            "storage_node_5": "http://localhost:8005"
        }

    def scrub_and_heal_chunk(
        self,
        chunk_hash: str,
        target_nodes: List[str]
    ) -> Dict[str, Any]:
        """
        Scans all replica copies of a single chunk. If bit-rot is detected on any node,
        fetches clean data from a healthy node and heals the damaged replica.
        """
        healthy_node = None
        healthy_payload = None
        corrupted_nodes = []
        healed_nodes = []

        with httpx.Client(timeout=4.0) as client:
            # 1. Inspect all replicas
            for node_id in target_nodes:
                base_url = self.node_urls.get(node_id)
                if not base_url:
                    continue
                try:
                    resp = client.get(f"{base_url}/chunks/{chunk_hash}")
                    if resp.status_code == 200:
                        content = resp.content
                        if hashlib.sha256(content).hexdigest() == chunk_hash:
                            if healthy_payload is None:
                                healthy_payload = content
                                healthy_node = node_id
                        else:
                            corrupted_nodes.append(node_id)
                    elif resp.status_code == 500: # Bit-rot caught by storage node integrity check
                        corrupted_nodes.append(node_id)
                except Exception:
                    continue

            # 2. Heal corrupted replicas from healthy copy
            if corrupted_nodes and healthy_payload is not None:
                for bad_node in corrupted_nodes:
                    bad_url = self.node_urls.get(bad_node)
                    if bad_url:
                        # PUT clean payload to heal the on-disk corruption
                        put_resp = client.put(f"{bad_url}/chunks/{chunk_hash}", content=healthy_payload)
                        if put_resp.status_code == 201:
                            # Verify healed state
                            verify_resp = client.get(f"{bad_url}/chunks/{chunk_hash}")
                            if verify_resp.status_code == 200:
                                healed_nodes.append(bad_node)

        return {
            "chunk_hash": chunk_hash,
            "corrupted_nodes_found": corrupted_nodes,
            "healthy_source_node": healthy_node,
            "healed_nodes": healed_nodes,
            "is_healed": len(healed_nodes) == len(corrupted_nodes) if corrupted_nodes else True
        }

    def scrub_entire_catalog(self) -> Dict[str, Any]:
        """
        Runs a full scrubber pass across all objects and chunks registered in metadata.
        """
        session = self.db.SessionLocal()
        all_chunks = []
        try:
            records = session.query(StoredChunk).all()
            for r in records:
                all_chunks.append((r.chunk_hash, json.loads(r.target_nodes)))
        finally:
            session.close()

        scanned = 0
        corruptions_detected = 0
        repaired_count = 0
        audit_log = []

        for c_hash, nodes in all_chunks:
            res = self.scrub_and_heal_chunk(c_hash, nodes)
            scanned += 1
            if res["corrupted_nodes_found"]:
                corruptions_detected += len(res["corrupted_nodes_found"])
                repaired_count += len(res["healed_nodes"])
                audit_log.append(res)

        return {
            "total_chunks_scanned": scanned,
            "corrupted_replicas_found": corruptions_detected,
            "replicas_healed": repaired_count,
            "audit_log": audit_log
        }
