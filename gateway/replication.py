import hashlib
from typing import List, Dict, Any, Optional, Tuple
import httpx
from gateway.consistent_hash import ConsistentHashRing

class QuorumError(Exception):
    pass

class QuorumWriteError(QuorumError):
    pass

class QuorumReadError(QuorumError):
    pass

class ReplicationManager:
    """
    Manages Quorum-based Replication across Distributed Storage Nodes.
    Configured with Replication Factor N, Write Quorum W, and Read Quorum R (W + R > N).
    """
    def __init__(self, n: int = 3, w: int = 2, r: int = 2, node_urls: Optional[Dict[str, str]] = None):
        self.n = n
        self.w = w
        self.r = r
        if self.w + self.r <= self.n:
            raise ValueError(f"Quorum condition violated: W ({w}) + R ({r}) must be > N ({n})")
        
        # Default local ports for the 4+ storage node containers
        self.node_urls = node_urls or {
            "storage_node_1": "http://localhost:8001",
            "storage_node_2": "http://localhost:8002",
            "storage_node_3": "http://localhost:8003",
            "storage_node_4": "http://localhost:8004",
            "storage_node_5": "http://localhost:8005"
        }

    def write_chunk(
        self,
        chunk_hash: str,
        data: bytes,
        ring: ConsistentHashRing,
        target_nodes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Writes a chunk to N storage nodes using Write Quorum W.
        Returns success once at least W nodes confirm HTTP 201.
        """
        if not target_nodes:
            target_nodes = ring.get_nodes(chunk_hash, count=self.n)

        if len(target_nodes) < self.w:
            raise QuorumWriteError(
                f"Cannot meet write quorum: requested W={self.w}, but only {len(target_nodes)} target nodes available"
            )

        successful_nodes: List[str] = []
        failed_nodes: List[str] = []

        with httpx.Client(timeout=4.0) as client:
            for node_id in target_nodes:
                base_url = self.node_urls.get(node_id, f"http://localhost:{8000 + int(node_id.split('_')[-1])}")
                put_url = f"{base_url}/chunks/{chunk_hash}"
                try:
                    resp = client.put(put_url, content=data)
                    if resp.status_code == 201:
                        successful_nodes.append(node_id)
                    else:
                        failed_nodes.append(f"{node_id} (HTTP {resp.status_code})")
                except Exception as ex:
                    failed_nodes.append(f"{node_id} ({str(ex)})")

        if len(successful_nodes) >= self.w:
            return {
                "status": "quorum_achieved",
                "chunk_hash": chunk_hash,
                "n": self.n,
                "w": self.w,
                "successful_nodes": successful_nodes,
                "failed_nodes": failed_nodes,
                "quorum_met": True
            }
        else:
            raise QuorumWriteError(
                f"Quorum write failed for chunk {chunk_hash[:12]}... "
                f"Only {len(successful_nodes)} nodes succeeded (required W={self.w}). Failures: {failed_nodes}"
            )

    def read_chunk(
        self,
        chunk_hash: str,
        target_nodes: List[str]
    ) -> Tuple[bytes, List[str], List[str]]:
        """
        Reads a chunk using Read Quorum R.
        Verifies SHA-256 hash on retrieval. Detects and reports corrupted/degraded replicas.
        Returns: (data_bytes, healthy_nodes, degraded_or_failed_nodes)
        """
        healthy_nodes: List[str] = []
        degraded_nodes: List[str] = []
        valid_payload: Optional[bytes] = None

        with httpx.Client(timeout=4.0) as client:
            for node_id in target_nodes:
                base_url = self.node_urls.get(node_id, f"http://localhost:{8000 + int(node_id.split('_')[-1])}")
                get_url = f"{base_url}/chunks/{chunk_hash}"
                try:
                    resp = client.get(get_url)
                    if resp.status_code == 200:
                        payload = resp.content
                        if hashlib.sha256(payload).hexdigest() == chunk_hash:
                            healthy_nodes.append(node_id)
                            if valid_payload is None:
                                valid_payload = payload
                        else:
                            degraded_nodes.append(f"{node_id} (Hash mismatch)")
                    elif resp.status_code == 500: # Bit-rot detected on storage node
                        degraded_nodes.append(f"{node_id} (Bit-rot HTTP 500)")
                    else:
                        degraded_nodes.append(f"{node_id} (HTTP {resp.status_code})")
                except Exception as ex:
                    degraded_nodes.append(f"{node_id} ({str(ex)})")

        if len(healthy_nodes) >= self.r and valid_payload is not None:
            return valid_payload, healthy_nodes, degraded_nodes
        else:
            raise QuorumReadError(
                f"Quorum read failed for chunk {chunk_hash[:12]}... "
                f"Only {len(healthy_nodes)} healthy replicas found (required R={self.r}). Degraded: {degraded_nodes}"
            )
