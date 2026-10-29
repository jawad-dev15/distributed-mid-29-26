import hashlib
import bisect
from typing import List, Dict, Set, Optional, Tuple, Any

class ConsistentHashRing:
    """
    Consistent Hashing Ring with Virtual Nodes.
    Distributes chunk hashes uniformly across storage nodes and guarantees minimal
    data movement (~1/N migration) when nodes are added or removed.
    """
    def __init__(self, nodes: Optional[List[str]] = None, virtual_nodes: int = 150):
        self.virtual_nodes: int = virtual_nodes
        self.ring: Dict[int, str] = {}           # hash_val -> physical_node_id
        self.sorted_keys: List[int] = []         # sorted hash_vals
        self.physical_nodes: Set[str] = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        """Deterministic 32-bit integer hash from SHA-256 for ring placement."""
        return int(hashlib.sha256(key.encode('utf-8')).hexdigest()[:8], 16)

    def add_node(self, node: str) -> None:
        """Adds a physical node by creating multiple virtual node tokens on the ring."""
        if node in self.physical_nodes:
            return
        self.physical_nodes.add(node)
        for i in range(self.virtual_nodes):
            v_token = f"{node}#vn{i}"
            h = self._hash(v_token)
            self.ring[h] = node
            bisect.insort(self.sorted_keys, h)

    def remove_node(self, node: str) -> None:
        """Removes a physical node and all its virtual tokens from the ring."""
        if node not in self.physical_nodes:
            return
        self.physical_nodes.remove(node)
        tokens_to_remove = []
        for i in range(self.virtual_nodes):
            v_token = f"{node}#vn{i}"
            h = self._hash(v_token)
            if h in self.ring:
                del self.ring[h]
                tokens_to_remove.append(h)
        
        self.sorted_keys = [k for k in self.sorted_keys if k not in tokens_to_remove]

    def get_node(self, chunk_hash: str) -> Optional[str]:
        """
        Maps a chunk SHA-256 hash to the nearest storage node clockwise on the ring.
        """
        if not self.ring:
            return None
        h = self._hash(chunk_hash)
        idx = bisect.bisect_right(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]

    def get_nodes(self, chunk_hash: str, count: int) -> List[str]:
        """
        Finds 'count' distinct physical storage nodes clockwise from the chunk hash
        (used for Quorum Replication and Erasure Coding placement).
        """
        if not self.ring or count <= 0:
            return []
        
        needed = min(count, len(self.physical_nodes))
        h = self._hash(chunk_hash)
        idx = bisect.bisect_right(self.sorted_keys, h)
        
        selected: List[str] = []
        n = len(self.sorted_keys)
        for i in range(n):
            curr_idx = (idx + i) % n
            node = self.ring[self.sorted_keys[curr_idx]]
            if node not in selected:
                selected.append(node)
                if len(selected) == needed:
                    break
        return selected

    def distribution_stats(self, chunk_hashes: List[str]) -> Dict[str, int]:
        """Calculates distribution of chunk allocations across physical nodes."""
        stats = {node: 0 for node in self.physical_nodes}
        for ch in chunk_hashes:
            target = self.get_node(ch)
            if target in stats:
                stats[target] += 1
        return stats

    def test_placement_migration(
        self,
        chunk_hashes: List[str],
        test_new_node: str = "storage_node_5",
        test_remove_node: str = "storage_node_2"
    ) -> Dict[str, Any]:
        """
        Simulates and benchmarks chunk migration percentages upon adding and removing nodes.
        Verifies theoretical 1/N migration rate vs naive modulo hashing.
        """
        total_chunks = len(chunk_hashes)
        
        # 1. Baseline placement
        initial_placement = {ch: self.get_node(ch) for ch in chunk_hashes}
        initial_node_count = len(self.physical_nodes)

        # 2. Add Node Test
        self.add_node(test_new_node)
        add_placement = {ch: self.get_node(ch) for ch in chunk_hashes}
        
        chunks_moved_on_add = sum(
            1 for ch in chunk_hashes if add_placement[ch] != initial_placement[ch]
        )
        pct_moved_add = (chunks_moved_on_add / total_chunks) * 100.0
        theoretical_add_pct = (1.0 / (initial_node_count + 1)) * 100.0

        # Naive modulo hash migration comparison for same scenario:
        # hash % 4 vs hash % 5 -> moves ~80% of items!
        naive_moves = sum(
            1 for ch in chunk_hashes 
            if (self._hash(ch) % initial_node_count) != (self._hash(ch) % (initial_node_count + 1))
        )
        naive_pct = (naive_moves / total_chunks) * 100.0

        # 3. Remove Node Test
        self.remove_node(test_remove_node)
        remove_placement = {ch: self.get_node(ch) for ch in chunk_hashes}
        chunks_moved_on_remove = sum(
            1 for ch in chunk_hashes if remove_placement[ch] != add_placement[ch]
        )
        pct_moved_remove = (chunks_moved_on_remove / total_chunks) * 100.0

        # Reset ring back to baseline
        self.remove_node(test_new_node)
        self.add_node(test_remove_node)

        return {
            "total_chunks_tested": total_chunks,
            "initial_nodes": initial_node_count,
            "add_node_test": {
                "added_node": test_new_node,
                "chunks_moved": chunks_moved_on_add,
                "percentage_moved": round(pct_moved_add, 2),
                "theoretical_percentage": round(theoretical_add_pct, 2),
                "naive_modulo_percentage": round(naive_pct, 2)
            },
            "remove_node_test": {
                "removed_node": test_remove_node,
                "chunks_moved": chunks_moved_on_remove,
                "percentage_moved": round(pct_moved_remove, 2)
            }
        }
