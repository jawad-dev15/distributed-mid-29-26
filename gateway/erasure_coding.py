import hashlib
from typing import List, Dict, Any, Tuple
import zfec

class ErasureCoder:
    """
    Reed-Solomon (k + m) Erasure Coding implementation using zfec.
    Splits data into k data fragments and m parity fragments.
    Any k fragments are mathematically sufficient to reconstruct the full original object.
    """
    def __init__(self, k: int = 4, m: int = 2):
        self.k = k
        self.m = m
        self.total = k + m
        self.encoder = zfec.Encoder(self.k, self.total)
        self.decoder = zfec.Decoder(self.k, self.total)

    def encode(self, data: bytes) -> Dict[str, Any]:
        """
        Encodes data into k data fragments + m parity fragments.
        Returns fragments dictionary mapping index (0 to k+m-1) to fragment bytes,
        along with fragment hashes and metadata.
        """
        orig_length = len(data)
        if orig_length == 0:
            return {"original_size": 0, "fragments": {}, "k": self.k, "m": self.m}

        # Pad payload so its length is an exact multiple of k
        pad = (self.k - (orig_length % self.k)) % self.k
        padded_data = data + (b"\x00" * pad)
        block_size = len(padded_data) // self.k

        blocks = [padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)]
        encoded_fragments = self.encoder.encode(blocks)

        fragment_dict: Dict[int, bytes] = {}
        fragment_hashes: Dict[int, str] = {}

        for idx, frag in enumerate(encoded_fragments):
            fragment_dict[idx] = frag
            fragment_hashes[idx] = hashlib.sha256(frag).hexdigest()

        return {
            "original_size": orig_length,
            "padded_size": len(padded_data),
            "fragment_size": block_size,
            "k": self.k,
            "m": self.m,
            "total_fragments": self.total,
            "storage_multiplier": round(self.total / self.k, 2),
            "storage_overhead_pct": round(((self.total - self.k) / self.k) * 100.0, 1),
            "fragments": fragment_dict,
            "fragment_hashes": fragment_hashes
        }

    def decode(
        self,
        surviving_fragments: Dict[int, bytes],
        original_size: int
    ) -> bytes:
        """
        Reconstructs the original data from ANY k surviving fragments out of k+m.
        Tolerates the loss of up to m fragments.
        """
        if len(surviving_fragments) < self.k:
            raise ValueError(
                f"Cannot reconstruct: required at least k={self.k} fragments, but only received {len(surviving_fragments)}"
            )

        # Select the first k available fragments
        indices = list(surviving_fragments.keys())[:self.k]
        blocks = [surviving_fragments[idx] for idx in indices]

        reconstructed_blocks = self.decoder.decode(blocks, indices)
        full_data = b"".join(reconstructed_blocks)

        # Trim padding back to original length
        return full_data[:original_size]

    def reconstruct_lost_fragment(
        self,
        surviving_fragments: Dict[int, bytes],
        original_size: int,
        target_lost_index: int
    ) -> bytes:
        """
        Re-encodes and recovers a specific lost fragment (index target_lost_index)
        using the surviving fragments.
        """
        full_data = self.decode(surviving_fragments, original_size)
        re_encoded = self.encode(full_data)
        return re_encoded["fragments"][target_lost_index]
