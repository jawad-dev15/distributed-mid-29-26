import hashlib
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass

@dataclass
class Chunk:
    chunk_hash: str
    size: int
    data: bytes
    offset: int

    def __repr__(self):
        return f"Chunk(hash={self.chunk_hash[:12]}..., size={self.size}, offset={self.offset})"

def hash_data(data: bytes) -> str:
    """Computes SHA-256 hexadecimal hash for chunk content addressing."""
    return hashlib.sha256(data).hexdigest()

def fixed_size_chunking(data: bytes, chunk_size: int = 4096) -> List[Chunk]:
    """
    Splits data into fixed-size chunks (default 4 KB).
    The last chunk may be smaller than chunk_size.
    """
    chunks: List[Chunk] = []
    total_len = len(data)
    offset = 0

    if total_len == 0:
        return chunks

    while offset < total_len:
        end = min(offset + chunk_size, total_len)
        chunk_bytes = data[offset:end]
        c_hash = hash_data(chunk_bytes)
        chunks.append(Chunk(chunk_hash=c_hash, size=len(chunk_bytes), data=chunk_bytes, offset=offset))
        offset = end

    return chunks

# 256-entry Gear Hash table for FastCDC rolling fingerprint
GEAR_MATRIX = [
    0x8a970e7e, 0xa5a7538a, 0x5a18a994, 0x534e793e, 0x2213e903, 0xe116a4c2, 0xd2c0d024, 0x4858d249,
    0x2c4e3650, 0x76c3822a, 0x6e7e1742, 0x14022a10, 0x05b9b8b6, 0x2477d612, 0x07548773, 0x3a4b64a6,
    0x91893c8d, 0x82b9347d, 0x7593b4d6, 0xb2ea51d6, 0x7a87612c, 0x1870a562, 0xd0e6b52a, 0xb8022b72,
    0xfc08cf5e, 0x1c6999a0, 0xcaee27a2, 0x29ef995c, 0xb33887f3, 0x6e641774, 0x74f1bf45, 0x82c81a29,
    0x2035e165, 0x937ef390, 0x770a5ab3, 0x1cf9e26e, 0xa8f48e24, 0x6543b59a, 0xd275ff60, 0x33486c99,
    0x2045e486, 0x2f901844, 0x524cf92c, 0x2b21c436, 0x8244e857, 0x205562d2, 0xc27a4d55, 0xa834161a,
    0x8898165d, 0x5463f25c, 0x1a877c42, 0x74221162, 0x6c6b4124, 0x47b98d24, 0xd0df2444, 0x2b9c7924,
    0x54ec12d0, 0x47c29548, 0x68b92b6a, 0xb3448b11, 0x72c544d6, 0x25a4d455, 0x45bb29e0, 0x24ffc926,
    0x51b8a5d4, 0x8846c912, 0x524ea2a0, 0x41bbd424, 0x95c8992e, 0x3a48e26a, 0x61c77d46, 0xb4e112d8,
    0x7a28e511, 0x8b32115c, 0x6e902b42, 0x51b8d224, 0xd46c9444, 0x5287f42c, 0xb9244c11, 0x5447a950,
    0x2a1c6a44, 0x7295d462, 0xb8277c12, 0x20c918a2, 0x41e8d424, 0x72a5b611, 0x548bd260, 0x4b77c924,
    0x5246a4d2, 0x6189e424, 0x7b2446a2, 0x51b8c2d6, 0x7486e92c, 0x41bbd512, 0x882a1744, 0x524e9b6a,
    0xd286c424, 0xb482276a, 0x51c77d42, 0x72a8b512, 0x6e41b924, 0x48bbd2a0, 0x25c88942, 0x524ea46c,
    0x7189c424, 0x47b92548, 0x68c22b11, 0x88b489d2, 0x54c54124, 0x61a4d455, 0x45bb29e0, 0x24ffc926,
    0x51b8a5d4, 0x8846c912, 0x524ea2a0, 0x41bbd424, 0x95c8992e, 0x3a48e26a, 0x61c77d46, 0xb4e112d8,
    0x7a28e511, 0x8b32115c, 0x6e902b42, 0x51b8d224, 0xd46c9444, 0x5287f42c, 0xb9244c11, 0x5447a950,
    0x2a1c6a44, 0x7295d462, 0xb8277c12, 0x20c918a2, 0x41e8d424, 0x72a5b611, 0x548bd260, 0x4b77c924,
    0x5246a4d2, 0x6189e424, 0x7b2446a2, 0x51b8c2d6, 0x7486e92c, 0x41bbd512, 0x882a1744, 0x524e9b6a,
    0xd286c424, 0xb482276a, 0x51c77d42, 0x72a8b512, 0x6e41b924, 0x48bbd2a0, 0x25c88942, 0x524ea46c,
    0x7189c424, 0x47b92548, 0x68c22b11, 0x88b489d2, 0x54c54124, 0x61a4d455, 0x45bb29e0, 0x24ffc926,
    0x31b8a5d4, 0x7846c912, 0x424ea2a0, 0x31bbd424, 0x85c8992e, 0x2a48e26a, 0x51c77d46, 0xa4e112d8,
    0x6a28e511, 0x7b32115c, 0x5e902b42, 0x41b8d224, 0xc46c9444, 0x4287f42c, 0xa9244c11, 0x4447a950,
    0x1a1c6a44, 0x6295d462, 0xa8277c12, 0x10c918a2, 0x31e8d424, 0x62a5b611, 0x448bd260, 0x3b77c924,
    0x4246a4d2, 0x5189e424, 0x6b2446a2, 0x41b8c2d6, 0x6486e92c, 0x31bbd512, 0x782a1744, 0x424e9b6a,
    0xc286c424, 0xa482276a, 0x41c77d42, 0x62a8b512, 0x5e41b924, 0x38bbd2a0, 0x15c88942, 0x424ea46c,
    0x6189c424, 0x37b92548, 0x58c22b11, 0x78b489d2, 0x44c54124, 0x51a4d455, 0x35bb29e0, 0x14ffc926,
    0x41b8a5d4, 0x7846c912, 0x424ea2a0, 0x31bbd424, 0x85c8992e, 0x2a48e26a, 0x51c77d46, 0xa4e112d8,
    0x6a28e511, 0x7b32115c, 0x5e902b42, 0x41b8d224, 0xc46c9444, 0x4287f42c, 0xa9244c11, 0x4447a950,
    0x1a1c6a44, 0x6295d462, 0xa8277c12, 0x10c918a2, 0x31e8d424, 0x62a5b611, 0x448bd260, 0x3b77c924,
    0x4246a4d2, 0x5189e424, 0x6b2446a2, 0x41b8c2d6, 0x6486e92c, 0x31bbd512, 0x782a1744, 0x424e9b6a,
    0xc286c424, 0xa482276a, 0x41c77d42, 0x62a8b512, 0x5e41b924, 0x38bbd2a0, 0x15c88942, 0x424ea46c,
    0x6189c424, 0x37b92548, 0x58c22b11, 0x78b489d2, 0x44c54124, 0x51a4d455, 0x35bb29e0, 0x14ffc926
]

def fastcdc_chunking(
    data: bytes,
    min_size: int = 1024,
    avg_size: int = 4096,
    max_size: int = 8192
) -> List[Chunk]:
    """
    Fast Content-Defined Chunking (FastCDC) algorithm.
    Uses rolling Gear hashing with two-level normalized chunking masks.
    Eliminates boundary shift problems when bytes are inserted/deleted.
    """
    chunks: List[Chunk] = []
    total_len = len(data)
    if total_len == 0:
        return chunks

    # Mask calculation for target average chunk size:
    # 4096 -> 12 bits mask. Normalized: MaskS (13 bits), MaskL (11 bits)
    bits = avg_size.bit_length() - 1
    mask_s = (1 << (bits + 1)) - 1  # 0x1FFF (13 bits)
    mask_l = (1 << (bits - 1)) - 1  # 0x07FF (11 bits)

    offset = 0
    while offset < total_len:
        remaining = total_len - offset
        if remaining <= min_size:
            chunk_bytes = data[offset:]
            chunks.append(Chunk(hash_data(chunk_bytes), len(chunk_bytes), chunk_bytes, offset))
            break

        limit = min(remaining, max_size)
        fingerprint = 0
        split_pos = limit  # Default to max_size boundary

        # FastCDC normalized window check
        for i in range(min_size, limit):
            b = data[offset + i]
            fingerprint = ((fingerprint << 1) + GEAR_MATRIX[b]) & 0xFFFFFFFF

            if i < avg_size:
                if (fingerprint & mask_s) == 0:
                    split_pos = i + 1
                    break
            else:
                if (fingerprint & mask_l) == 0:
                    split_pos = i + 1
                    break

        chunk_bytes = data[offset:offset + split_pos]
        chunks.append(Chunk(hash_data(chunk_bytes), len(chunk_bytes), chunk_bytes, offset))
        offset += split_pos

    return chunks

def evaluate_boundary_shift(
    original_data: bytes,
    modified_data: bytes,
    chunk_size: int = 4096
) -> Dict[str, Any]:
    """
    Evaluates and compares Fixed-Size Chunking vs FastCDC when a boundary shift
    occurs (e.g. byte insertion at the start of the data stream).
    """
    # 1. Fixed Size Chunking
    fixed_orig = fixed_size_chunking(original_data, chunk_size)
    fixed_mod = fixed_size_chunking(modified_data, chunk_size)

    fixed_orig_hashes = set(c.chunk_hash for c in fixed_orig)
    fixed_mod_hashes = set(c.chunk_hash for c in fixed_mod)
    fixed_shared = fixed_orig_hashes.intersection(fixed_mod_hashes)
    fixed_dedup_ratio = (len(fixed_shared) / max(len(fixed_orig_hashes), 1)) * 100.0

    # 2. Fast Content-Defined Chunking (FastCDC)
    cdc_orig = fastcdc_chunking(original_data, min_size=chunk_size // 4, avg_size=chunk_size, max_size=chunk_size * 2)
    cdc_mod = fastcdc_chunking(modified_data, min_size=chunk_size // 4, avg_size=chunk_size, max_size=chunk_size * 2)

    cdc_orig_hashes = set(c.chunk_hash for c in cdc_orig)
    cdc_mod_hashes = set(c.chunk_hash for c in cdc_mod)
    cdc_shared = cdc_orig_hashes.intersection(cdc_mod_hashes)
    cdc_dedup_ratio = (len(cdc_shared) / max(len(cdc_orig_hashes), 1)) * 100.0

    return {
        "fixed": {
            "orig_chunk_count": len(fixed_orig),
            "mod_chunk_count": len(fixed_mod),
            "reusable_chunks": len(fixed_shared),
            "dedup_efficiency_pct": round(fixed_dedup_ratio, 2)
        },
        "fastcdc": {
            "orig_chunk_count": len(cdc_orig),
            "mod_chunk_count": len(cdc_mod),
            "reusable_chunks": len(cdc_shared),
            "dedup_efficiency_pct": round(cdc_dedup_ratio, 2)
        }
    }
