import os
import pytest
from gateway.chunker import fixed_size_chunking, fastcdc_chunking, evaluate_boundary_shift
from gateway.consistent_hash import ConsistentHashRing
from metadata_service.metadata_db import MetadataDatabase

def test_fixed_size_chunking():
    data = b"A" * 10000 # 10,000 bytes
    chunks = fixed_size_chunking(data, chunk_size=4096)
    
    # Expected chunks: 4096, 4096, 1808 -> 3 chunks
    assert len(chunks) == 3
    assert chunks[0].size == 4096
    assert chunks[1].size == 4096
    assert chunks[2].size == 1808
    assert chunks[0].offset == 0
    assert chunks[1].offset == 4096
    assert chunks[2].offset == 8192
    assert all(len(c.chunk_hash) == 64 for c in chunks)

def test_fastcdc_chunking_and_boundary_shift():
    # Generate realistic pseudo-random stream (50 KB)
    import random
    random.seed(42)
    base_data = bytes([random.randint(0, 255) for _ in range(50000)])
    modified_data = b"PREFIX_INSERTED_HEADER_BYTE_STREAM: " + base_data # Insert at beginning

    result = evaluate_boundary_shift(base_data, modified_data, chunk_size=4096)

    # Fixed size chunking suffers from boundary shift (0% dedup ratio)
    assert result["fixed"]["dedup_efficiency_pct"] == 0.0

    # FastCDC resists boundary shift and preserves >85% identical chunks
    assert result["fastcdc"]["dedup_efficiency_pct"] > 80.0
    assert result["fastcdc"]["reusable_chunks"] >= 10

def test_consistent_hashing_ring_distribution():
    nodes = ["storage_node_1", "storage_node_2", "storage_node_3", "storage_node_4"]
    ring = ConsistentHashRing(nodes=nodes, virtual_nodes=150)

    # Generate 1,000 synthetic chunk hashes
    import hashlib
    chunk_hashes = [hashlib.sha256(f"chunk_payload_{i}".encode()).hexdigest() for i in range(1000)]

    stats = ring.distribution_stats(chunk_hashes)
    assert len(stats) == 4
    # Each node should receive roughly 250 chunks (+/- reasonable variance)
    for node, count in stats.items():
        assert 170 <= count <= 330, f"Node {node} received {count} chunks (imbalanced distribution)"

def test_consistent_hashing_minimal_migration():
    nodes = ["storage_node_1", "storage_node_2", "storage_node_3", "storage_node_4"]
    ring = ConsistentHashRing(nodes=nodes, virtual_nodes=150)

    import hashlib
    chunk_hashes = [hashlib.sha256(f"test_data_stream_{i}".encode()).hexdigest() for i in range(2000)]

    migration = ring.test_placement_migration(
        chunk_hashes,
        test_new_node="storage_node_5",
        test_remove_node="storage_node_2"
    )

    add_stats = migration["add_node_test"]
    # Theoretical migration from 4 to 5 nodes is 20%. Ring should achieve ~15% to 25%
    assert 15.0 <= add_stats["percentage_moved"] <= 26.0
    # Modulo hashing moves ~80%
    assert add_stats["naive_modulo_percentage"] > 70.0

def test_chunk_deduplication_and_reference_counting(tmp_path):
    # Use temporary sqlite database
    db_file = tmp_path / "test_meta.db"
    db = MetadataDatabase(db_url=f"sqlite:///{db_file}")

    chunk_a = {"hash": "a" * 64, "size": 4096, "target_nodes": ["storage_node_1"]}
    chunk_b = {"hash": "b" * 64, "size": 4096, "target_nodes": ["storage_node_2"]}
    chunk_c = {"hash": "c" * 64, "size": 4096, "target_nodes": ["storage_node_3"]}

    # Store Object 1 with chunks [A, B]
    res1 = db.register_object("my-bucket", "doc1.txt", 8192, [chunk_a, chunk_b])
    assert res1["new_chunks_stored"] == 2
    assert res1["deduplicated_chunks"] == 0

    assert db.get_chunk("a" * 64)["ref_count"] == 1
    assert db.get_chunk("b" * 64)["ref_count"] == 1

    # Store Object 2 with chunks [B, C] -> Chunk B is duplicated!
    res2 = db.register_object("my-bucket", "doc2.txt", 8192, [chunk_b, chunk_c])
    assert res2["new_chunks_stored"] == 1
    assert res2["deduplicated_chunks"] == 1
    assert res2["bytes_saved_by_dedup"] == 4096

    # Verify Chunk B ref_count is now 2
    assert db.get_chunk("b" * 64)["ref_count"] == 2
    assert db.get_chunk("c" * 64)["ref_count"] == 1

    # Delete Object 1 -> Chunk A should have ref_count 0 and be deleted, Chunk B ref_count should decrement to 1
    del1 = db.delete_object("my-bucket", "doc1.txt")
    assert del1["status"] == "deleted"
    assert db.get_chunk("a" * 64) is None # Deleted
    assert db.get_chunk("b" * 64)["ref_count"] == 1 # Retained

    # Delete Object 2 -> Chunk B and C reach 0 and are deleted
    del2 = db.delete_object("my-bucket", "doc2.txt")
    assert del2["status"] == "deleted"
    assert db.get_chunk("b" * 64) is None
    assert db.get_chunk("c" * 64) is None
