import pytest
import hashlib
from gateway.erasure_coding import ErasureCoder
from gateway.replication import ReplicationManager, QuorumWriteError, QuorumReadError
from gateway.consistent_hash import ConsistentHashRing

def test_erasure_coding_encode_and_full_decode():
    coder = ErasureCoder(k=4, m=2)
    payload = b"Distributed Object-Tolerant Storage Midterm Test Payload 2026! " * 50
    encoded = coder.encode(payload)

    assert encoded["k"] == 4
    assert encoded["m"] == 2
    assert encoded["total_fragments"] == 6
    assert encoded["storage_multiplier"] == 1.5
    assert encoded["storage_overhead_pct"] == 50.0
    assert len(encoded["fragments"]) == 6

    # Decode with all 6 fragments
    decoded = coder.decode(encoded["fragments"], encoded["original_size"])
    assert decoded == payload

def test_erasure_coding_tolerates_loss_of_any_two_fragments():
    coder = ErasureCoder(k=4, m=2)
    payload = b"Critical Distributed Database Record Containing State Transactions" * 40
    encoded = coder.encode(payload)
    orig_size = encoded["original_size"]
    fragments = encoded["fragments"]

    # Scenario A: Lose 2 data fragments (0 and 1)
    surviving_a = {idx: fr for idx, fr in fragments.items() if idx in [2, 3, 4, 5]}
    assert len(surviving_a) == 4
    decoded_a = coder.decode(surviving_a, orig_size)
    assert decoded_a == payload

    # Scenario B: Lose 1 data fragment and 1 parity fragment (1 and 4)
    surviving_b = {idx: fr for idx, fr in fragments.items() if idx in [0, 2, 3, 5]}
    assert len(surviving_b) == 4
    decoded_b = coder.decode(surviving_b, orig_size)
    assert decoded_b == payload

    # Scenario C: Lose both parity fragments (4 and 5)
    surviving_c = {idx: fr for idx, fr in fragments.items() if idx in [0, 1, 2, 3]}
    assert len(surviving_c) == 4
    decoded_c = coder.decode(surviving_c, orig_size)
    assert decoded_c == payload

def test_erasure_coding_fails_if_less_than_k_fragments():
    coder = ErasureCoder(k=4, m=2)
    payload = b"Small test buffer"
    encoded = coder.encode(payload)

    # Only 3 surviving fragments (less than k=4)
    surviving = {0: encoded["fragments"][0], 1: encoded["fragments"][1], 2: encoded["fragments"][2]}
    with pytest.raises(ValueError, match="Cannot reconstruct: required at least k=4"):
        coder.decode(surviving, encoded["original_size"])

def test_quorum_parameters_validation():
    # Valid: W=2, R=2, N=3 -> W+R = 4 > 3
    mgr = ReplicationManager(n=3, w=2, r=2)
    assert mgr.w + mgr.r > mgr.n

    # Invalid: W=1, R=2, N=3 -> W+R = 3 (not > N)
    with pytest.raises(ValueError, match="Quorum condition violated"):
        ReplicationManager(n=3, w=1, r=2)

def test_quorum_write_and_read_against_live_cluster():
    ring = ConsistentHashRing(["storage_node_1", "storage_node_2", "storage_node_3", "storage_node_4"])
    mgr = ReplicationManager(n=3, w=2, r=2)

    payload = b"Live Cluster Quorum Replication Test Content Bytes!"
    c_hash = hashlib.sha256(payload).hexdigest()

    # 1. Quorum Write (N=3, W=2)
    write_res = mgr.write_chunk(c_hash, payload, ring)
    assert write_res["quorum_met"] is True
    assert len(write_res["successful_nodes"]) >= 2

    # 2. Quorum Read (N=3, R=2)
    read_bytes, healthy, degraded = mgr.read_chunk(c_hash, write_res["successful_nodes"])
    assert read_bytes == payload
    assert len(healthy) >= 2
    assert len(degraded) == 0
