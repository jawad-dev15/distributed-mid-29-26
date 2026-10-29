import pytest
import time
from fastapi.testclient import TestClient
from gateway.app import app, client_requests

@pytest.fixture
def client():
    # Clear rate limiter state between tests
    client_requests.clear()
    return TestClient(app)

@pytest.fixture
def auth_headers(client):
    res = client.post("/api/auth/token", json={"username": "admin", "password": "password123"})
    assert res.status_code == 200
    token = res.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

def test_auth_token_and_unauthorized_protection(client):
    # 1. Unauthenticated request to protected endpoint should return 401
    unauth_res = client.get("/api/buckets")
    assert unauth_res.status_code == 401

    # 2. Invalid Bearer token should return 401
    bad_token_res = client.get("/api/buckets", headers={"Authorization": "Bearer invalid.token.payload"})
    assert bad_token_res.status_code == 401

    # 3. Valid credentials generate JWT token
    auth_res = client.post("/api/auth/token", json={"username": "test_user", "password": "any"})
    assert auth_res.status_code == 200
    assert "access_token" in auth_res.json()

def test_bucket_lifecycle(client, auth_headers):
    # 1. Create Bucket
    create_res = client.post(
        "/api/buckets",
        json={"name": "test-vault-bucket", "policy": "replication"},
        headers=auth_headers
    )
    assert create_res.status_code == 200
    assert create_res.json()["name"] == "test-vault-bucket"

    # 2. List Buckets
    list_res = client.get("/api/buckets", headers=auth_headers)
    assert list_res.status_code == 200
    names = [b["name"] for b in list_res.json()["buckets"]]
    assert "test-vault-bucket" in names

def test_streaming_upload_and_download(client, auth_headers):
    payload = b"Streaming upload payload containing content-addressed storage bytes! " * 20
    
    # 1. Streaming Upload with User Tags
    upload_res = client.post(
        "/api/objects/test-vault-bucket/docs/spec.txt",
        content=payload,
        headers={**auth_headers, "X-Tags": "env=staging,author=jawad"},
        params={"chunk_type": "fastcdc"}
    )
    assert upload_res.status_code == 201
    data = upload_res.json()
    assert data["key"] == "docs/spec.txt"
    assert data["tags"] == {"env": "staging", "author": "jawad"}
    assert data["total_chunks"] >= 1

    # 2. Full Download
    download_res = client.get("/api/objects/test-vault-bucket/docs/spec.txt", headers=auth_headers)
    assert download_res.status_code == 200
    assert download_res.content == payload

def test_byte_range_request(client, auth_headers):
    full_payload = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" * 10
    client.post(
        "/api/objects/test-vault-bucket/media/video.bin",
        content=full_payload,
        headers=auth_headers
    )

    # Request bytes 10 to 35
    range_res = client.get(
        "/api/objects/test-vault-bucket/media/video.bin",
        headers={**auth_headers, "Range": "bytes=10-35"}
    )
    assert range_res.status_code == 206 # Partial Content
    assert range_res.headers["Content-Range"].startswith("bytes 10-35/")
    assert range_res.content == full_payload[10:36]

def test_multipart_upload_workflow(client, auth_headers):
    part1 = b"Part 1 Data Content Stream " * 30
    part2 = b"Part 2 Data Content Stream " * 30
    expected_full = part1 + part2

    # 1. Initiate Multipart
    init_res = client.post(
        "/api/objects/test-vault-bucket/archive/large.tar/multipart/initiate",
        headers=auth_headers
    )
    assert init_res.status_code == 200
    upload_id = init_res.json()["upload_id"]

    # 2. Upload Part 1
    p1_res = client.put(
        f"/api/objects/test-vault-bucket/archive/large.tar/multipart/{upload_id}/1",
        content=part1,
        headers=auth_headers
    )
    assert p1_res.status_code == 200

    # 3. Upload Part 2
    p2_res = client.put(
        f"/api/objects/test-vault-bucket/archive/large.tar/multipart/{upload_id}/2",
        content=part2,
        headers=auth_headers
    )
    assert p2_res.status_code == 200

    # 4. Complete Multipart
    complete_res = client.post(
        f"/api/objects/test-vault-bucket/archive/large.tar/multipart/{upload_id}/complete",
        headers={**auth_headers, "X-Tags": "type=archive"}
    )
    assert complete_res.status_code == 200
    assert complete_res.json()["key"] == "archive/large.tar"

    # 5. Download and verify concatenated content
    get_res = client.get("/api/objects/test-vault-bucket/archive/large.tar", headers=auth_headers)
    assert get_res.status_code == 200
    assert get_res.content == expected_full

def test_object_search_by_prefix_and_tags(client, auth_headers):
    # Upload searchable objects
    client.post(
        "/api/objects/test-vault-bucket/finance/invoice_2026_01.pdf",
        content=b"Invoice data January",
        headers={**auth_headers, "X-Tags": "department=finance,year=2026"}
    )
    client.post(
        "/api/objects/test-vault-bucket/finance/invoice_2026_02.pdf",
        content=b"Invoice data February",
        headers={**auth_headers, "X-Tags": "department=finance,year=2026"}
    )
    client.post(
        "/api/objects/test-vault-bucket/marketing/brochure.pdf",
        content=b"Brochure data",
        headers={**auth_headers, "X-Tags": "department=marketing"}
    )

    # 1. Search by prefix 'finance/'
    search_prefix = client.get("/api/search?prefix=finance/", headers=auth_headers)
    assert search_prefix.status_code == 200
    keys = [r["key"] for r in search_prefix.json()["results"]]
    assert "finance/invoice_2026_01.pdf" in keys
    assert "finance/invoice_2026_02.pdf" in keys
    assert "marketing/brochure.pdf" not in keys

    # 2. Search by tag department=marketing
    search_tag = client.get("/api/search?tag_key=department&tag_value=marketing", headers=auth_headers)
    assert search_tag.status_code == 200
    tag_keys = [r["key"] for r in search_tag.json()["results"]]
    assert "marketing/brochure.pdf" in tag_keys
    assert "finance/invoice_2026_01.pdf" not in tag_keys

def test_rate_limiting_enforcement(client, auth_headers):
    # Trigger requests up to limit
    from gateway.app import RATE_LIMIT_MAX_REQUESTS
    client_requests.clear()

    # Fire bursts until rate limited
    limit_hit = False
    for i in range(RATE_LIMIT_MAX_REQUESTS + 5):
        resp = client.get("/api/buckets", headers=auth_headers)
        if resp.status_code == 429:
            limit_hit = True
            assert "Retry-After" in resp.headers
            break
    assert limit_hit is True
