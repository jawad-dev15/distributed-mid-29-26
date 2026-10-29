import pytest
from fastapi.testclient import TestClient
from gateway.app import app, client_requests, es_index
from gateway.search_engine import ElasticSearchIndex

@pytest.fixture
def client():
    client_requests.clear()
    return TestClient(app)

@pytest.fixture
def auth_headers(client):
    res = client.post("/api/auth/token", json={"username": "admin", "password": "password123"})
    assert res.status_code == 200
    token = res.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

def test_standalone_elasticsearch_index():
    engine = ElasticSearchIndex(index_name="test_objects")
    
    doc1 = {
        "bucket": "data-warehouse",
        "key": "finance/2026/q1_earnings.csv",
        "size": 15000,
        "tags": {"confidential": "true", "dept": "finance"}
    }
    doc2 = {
        "bucket": "data-warehouse",
        "key": "engineering/telemetry/nodes.log",
        "size": 4200,
        "tags": {"confidential": "false", "dept": "devops"}
    }
    
    engine.index_document(doc1)
    engine.index_document(doc2)

    # 1. Match query
    match_res = engine.query_dsl({
        "query": {
            "match": {"key": "earnings"}
        }
    })
    assert match_res["hits"]["total"]["value"] == 1
    assert match_res["hits"]["hits"][0]["_id"] == "data-warehouse/finance/2026/q1_earnings.csv"

    # 2. Prefix query
    prefix_res = engine.query_dsl({
        "query": {
            "prefix": {"key": "engineering/"}
        }
    })
    assert prefix_res["hits"]["total"]["value"] == 1
    assert prefix_res["hits"]["hits"][0]["_id"] == "data-warehouse/engineering/telemetry/nodes.log"

    # 3. Term query on tags
    term_res = engine.query_dsl({
        "query": {
            "term": {"tags.dept": "finance"}
        }
    })
    assert term_res["hits"]["total"]["value"] == 1

    # 4. Range query on size
    range_res = engine.query_dsl({
        "query": {
            "range": {"size": {"gte": 5000, "lte": 20000}}
        }
    })
    assert range_res["hits"]["total"]["value"] == 1
    assert range_res["hits"]["hits"][0]["_source"]["size"] == 15000

    # 5. Delete document
    deleted = engine.delete_document("data-warehouse", "finance/2026/q1_earnings.csv")
    assert deleted is True
    post_delete_res = engine.query_dsl({
        "query": {
            "match": {"key": "earnings"}
        }
    })
    assert post_delete_res["hits"]["total"]["value"] == 0

def test_gateway_elasticsearch_api(client, auth_headers):
    # Upload test object with unique key and tag
    unique_key = "analytics/es_test_report.csv"
    payload = b"col1,col2,col3\n100,200,300\n"
    
    up_res = client.post(
        f"/api/objects/es-bucket/{unique_key}",
        content=payload,
        headers={**auth_headers, "X-Tags": "tier=gold,system=elasticsearch"}
    )
    assert up_res.status_code == 201

    # Test POST /api/search/es
    dsl_query = {
        "query": {
            "match": {"key": "analytics"}
        }
    }
    es_post_res = client.post("/api/search/es", json=dsl_query, headers=auth_headers)
    assert es_post_res.status_code == 200
    res_data = es_post_res.json()
    assert res_data["hits"]["total"]["value"] >= 1
    matched_ids = [h["_id"] for h in res_data["hits"]["hits"]]
    assert f"es-bucket/{unique_key}" in matched_ids

    # Test GET /api/search/es
    es_get_res = client.get("/api/search/es?q=report&bucket=es-bucket", headers=auth_headers)
    assert es_get_res.status_code == 200
    assert es_get_res.json()["hits"]["total"]["value"] >= 1
