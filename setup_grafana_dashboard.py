import json
import requests

GRAFANA_URL = "http://localhost:3000"
AUTH = ("admin", "admin")
PROMETHEUS_DS = {"type": "prometheus", "uid": "fg2op6h4d9hxce"}

def update_dashboard():
    dashboard_payload = {
        "dashboard": {
            "id": None,
            "uid": "distributed-storage-mid29",
            "title": "Distributed Storage Cluster Telemetry - Mid-29",
            "tags": ["distributed-systems", "midterm", "storage"],
            "timezone": "browser",
            "schemaVersion": 38,
            "version": 2,
            "refresh": "5s",
            "panels": [
                {
                    "id": 1,
                    "title": "System Throughput & Bytes Transferred",
                    "type": "stat",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 0, "y": 0, "w": 6, "h": 5},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "sum(storage_gateway_throughput_bytes_total)",
                            "legendFormat": "Throughput",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "decbytes",
                            "color": {"mode": "palette-classic"}
                        }
                    }
                },
                {
                    "id": 2,
                    "title": "HTTP Gateway Requests (by Status Code)",
                    "type": "stat",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 6, "y": 0, "w": 6, "h": 5},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_gateway_requests_total",
                            "legendFormat": "HTTP {{status}}",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "short",
                            "color": {"mode": "palette-classic"}
                        }
                    }
                },
                {
                    "id": 3,
                    "title": "Active Storage Nodes",
                    "type": "stat",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 12, "y": 0, "w": 6, "h": 5},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "max(storage_gateway_active_nodes)",
                            "legendFormat": "Online Nodes",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "color": {"fixedColor": "green", "mode": "fixed"}
                        }
                    }
                },
                {
                    "id": 4,
                    "title": "Degraded Objects Count",
                    "type": "stat",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 18, "y": 0, "w": 6, "h": 5},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "max(storage_gateway_degraded_objects)",
                            "legendFormat": "Degraded",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "color": {"fixedColor": "blue", "mode": "fixed"}
                        }
                    }
                },
                {
                    "id": 5,
                    "title": "Chunks Stored Distribution Across Storage Nodes",
                    "type": "bargauge",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 0, "y": 5, "w": 12, "h": 7},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_cluster_chunks_stored_total",
                            "legendFormat": "{{node}}",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "short",
                            "min": 0,
                            "max": 30,
                            "color": {"mode": "continuous-GrYlRd"}
                        }
                    }
                },
                {
                    "id": 6,
                    "title": "Available Free Disk Capacity per Node (GB)",
                    "type": "bargauge",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 12, "y": 5, "w": 12, "h": 7},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_cluster_disk_free_gb",
                            "legendFormat": "{{node}}",
                            "refId": "A"
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "decgbytes",
                            "min": 0,
                            "max": 50,
                            "color": {"fixedColor": "green", "mode": "fixed"}
                        }
                    }
                },
                {
                    "id": 7,
                    "title": "Background Self-Healing & Asynchronous Task Rates",
                    "type": "timeseries",
                    "datasource": PROMETHEUS_DS,
                    "gridPos": {"x": 0, "y": 12, "w": 24, "h": 8},
                    "targets": [
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_repair_jobs_completed_total",
                            "legendFormat": "Repair Jobs Completed",
                            "refId": "A"
                        },
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_rebalance_chunks_migrated_total",
                            "legendFormat": "Rebalance Chunks Migrated",
                            "refId": "B"
                        },
                        {
                            "datasource": PROMETHEUS_DS,
                            "expr": "storage_gc_chunks_purged_total",
                            "legendFormat": "GC Chunks Purged",
                            "refId": "C"
                        }
                    ]
                }
            ]
        },
        "overwrite": True
    }

    resp = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        json=dashboard_payload,
        auth=AUTH
    )
    print("Updated Grafana Dashboard Status:", resp.status_code, resp.text)

if __name__ == "__main__":
    update_dashboard()
