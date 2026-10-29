import json
import time
import pika
from typing import Dict, Any, List, Optional

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

QUEUE_REPAIR = "storage_repair_queue"
QUEUE_REBALANCE = "storage_rebalance_queue"
QUEUE_GC = "storage_gc_queue"

class RabbitMQJobManager:
    """Publishes asynchronous storage maintenance jobs to RabbitMQ queues."""
    def __init__(self, host: str = RABBITMQ_HOST, port: int = RABBITMQ_PORT):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    def _get_channel(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=self.credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_REPAIR, durable=True)
        channel.queue_declare(queue=QUEUE_REBALANCE, durable=True)
        channel.queue_declare(queue=QUEUE_GC, durable=True)
        return connection, channel

    def publish_repair_job(self, chunk_hash: str, failed_node: str, target_node: str) -> Dict[str, Any]:
        """Queues a chunk rebuild job when a node is missing heartbeats."""
        payload = {
            "job_type": "REPAIR",
            "chunk_hash": chunk_hash,
            "failed_node": failed_node,
            "target_node": target_node,
            "timestamp": time.time()
        }
        conn, ch = self._get_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_REPAIR,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2) # Persistent message
        )
        conn.close()
        return {"status": "queued", "queue": QUEUE_REPAIR, "payload": payload}

    def publish_rebalance_job(self, chunk_hash: str, source_node: str, dest_node: str) -> Dict[str, Any]:
        """Queues a rebalance migration job when scaling the hash ring."""
        payload = {
            "job_type": "REBALANCE",
            "chunk_hash": chunk_hash,
            "source_node": source_node,
            "dest_node": dest_node,
            "timestamp": time.time()
        }
        conn, ch = self._get_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_REBALANCE,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()
        return {"status": "queued", "queue": QUEUE_REBALANCE, "payload": payload}

    def publish_gc_job(self, chunk_hash: str, target_nodes: List[str]) -> Dict[str, Any]:
        """Queues an orphaned chunk deletion job when ref_count reaches 0."""
        payload = {
            "job_type": "GARBAGE_COLLECTION",
            "chunk_hash": chunk_hash,
            "target_nodes": target_nodes,
            "timestamp": time.time()
        }
        conn, ch = self._get_channel()
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_GC,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()
        return {"status": "queued", "queue": QUEUE_GC, "payload": payload}
