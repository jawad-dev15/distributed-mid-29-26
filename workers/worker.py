import json
import time
import sys
from typing import Optional
import pika
import requests

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

QUEUE_REPAIR = "storage_repair_queue"
QUEUE_REBALANCE = "storage_rebalance_queue"
QUEUE_GC = "storage_gc_queue"

def log_worker(job_type: str, details: str):
    print(f"[{time.strftime('%X')}] [WORKER] [{job_type}] {details}", flush=True)

class DistributedStorageWorker:
    """Processes asynchronous storage maintenance tasks dispatched via RabbitMQ."""
    def __init__(self, host: str = RABBITMQ_HOST, port: int = RABBITMQ_PORT):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    def handle_repair(self, ch, method, properties, body):
        data = json.loads(body.decode("utf-8"))
        c_hash = data.get("chunk_hash")
        failed = data.get("failed_node")
        target = data.get("target_node")
        log_worker("REPAIR", f"Rebuilding chunk {c_hash[:12]}... (Failed: {failed} -> Restored on: {target})")
        # Simulate rebuild time
        time.sleep(0.05)
        log_worker("REPAIR", f"Successfully reconstructed chunk {c_hash[:12]} on {target}!")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_rebalance(self, ch, method, properties, body):
        data = json.loads(body.decode("utf-8"))
        c_hash = data.get("chunk_hash")
        src = data.get("source_node")
        dst = data.get("dest_node")
        log_worker("REBALANCE", f"Migrating chunk {c_hash[:12]}... ({src} -> {dst})")
        time.sleep(0.05)
        log_worker("REBALANCE", f"Successfully migrated chunk {c_hash[:12]} to new token boundary on {dst}!")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_gc(self, ch, method, properties, body):
        data = json.loads(body.decode("utf-8"))
        c_hash = data.get("chunk_hash")
        nodes = data.get("target_nodes", [])
        log_worker("GC", f"Purging orphaned chunk {c_hash[:12]} from nodes: {nodes}")
        time.sleep(0.05)
        log_worker("GC", f"Reclaimed physical disk blocks for chunk {c_hash[:12]}!")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self, max_seconds: Optional[int] = None):
        """Starts worker loop listening on all three queues."""
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=self.credentials)
        )
        channel = conn.channel()
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(queue=QUEUE_REPAIR, on_message_callback=self.handle_repair)
        channel.basic_consume(queue=QUEUE_REBALANCE, on_message_callback=self.handle_rebalance)
        channel.basic_consume(queue=QUEUE_GC, on_message_callback=self.handle_gc)

        print("[*] Distributed Storage Worker listening for RabbitMQ jobs... (CTRL+C to stop)", flush=True)
        start_t = time.time()
        try:
            while True:
                conn.process_data_events(time_limit=1)
                if max_seconds and (time.time() - start_t) > max_seconds:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            conn.close()

if __name__ == "__main__":
    max_sec = int(sys.argv[1]) if len(sys.argv) > 1 else None
    worker = DistributedStorageWorker()
    worker.start_consuming(max_seconds=max_sec)
