"""Two daemons talking through a real Redis.

Run a Redis on localhost:6379, then:
    REDIS_URL=redis://localhost:6379 uv run python examples/redis_demo.py
"""

from __future__ import annotations

import threading
import time

from a5c_murmur import Agent, Bus


def main() -> None:
    bus = Bus.open(kind="redis")  # uses REDIS_URL

    class Worker(Agent):
        role = "worker"
        streams = ["bus:jobs"]

        def handle_message(self, stream, msg_id, fields):
            print(f"  [worker] processing {fields}")
            self.bus.publish("bus:results", {"job_id": fields["job_id"], "ok": "true"})
            self.stop()

    class Collector(Agent):
        role = "collector"
        streams = ["bus:results"]

        def handle_message(self, stream, msg_id, fields):
            print(f"  [collector] result {fields}")
            self.stop()

    worker = Worker(bus=bus, block_ms=500)
    collector = Collector(bus=bus, block_ms=500)
    t1 = threading.Thread(target=worker.run)
    t2 = threading.Thread(target=collector.run)
    t1.start()
    t2.start()

    time.sleep(0.3)
    bus.publish("bus:jobs", {"job_id": "j-1", "payload": "do the thing"})

    t1.join(timeout=5)
    t2.join(timeout=5)
    print("done.")


if __name__ == "__main__":
    main()
