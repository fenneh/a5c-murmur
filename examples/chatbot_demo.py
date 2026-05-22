"""Smallest possible thing. Two agents on an in-memory bus, no Redis needed.

Run:
    uv run python examples/chatbot_demo.py
"""

from __future__ import annotations

import threading
import time

from a5c_murmur import Agent
from a5c_murmur.bus import InMemoryBus


def main() -> None:
    bus = InMemoryBus()

    class Reviewer(Agent):
        role = "reviewer"
        streams = ["bus:reviews"]

        def handle_message(self, stream, msg_id, fields):
            print(f"  [reviewer] saw {fields['pr']} from {fields['author']}")
            # Acknowledge.
            bus.publish(
                "bus:reviews-ack",
                {"pr": fields["pr"], "verdict": "approve"},
            )
            self.stop()

    class Author(Agent):
        role = "author"
        streams = ["bus:reviews-ack"]

        def handle_message(self, stream, msg_id, fields):
            print(f"  [author] reviewer said {fields['verdict']} on {fields['pr']}")
            self.stop()

    reviewer = Reviewer(bus=bus, block_ms=200)
    author = Author(bus=bus, block_ms=200)
    t1 = threading.Thread(target=reviewer.run)
    t2 = threading.Thread(target=author.run)
    t1.start()
    t2.start()

    time.sleep(0.1)
    bus.publish("bus:reviews", {"pr": "PR-42", "author": "alice"})

    t1.join(timeout=2)
    t2.join(timeout=2)


if __name__ == "__main__":
    main()
