"""Full debate cycle: propose, challenge, revise, agree, decide.

Three agents review a pull request and have to land on a single verdict.
No Redis needed, runs against the in-memory bus.

Run:
    uv run python examples/debate_demo.py
"""

from __future__ import annotations

from a5c_murmur import Debate
from a5c_murmur.bus import InMemoryBus


def main() -> None:
    bus = InMemoryBus()
    debate = Debate.open(
        task_id="pr-42",
        channel=bus,
        roster=["reviewer", "tester", "lead"],
    )

    # Round 1: an initial proposal.
    debate.post(
        agent="reviewer",
        kind="propose",
        text="LGTM, merge after a rebase.",
        action={"verdict": "merge", "after": "rebase"},
        round=1,
    )

    # Round 2: someone pushes back.
    debate.post(
        agent="tester",
        kind="challenge",
        text="The new test is flaky on macOS, I've seen it fail 1 in 5 runs.",
        round=2,
    )

    # Round 3: the proposer revises.
    debate.post(
        agent="reviewer",
        kind="revise",
        text="Fair, let's fix the flake before merging.",
        action={"verdict": "merge", "after": "rebase + fix flaky test"},
        round=3,
    )

    # Round 4: agreements come in.
    debate.post(
        agent="tester",
        kind="agree",
        action={"verdict": "merge", "after": "rebase + fix flaky test"},
        round=4,
    )
    debate.post(
        agent="lead",
        kind="agree",
        action={"verdict": "merge", "after": "rebase + fix flaky test"},
        round=4,
    )

    # Orchestrator side: wait for quorum.
    outcome = debate.wait_for_decision(quorum=2, timeout_s=5)

    print(f"status:  {outcome.status}")
    print(f"signers: {outcome.signers}")
    print(f"action:  {outcome.action}")
    print()
    print("transcript:")
    for m in outcome.messages:
        action = f" action={m.action}" if m.action else ""
        print(f"  r{m.round} [{m.kind.value:10}] {m.agent:9} {m.text}{action}")


if __name__ == "__main__":
    main()
