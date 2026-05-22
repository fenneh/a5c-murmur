import threading
import time

from a5c_murmur.debate import Debate
from a5c_murmur.schema import MessageKind


def test_two_agree_on_same_action_decides(bus):
    d = Debate("t", bus, roster=["a", "b"])
    d.post(agent="a", kind="propose", action={"verdict": "merge"})
    d.post(agent="b", kind="agree", action={"verdict": "merge"})
    d.post(agent="a", kind="agree", action={"verdict": "merge"})
    out = d.wait_for_decision(quorum=2, timeout_s=2)
    assert out.status == "agreed"
    assert out.action == {"verdict": "merge"}
    assert sorted(out.signers) == ["a", "b"]


def test_one_agree_is_not_quorum(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="propose", action={"x": 1})
    d.post(agent="a", kind="agree", action={"x": 1})
    out = d.wait_for_decision(quorum=2, timeout_s=1)
    assert out.status == "timeout"
    assert out.action is None


def test_two_agree_on_different_actions_is_no_consensus(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="agree", action={"x": 1})
    d.post(agent="b", kind="agree", action={"x": 2})
    out = d.wait_for_decision(quorum=2, timeout_s=1)
    assert out.status == "timeout"


def test_abort_short_circuits(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="agree", action={"x": 1})
    d.post(agent="b", kind="abort", text="cancelled")
    out = d.wait_for_decision(quorum=2, timeout_s=2)
    assert out.status == "aborted"


def test_action_payload_equality_is_key_order_independent(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="agree", action={"verdict": "merge", "after": "rebase"})
    d.post(agent="b", kind="agree", action={"after": "rebase", "verdict": "merge"})
    out = d.wait_for_decision(quorum=2, timeout_s=1)
    assert out.status == "agreed"


def test_decision_arrives_while_waiting(bus):
    """Late-arriving messages still count."""
    d = Debate("t", bus)

    def slow_signers():
        time.sleep(0.1)
        d.post(agent="a", kind="agree", action={"x": 1})
        time.sleep(0.1)
        d.post(agent="b", kind="agree", action={"x": 1})

    t = threading.Thread(target=slow_signers)
    t.start()
    out = d.wait_for_decision(quorum=2, timeout_s=2)
    t.join(timeout=2)
    assert out.status == "agreed"


def test_tally_helper(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="agree", action={"x": 1})
    d.post(agent="b", kind="agree", action={"x": 1})
    d.post(agent="c", kind="agree", action={"x": 2})
    msgs = d.history()
    tally = Debate.tally_agreements(msgs)
    counts = {k: len(v) for k, v in tally.items()}
    assert sorted(counts.values()) == [1, 2]


def test_messages_are_collected_in_outcome(bus):
    d = Debate("t", bus)
    d.post(agent="a", kind="propose", text="propose this", action={"v": 1})
    d.post(agent="b", kind="agree", action={"v": 1})
    d.post(agent="a", kind="agree", action={"v": 1})
    out = d.wait_for_decision(quorum=2, timeout_s=2)
    kinds = [m.kind for m in out.messages]
    assert MessageKind.PROPOSE in kinds
    assert MessageKind.AGREE in kinds
