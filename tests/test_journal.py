from a5c_murmur.schema import Decision, Message


def test_open_task_idempotent(journal):
    journal.open_task("t1", label="hello")
    journal.open_task("t1", label="hello")
    tasks = journal.list_tasks()
    assert sum(1 for t in tasks if t["task_id"] == "t1") == 1


def test_record_and_fetch_messages(journal):
    journal.open_task("t1")
    journal.record_message(Message.new(task_id="t1", agent="a", kind="propose", action={"x": 1}))
    journal.record_message(Message.new(task_id="t1", agent="b", kind="agree", action={"x": 1}))
    msgs = journal.messages_for("t1")
    assert len(msgs) == 2
    assert msgs[0]["action_hash"] == msgs[1]["action_hash"]


def test_record_decision_and_fetch(journal):
    journal.open_task("t1")
    journal.record_decision(
        Decision(
            task_id="t1",
            status="agreed",
            action={"x": 1},
            action_hash="abc",
            signers=["a", "b"],
            rationale="2/2 agreed",
        )
    )
    d = journal.get_decision("t1")
    assert d is not None
    assert d["status"] == "agreed"
    assert "abc" in d["action_hash"]


def test_tool_call_log(journal):
    journal.open_task("t1")
    journal.record_tool_call(
        task_id="t1",
        agent="a",
        tool="lookup",
        args={"q": "x"},
        result_preview="found",
        round=1,
    )
    calls = journal.tool_calls_for("t1")
    assert len(calls) == 1
    assert calls[0]["tool"] == "lookup"
