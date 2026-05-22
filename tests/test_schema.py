from a5c_murmur.schema import Message, MessageKind, action_hash


def test_action_hash_stable_under_key_order():
    a = {"verdict": "merge", "after": "rebase"}
    b = {"after": "rebase", "verdict": "merge"}
    assert action_hash(a) == action_hash(b)


def test_action_hash_empty_or_none_is_none():
    assert action_hash(None) is None
    assert action_hash({}) is None


def test_action_hash_changes_with_content():
    assert action_hash({"v": "merge"}) != action_hash({"v": "hold"})


def test_message_redis_roundtrip():
    m = Message.new(
        task_id="t1",
        agent="a",
        kind="propose",
        text="merge it",
        action={"verdict": "merge"},
        round=1,
    )
    fields = m.to_redis_fields()
    back = Message.from_redis_fields(fields)
    assert back.task_id == "t1"
    assert back.agent == "a"
    assert back.kind == MessageKind.PROPOSE
    assert back.action == {"verdict": "merge"}
    assert back.round == 1


def test_message_action_hash_property():
    m = Message.new(task_id="t", agent="a", kind="agree", action={"x": 1})
    assert m.action_hash is not None
    assert m.action_hash == action_hash({"x": 1})
