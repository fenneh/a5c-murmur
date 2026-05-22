import pytest
from fastapi.testclient import TestClient

from a5c_murmur.bus import InMemoryBus
from a5c_murmur.journal import Journal
from a5c_murmur.schema import Decision, Message


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("A5C_MURMUR_BUS", "memory")
    monkeypatch.setenv("A5C_MURMUR_DB", str(tmp_path / "m.db"))
    import importlib

    from a5c_murmur.server import app as srv_module

    importlib.reload(srv_module)
    # Reset module-level singletons.
    srv_module._bus = None
    srv_module._journal = None
    return srv_module


@pytest.fixture
def client(app):
    return TestClient(app.app)


def _seed_journal(journal: Journal):
    journal.open_task("t-1", label="a debate")
    journal.record_message(Message.new(task_id="t-1", agent="a", kind="propose", action={"v": 1}))
    journal.record_message(Message.new(task_id="t-1", agent="b", kind="agree", action={"v": 1}))
    journal.record_decision(
        Decision(
            task_id="t-1",
            status="agreed",
            action={"v": 1},
            action_hash="x",
            signers=["a", "b"],
            rationale="2/2",
        )
    )


def _seed_bus(bus: InMemoryBus):
    bus.hset("agent:a:status", {"status": "running", "last_seen": "9999999999", "pid": "1"})
    bus.hset("agent:b:status", {"status": "running", "last_seen": "9999999999", "pid": "2"})


def test_index_renders_empty(client):
    r = client.get("/ui")
    assert r.status_code == 200
    assert "a5c-murmur" in r.text


def test_index_renders_with_data(app, client):
    _seed_bus(app.get_bus())
    _seed_journal(app.get_journal())
    r = client.get("/ui")
    assert r.status_code == 200
    assert "t-1" in r.text
    # Agents section appears.
    assert ">a<" in r.text or "a</strong>" in r.text


def test_api_agents(app, client):
    _seed_bus(app.get_bus())
    r = client.get("/api/agents")
    assert r.status_code == 200
    data = r.json()
    roles = {a["role"] for a in data}
    assert roles == {"a", "b"}


def test_api_debates_lists(app, client):
    _seed_journal(app.get_journal())
    r = client.get("/api/debates")
    assert r.status_code == 200
    data = r.json()
    assert any(d["task_id"] == "t-1" for d in data)
    target = next(d for d in data if d["task_id"] == "t-1")
    assert target["decision"]["status"] == "agreed"


def test_debate_detail_page(app, client):
    _seed_journal(app.get_journal())
    r = client.get("/ui/debates/t-1")
    assert r.status_code == 200
    assert "agreed" in r.text
    assert "propose" in r.text
    assert "agree" in r.text


def test_debate_404(client):
    r = client.get("/ui/debates/does-not-exist")
    assert r.status_code == 404


def test_api_debate_tools(app, client):
    j = app.get_journal()
    j.open_task("t-1")
    j.record_tool_call(task_id="t-1", agent="a", tool="lookup", args={"q": "x"}, round=1)
    j.record_tool_call(task_id="t-1", agent="b", tool="search", args={"q": "y"}, round=2)
    r = client.get("/api/debates/t-1/tools")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert {row["tool"] for row in data} == {"lookup", "search"}


def test_api_agent_single(app, client):
    _seed_bus(app.get_bus())
    r = client.get("/api/agents/a")
    assert r.status_code == 200
    data = r.json()
    assert data["role"] == "a"
    assert data["status"] == "running"
    assert data["pid"] == "1"


def test_api_agent_404(client):
    r = client.get("/api/agents/never-seen")
    assert r.status_code == 404


def test_api_agent_carries_extra_fields(app, client):
    # Adapters may write custom fields (tokens_today_gbp, task, etc).
    # They should land in the `extra` dict.
    app.get_bus().hset(
        "agent:custom:status",
        {
            "status": "alive",
            "last_seen": "9999999999",
            "pid": "42",
            "tokens_today_gbp": "1.23",
            "task": "epl:arsenal-v-chelsea",
        },
    )
    r = client.get("/api/agents/custom")
    assert r.status_code == 200
    data = r.json()
    assert data["extra"]["tokens_today_gbp"] == "1.23"
    assert data["extra"]["task"] == "epl:arsenal-v-chelsea"


def test_api_bus_recent(app, client):
    bus = app.get_bus()
    bus.publish("bus:fixtures", {"instrument": "epl:a-v-b", "task_id": "t-1"})
    bus.publish("bus:fixtures", {"instrument": "epl:c-v-d", "task_id": "t-2"})
    r = client.get("/api/bus/recent", params={"stream": "bus:fixtures", "limit": 5})
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    # Newest first.
    assert data[0]["fields"]["task_id"] == "t-2"
    assert data[1]["fields"]["task_id"] == "t-1"


def test_api_bus_recent_unknown_stream(client):
    r = client.get("/api/bus/recent", params={"stream": "nonexistent"})
    assert r.status_code == 200
    assert r.json() == []
