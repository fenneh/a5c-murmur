import pytest
from fastapi.testclient import TestClient

from a5c_murmur.bus import InMemoryBus
from a5c_murmur.journal import Journal
from a5c_murmur.schema import Decision, Message


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("A5C_MURMUR_BUS", "memory")
    monkeypatch.setenv("A5C_MURMUR_DB", str(tmp_path / "m.db"))
    from a5c_murmur.server import app as srv_module
    import importlib

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
