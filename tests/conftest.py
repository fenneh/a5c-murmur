from __future__ import annotations

import pytest

from a5c_murmur.bus import InMemoryBus
from a5c_murmur.journal import Journal


@pytest.fixture(autouse=True)
def _bus_default(monkeypatch):
    """Force the in-memory adapter in every test so nothing tries to talk to a
    real Redis."""
    monkeypatch.setenv("A5C_MURMUR_BUS", "memory")


@pytest.fixture
def bus():
    return InMemoryBus()


@pytest.fixture
def journal(tmp_path):
    j = Journal(path=tmp_path / "j.db")
    j.init()
    return j
