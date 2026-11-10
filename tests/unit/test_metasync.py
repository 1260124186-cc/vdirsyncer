from __future__ import annotations

import asyncio

import hypothesis.strategies as st
import pytest
import pytest_asyncio
from hypothesis import example
from hypothesis import given

from tests import blow_up
from vdirsyncer.exceptions import ReadOnlyError
from vdirsyncer.exceptions import UserError
from vdirsyncer.metasync import MetaSyncConflict
from vdirsyncer.metasync import logger
from vdirsyncer.metasync import metasync
from vdirsyncer.storage.base import normalize_meta_value
from vdirsyncer.storage.memory import MemoryStorage


def entry_value(status, key):
    """Read the synced value from either a provenance or a legacy entry."""
    entry = status.get(key)
    if isinstance(entry, dict):
        return normalize_meta_value(entry.get("value"))
    return normalize_meta_value(entry)


async def assert_synced(a, b, status, key, value):
    assert await a.get_meta(key) == value
    assert await b.get_meta(key) == value
    assert entry_value(status, key) == value


def assert_provenance(status, key, value, source, generation):
    assert status[key] == {
        "value": value,
        "source": source,
        "generation": generation,
    }


@pytest.mark.asyncio
async def test_irrelevant_status():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {
        "foo": "bar",
        "baz": {"value": "qux", "source": "a", "generation": 1},
    }

    await metasync(a, b, status, keys=())
    assert not status


@pytest.mark.asyncio
async def test_basic(monkeypatch):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}

    await a.set_meta("foo", None)
    await metasync(a, b, status, keys=["foo"])
    assert await a.get_meta("foo") is None
    assert await b.get_meta("foo") is None
    assert not status

    await a.set_meta("foo", "bar")
    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "bar")
    assert_provenance(status, "foo", "bar", "a", 1)

    await a.set_meta("foo", "baz")
    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "baz")
    assert_provenance(status, "foo", "baz", "a", 2)

    # A rerun without any changes must not write anything and must not bump
    # the generation.
    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)
    await metasync(a, b, status, keys=["foo"])
    assert_provenance(status, "foo", "baz", "a", 2)
    monkeypatch.undo()
    monkeypatch.undo()

    await b.set_meta("foo", None)
    await metasync(a, b, status, keys=["foo"])
    assert not await a.get_meta("foo")
    assert not await b.get_meta("foo")
    assert not status


@pytest_asyncio.fixture
async def conflict_state(request):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "baz")

    async def do_cleanup():
        assert await a.get_meta("foo") == "bar"
        assert await b.get_meta("foo") == "baz"
        assert not status

    request.addfinalizer(lambda: asyncio.run(do_cleanup()))

    return a, b, status


@pytest_asyncio.fixture
async def test_conflict(conflict_state):
    a, b, status = conflict_state

    with pytest.raises(MetaSyncConflict):
        await metasync(a, b, status, keys=["foo"])


@pytest.mark.asyncio
async def test_conflict_does_not_write_or_record(conflict_state, monkeypatch):
    a, b, status = conflict_state
    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)

    with pytest.raises(MetaSyncConflict):
        await metasync(a, b, status, keys=["foo"])

    assert await a.get_meta("foo") == "bar"
    assert await b.get_meta("foo") == "baz"
    assert not status


@pytest.mark.asyncio
async def test_conflict_is_stable_across_reruns(conflict_state):
    a, b, status = conflict_state

    for _ in range(3):
        with pytest.raises(MetaSyncConflict):
            await metasync(a, b, status, keys=["foo"])
        assert await a.get_meta("foo") == "bar"
        assert await b.get_meta("foo") == "baz"
        assert not status


@pytest.mark.asyncio
async def test_invalid_conflict_resolution(conflict_state):
    a, b, status = conflict_state

    with pytest.raises(UserError) as excinfo:
        await metasync(a, b, status, keys=["foo"], conflict_resolution="foo")

    assert "Invalid conflict resolution setting" in str(excinfo.value)


@pytest.mark.asyncio
async def test_warning_on_custom_conflict_commands(conflict_state, monkeypatch):
    a, b, status = conflict_state
    warnings = []
    monkeypatch.setattr(logger, "warning", warnings.append)

    with pytest.raises(MetaSyncConflict):
        await metasync(
            a,
            b,
            status,
            keys=["foo"],
            conflict_resolution=lambda *a, **kw: None,
        )

    assert warnings == ["Custom commands don't work on metasync."]


@pytest.mark.asyncio
async def test_conflict_same_content():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "bar")

    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "bar")
    # The value was adopted without a single clear source and nothing was
    # written.
    assert_provenance(status, "foo", "bar", None, 1)


@pytest.mark.parametrize("wins", "ab")
@pytest.mark.asyncio
async def test_conflict_x_wins(wins):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "baz")

    await metasync(
        a,
        b,
        status,
        keys=["foo"],
        conflict_resolution="a wins" if wins == "a" else "b wins",
    )

    expected = "bar" if wins == "a" else "baz"
    await assert_synced(a, b, status, "foo", expected)
    assert_provenance(status, "foo", expected, wins, 1)


@pytest.mark.asyncio
async def test_source_and_generation_follow_single_edits():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}

    await a.set_meta("foo", "one")
    await metasync(a, b, status, keys=["foo"])
    assert_provenance(status, "foo", "one", "a", 1)

    await b.set_meta("foo", "two")
    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "two")
    assert_provenance(status, "foo", "two", "b", 2)

    await b.set_meta("foo", "three")
    await metasync(a, b, status, keys=["foo"])
    assert_provenance(status, "foo", "three", "b", 3)


@pytest.mark.asyncio
async def test_external_edit_becomes_new_source():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}

    await a.set_meta("foo", "bar")
    await metasync(a, b, status, keys=["foo"])
    assert_provenance(status, "foo", "bar", "a", 1)

    # Something outside of vdirsyncer changes the value on B. The edit is a
    # new source and must be propagated instead of being reverted.
    await b.set_meta("foo", "external")
    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "external")
    assert_provenance(status, "foo", "external", "b", 2)


@pytest.mark.asyncio
async def test_concurrent_sources_keep_conflict_strategy(monkeypatch):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}

    await a.set_meta("foo", "bar")
    await metasync(a, b, status, keys=["foo"])

    # Both sides are edited independently before the next metasync.
    await a.set_meta("foo", "from-a")
    await b.set_meta("foo", "from-b")
    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)

    with pytest.raises(MetaSyncConflict):
        await metasync(a, b, status, keys=["foo"])

    monkeypatch.undo()
    monkeypatch.undo()

    # Explicitly choosing a winner resolves the conflict exactly once.
    await metasync(a, b, status, keys=["foo"], conflict_resolution="a wins")
    await assert_synced(a, b, status, "foo", "from-a")
    assert_provenance(status, "foo", "from-a", "a", 2)

    # The next run has nothing to do and must not overwrite anything.
    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)
    await metasync(a, b, status, keys=["foo"])
    await assert_synced(a, b, status, "foo", "from-a")
    assert_provenance(status, "foo", "from-a", "a", 2)


@pytest.mark.asyncio
async def test_legacy_status_without_changes_is_left_alone():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {"foo": "bar"}
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "bar")

    await metasync(a, b, status, keys=["foo"])

    # No transition happened, the legacy entry keeps its plain-value shape.
    assert status == {"foo": "bar"}


@pytest.mark.asyncio
async def test_legacy_status_single_change_is_upgraded():
    a = MemoryStorage()
    b = MemoryStorage()
    status = {"foo": "bar"}
    await a.set_meta("foo", "baz")
    await b.set_meta("foo", "bar")

    await metasync(a, b, status, keys=["foo"])

    await assert_synced(a, b, status, "foo", "baz")
    assert_provenance(status, "foo", "baz", "a", 1)


@pytest.mark.asyncio
async def test_legacy_status_conflict_uses_old_strategy(monkeypatch):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {"foo": "bar"}
    await a.set_meta("foo", "one")
    await b.set_meta("foo", "two")
    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)

    with pytest.raises(MetaSyncConflict):
        await metasync(a, b, status, keys=["foo"])

    # Missing provenance must not cause any writes and keeps the legacy entry.
    assert status == {"foo": "bar"}


@pytest.mark.asyncio
async def test_removed_key_is_not_restored(monkeypatch):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {
        "foo": {"value": "bar", "source": "a", "generation": 1},
        "other": {"value": "baz", "source": "b", "generation": 4},
    }
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "bar")

    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)

    await metasync(a, b, status, keys=["foo"])

    assert "other" not in status
    assert await a.get_meta("other") is None
    assert await b.get_meta("other") is None


@pytest.mark.asyncio
async def test_interrupted_write_is_completed_without_rewriting():
    a = MemoryStorage()
    b = MemoryStorage()
    # The storage write already happened, but the status was never saved.
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "bar")
    status = {}

    async def fail(*args, **kwargs):
        raise AssertionError("No write expected while catching up.")

    a.set_meta = b.set_meta = fail

    await metasync(a, b, status, keys=["foo"])

    await assert_synced(a, b, status, "foo", "bar")
    assert_provenance(status, "foo", "bar", None, 1)


@pytest.mark.asyncio
async def test_interrupted_provenance_write_keeps_generation_monotonic():
    a = MemoryStorage()
    b = MemoryStorage()
    await a.set_meta("foo", "new")
    await b.set_meta("foo", "new")
    status = {"foo": {"value": "old", "source": "a", "generation": 3}}

    a.set_meta = b.set_meta = blow_up

    await metasync(a, b, status, keys=["foo"])

    assert_provenance(status, "foo", "new", None, 4)


@pytest.mark.asyncio
async def test_repeated_runs_keep_final_value(monkeypatch):
    a = MemoryStorage()
    b = MemoryStorage()
    status = {}
    await a.set_meta("foo", "bar")
    await b.set_meta("foo", "baz")

    await metasync(
        a, b, status, keys=["foo"], conflict_resolution="b wins"
    )
    snapshot = (
        await a.get_meta("foo"),
        await b.get_meta("foo"),
        dict(status["foo"]),
    )

    monkeypatch.setattr(a, "set_meta", blow_up)
    monkeypatch.setattr(b, "set_meta", blow_up)
    for _ in range(3):
        await metasync(a, b, status, keys=["foo"])
        assert (
            await a.get_meta("foo"),
            await b.get_meta("foo"),
            dict(status["foo"]),
        ) == snapshot


@pytest.mark.asyncio
async def test_read_only_storage_uses_conflict_strategy():
    a = MemoryStorage(read_only=True)
    b = MemoryStorage()
    status = {}
    a.metadata["foo"] = "bar"
    await b.set_meta("foo", "baz")

    with pytest.raises(MetaSyncConflict):
        await metasync(a, b, status, keys=["foo"])

    with pytest.raises(ReadOnlyError):
        await metasync(
            a, b, status, keys=["foo"], conflict_resolution="b wins"
        )


@pytest.mark.asyncio
async def test_shared_storage_in_two_pairs_does_not_flap():
    # B participates in two pairs, like a calendar synced against two
    # accounts. Each pair has its own status, exactly like two .metadata
    # status files on disk.
    a = MemoryStorage(instance_name="A")
    b = MemoryStorage(instance_name="B")
    c = MemoryStorage(instance_name="C")
    status_ab = {}
    status_bc = {}
    keys = ["displayname", "color"]

    await a.set_meta("displayname", "X")
    await a.set_meta("color", "red")

    await metasync(a, b, status_ab, keys=keys)
    await metasync(b, c, status_bc, keys=keys)
    for key in keys:
        await assert_synced(a, b, status_ab, key, await a.get_meta(key))
        await assert_synced(b, c, status_bc, key, await b.get_meta(key))

    # An edit on A travels through B to C in the next round.
    await a.set_meta("displayname", "Y")
    await metasync(a, b, status_ab, keys=keys)
    await metasync(b, c, status_bc, keys=keys)
    assert await c.get_meta("displayname") == "Y"

    # An edit on C travels back the other way.
    await c.set_meta("color", "blue")
    await metasync(b, c, status_bc, keys=keys)
    await metasync(a, b, status_ab, keys=keys)
    assert await a.get_meta("color") == "blue"

    # Concurrent edits on the two outer storages are a real conflict:
    # nothing is overwritten until a conflict strategy is chosen.
    await a.set_meta("displayname", "from-a")
    await c.set_meta("displayname", "from-c")
    await metasync(a, b, status_ab, keys=keys)
    with pytest.raises(MetaSyncConflict):
        await metasync(b, c, status_bc, keys=keys)
    assert await b.get_meta("displayname") == "from-a"
    assert await c.get_meta("displayname") == "from-c"

    for _ in range(2):
        with pytest.raises(MetaSyncConflict):
            await metasync(b, c, status_bc, keys=keys)
    assert await b.get_meta("displayname") == "from-a"
    assert await c.get_meta("displayname") == "from-c"

    await metasync(
        b, c, status_bc, keys=keys, conflict_resolution="b wins"
    )
    await metasync(a, b, status_ab, keys=keys)

    # Once converged, arbitrarily many reruns must perform no writes and
    # leave every value and generation untouched.
    a.set_meta = b.set_meta = c.set_meta = blow_up
    generations = {
        "ab": {k: status_ab[k]["generation"] for k in keys},
        "bc": {k: status_bc[k]["generation"] for k in keys},
    }
    for _ in range(3):
        await metasync(a, b, status_ab, keys=keys)
        await metasync(b, c, status_bc, keys=keys)
        for key in keys:
            assert await a.get_meta(key) == await c.get_meta(key)
        assert {k: status_ab[k]["generation"] for k in keys} == generations["ab"]
        assert {k: status_bc[k]["generation"] for k in keys} == generations["bc"]


keys = st.text(min_size=1).filter(lambda x: x.strip() == x)
values = st.text().filter(lambda x: normalize_meta_value(x) == x)
metadata = st.dictionaries(keys, values)


@given(
    a=metadata,
    b=metadata,
    status=metadata,
    keys=st.sets(keys),
    conflict_resolution=st.just("a wins") | st.just("b wins"),
)
@example(
    a={"0": "0"}, b={}, status={"0": "0"}, keys={"0"}, conflict_resolution="a wins"
)
@example(
    a={"0": "0"},
    b={"0": "1"},
    status={"0": "0"},
    keys={"0"},
    conflict_resolution="a wins",
)
@pytest.mark.asyncio
async def test_fuzzing(a, b, status, keys, conflict_resolution):
    def _get_storage(m, instance_name):
        s = MemoryStorage(instance_name=instance_name)
        s.metadata = m
        return s

    a = _get_storage(a, "A")
    b = _get_storage(b, "B")

    winning_storage = a if conflict_resolution == "a wins" else b
    expected_values = {
        key: await winning_storage.get_meta(key) for key in keys if key not in status
    }

    await metasync(a, b, status, keys=keys, conflict_resolution=conflict_resolution)

    for key in keys:
        s = entry_value(status, key)
        assert await a.get_meta(key) == await b.get_meta(key) == s
        if expected_values.get(key) and s:
            assert s == expected_values[key]

    # Rerunning the same metasync must converge to the identical state and
    # perform no writes at all, regardless of whether the input status used
    # the legacy or provenance format.
    rerun_status = dict(status)
    a.set_meta = b.set_meta = blow_up
    await metasync(a, b, status, keys=keys, conflict_resolution=conflict_resolution)
    assert status == rerun_status
