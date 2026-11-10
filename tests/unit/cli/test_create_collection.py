from __future__ import annotations

import os

import pytest

from vdirsyncer import exceptions
from vdirsyncer.cli import create_collection
from vdirsyncer.cli.create_collection import CREATE_PLAN_VERSION
from vdirsyncer.cli.create_collection import CollectionCreationCoordinator
from vdirsyncer.cli.create_collection import load_create_plan
from vdirsyncer.cli.create_collection import new_create_plan
from vdirsyncer.cli.create_collection import parse_create_plan
from vdirsyncer.cli.utils import get_status_path
from vdirsyncer.cli.utils import handle_collection_not_found
from vdirsyncer.cli.utils import save_status
from vdirsyncer.storage.filesystem import FilesystemStorage

PAIR = "foobar"
COLLECTION = "mycoll"


class _Pair:
    def __init__(self, config_a, config_b):
        self.name = PAIR
        self.config_a = config_a
        self.config_b = config_b


def _fs_config(tmp_path, name):
    path = tmp_path / name
    path.mkdir()
    return {
        "type": "filesystem",
        "path": str(path),
        "fileext": ".txt",
        "instance_name": name,
    }


def _get_discovered(config):
    async def get_discovered():
        rv = {}
        async for args in FilesystemStorage.discover(**config):
            rv[args["collection"]] = args
        return rv

    return get_discovered


async def _implicit_handler(config, collection, e=None, implicit_create=False):
    return await handle_collection_not_found(
        config, collection, implicit_create=True
    )


def _coordinator(tmp_path, config_a, config_b, handler=_implicit_handler):
    return CollectionCreationCoordinator(
        status_path=str(tmp_path / "status"),
        pair=_Pair(config_a, config_b),
        connector=None,
        handle_collection_not_found=handler,
    )


def _plan_file(tmp_path, collection=COLLECTION):
    return get_status_path(
        str(tmp_path / "status"), PAIR, collection, data_type="create_plan"
    )


def _write_plan(tmp_path, plan):
    save_status(
        base_path=str(tmp_path / "status"),
        pair=PAIR,
        data_type="create_plan",
        data=plan,
        collection=plan["collection"],
    )


# --------------------------------------------------------------------------
# Plan (de)serialization and validation
# --------------------------------------------------------------------------


def test_new_plan_marks_nothing_created():
    plan = new_create_plan(COLLECTION, "coll_a", "coll_b")
    assert plan == {
        "version": CREATE_PLAN_VERSION,
        "collection": COLLECTION,
        "a": {"collection": "coll_a", "created": False, "args": None},
        "b": {"collection": "coll_b", "created": False, "args": None},
    }


def test_plan_roundtrip(tmp_path):
    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    _write_plan(tmp_path, plan)

    raw = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    parsed = parse_create_plan(
        raw,
        status_path=str(tmp_path / "status"),
        pair_name=PAIR,
        collection=COLLECTION,
        collection_a=COLLECTION,
        collection_b=COLLECTION,
    )
    assert parsed == plan


def test_missing_plan_returns_none(tmp_path):
    assert load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION) is None


def test_corrupt_plan_json_fails(tmp_path):
    _write_plan(tmp_path, new_create_plan(COLLECTION, COLLECTION, COLLECTION))
    with open(_plan_file(tmp_path), "w") as f:
        f.write("{not valid json")

    with pytest.raises(exceptions.UserError, match="corrupt"):
        load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(version=999),
        lambda p: p.update(collection="other"),
        lambda p: p["a"].update(created="yes"),
        lambda p: p["a"].pop("collection"),
        lambda p: p["b"].update(args=[]),
    ],
)
def test_malformed_plan_fails(tmp_path, mutate):
    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    mutate(plan)
    _write_plan(tmp_path, plan)

    raw = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    with pytest.raises(exceptions.UserError, match="corrupt"):
        parse_create_plan(
            raw,
            status_path=str(tmp_path / "status"),
            pair_name=PAIR,
            collection=COLLECTION,
            collection_a=COLLECTION,
            collection_b=COLLECTION,
        )


def test_plan_drift_fails(tmp_path):
    # Plan records a different remote collection than the config now resolves.
    plan = new_create_plan(COLLECTION, "old_a", COLLECTION)
    _write_plan(tmp_path, plan)

    raw = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    with pytest.raises(exceptions.UserError, match="resolves it to"):
        parse_create_plan(
            raw,
            status_path=str(tmp_path / "status"),
            pair_name=PAIR,
            collection=COLLECTION,
            collection_a=COLLECTION,
            collection_b=COLLECTION,
        )


def test_persistable_args_strip_connector():
    args = {"type": "filesystem", "path": "/x", "connector": object()}
    safe = create_collection._persistable_args(args)
    assert "connector" not in safe
    assert safe["path"] == "/x"
    # The original dict must not be modified.
    assert "connector" in args


# --------------------------------------------------------------------------
# Coordinator: successful creation
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_creates_both_sides_and_removes_plan(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")
    coordinator = _coordinator(tmp_path, config_a, config_b)

    a_args, b_args = await coordinator.ensure_collection(
        COLLECTION,
        COLLECTION,
        COLLECTION,
        _get_discovered(config_a),
        _get_discovered(config_b),
    )

    assert (tmp_path / "foo" / COLLECTION).is_dir()
    assert (tmp_path / "bar" / COLLECTION).is_dir()
    assert a_args["collection"] == COLLECTION
    assert b_args["collection"] == COLLECTION
    assert not os.path.exists(_plan_file(tmp_path))


@pytest.mark.asyncio
async def test_plan_is_written_before_any_creation(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")

    async def failing_handler(config, collection):
        raise OSError("boom before any write")

    coordinator = _coordinator(tmp_path, config_a, config_b, failing_handler)

    with pytest.raises(OSError, match="boom"):
        await coordinator.ensure_collection(
            COLLECTION,
            COLLECTION,
            COLLECTION,
            _get_discovered(config_a),
            _get_discovered(config_b),
        )

    plan = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    assert plan is not None
    assert plan["a"]["created"] is False
    assert plan["b"]["created"] is False
    assert not (tmp_path / "foo" / COLLECTION).exists()


# --------------------------------------------------------------------------
# Coordinator: interrupted creation and resumption
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transient_failure_is_resumable_without_duplicates(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")

    async def flaky_handler(config, collection):
        if config["instance_name"] == "bar" and not getattr(
            flaky_handler, "recovered", False
        ):
            flaky_handler.recovered = True
            raise OSError("transient server error")
        return await handle_collection_not_found(
            config, collection, implicit_create=True
        )

    coordinator = _coordinator(tmp_path, config_a, config_b, flaky_handler)

    with pytest.raises(OSError, match="transient"):
        await coordinator.ensure_collection(
            COLLECTION,
            COLLECTION,
            COLLECTION,
            _get_discovered(config_a),
            _get_discovered(config_b),
        )

    # Side A was created and recorded, side B was not.
    assert (tmp_path / "foo" / COLLECTION).is_dir()
    assert not (tmp_path / "bar" / COLLECTION).exists()
    plan = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    assert plan["a"]["created"] is True
    assert plan["b"]["created"] is False

    # A fresh coordinator simulates the next `vdirsyncer discover` run.
    resume = _coordinator(tmp_path, config_a, config_b)
    a_args, b_args = await resume.ensure_collection(
        COLLECTION,
        COLLECTION,
        COLLECTION,
        _get_discovered(config_a),
        _get_discovered(config_b),
    )

    assert a_args["collection"] == COLLECTION
    assert b_args["collection"] == COLLECTION
    # Exactly one collection per side, no duplicates.
    assert list((tmp_path / "foo").iterdir()) == [tmp_path / "foo" / COLLECTION]
    assert list((tmp_path / "bar").iterdir()) == [tmp_path / "bar" / COLLECTION]
    assert not os.path.exists(_plan_file(tmp_path))


@pytest.mark.asyncio
async def test_resume_does_not_recreate_existing_side(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")

    await FilesystemStorage.create_collection(
        collection=COLLECTION, path=config_a["path"], fileext=".txt"
    )
    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    plan["a"]["created"] = True
    plan["a"]["args"] = {
        "type": "filesystem",
        "path": str(tmp_path / "foo" / COLLECTION),
        "fileext": ".txt",
        "collection": COLLECTION,
        "instance_name": "foo",
    }
    _write_plan(tmp_path, plan)

    created_sides = []

    async def recording_handler(config, collection):
        created_sides.append(config["instance_name"])
        return await handle_collection_not_found(
            config, collection, implicit_create=True
        )

    coordinator = _coordinator(tmp_path, config_a, config_b, recording_handler)
    await coordinator.ensure_collection(
        COLLECTION,
        COLLECTION,
        COLLECTION,
        _get_discovered(config_a),
        _get_discovered(config_b),
    )

    # A already existed (and was discovered); only B was created.
    assert created_sides == ["bar"]
    assert not os.path.exists(_plan_file(tmp_path))


# --------------------------------------------------------------------------
# Coordinator: rollback safety
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_terminal_failure_rolls_back_empty_created_side(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")

    async def unsupported_b(config, collection):
        if config["instance_name"] == "bar":
            raise exceptions.UserError("storage cannot create collections")
        return await handle_collection_not_found(
            config, collection, implicit_create=True
        )

    coordinator = _coordinator(tmp_path, config_a, config_b, unsupported_b)

    with pytest.raises(exceptions.UserError, match="cannot create"):
        await coordinator.ensure_collection(
            COLLECTION,
            COLLECTION,
            COLLECTION,
            _get_discovered(config_a),
            _get_discovered(config_b),
        )

    # The empty collection vdirsyncer created on A is rolled back.
    assert not (tmp_path / "foo" / COLLECTION).exists()
    assert not (tmp_path / "bar" / COLLECTION).exists()
    assert not os.path.exists(_plan_file(tmp_path))


@pytest.mark.asyncio
async def test_preexisting_collection_is_never_deleted(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")

    # Pre-existing collection on A, with an item in it.
    coll_a = tmp_path / "foo" / COLLECTION
    coll_a.mkdir()
    (coll_a / "item.txt").write_text("BEGIN:VCARD\nUID:item\nEND:VCARD")

    async def unsupported_b(config, collection):
        if config["instance_name"] == "bar":
            raise exceptions.UserError("storage cannot create collections")
        return await handle_collection_not_found(
            config, collection, implicit_create=True
        )

    coordinator = _coordinator(tmp_path, config_a, config_b, unsupported_b)

    with pytest.raises(exceptions.UserError, match="cannot create"):
        await coordinator.ensure_collection(
            COLLECTION,
            COLLECTION,
            COLLECTION,
            _get_discovered(config_a),
            _get_discovered(config_b),
        )

    assert coll_a.is_dir()
    assert (coll_a / "item.txt").is_file()
    assert not (tmp_path / "bar" / COLLECTION).exists()


@pytest.mark.asyncio
async def test_nonempty_created_collection_is_not_deleted(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    coll_a = tmp_path / "foo" / COLLECTION
    coll_a.mkdir()
    (coll_a / "item.txt").write_text("BEGIN:VCARD\nUID:item\nEND:VCARD")

    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    plan["a"]["created"] = True
    plan["a"]["args"] = {
        "type": "filesystem",
        "path": str(coll_a),
        "fileext": ".txt",
        "collection": COLLECTION,
        "instance_name": "foo",
    }
    _write_plan(tmp_path, plan)

    coordinator = _coordinator(tmp_path, config_a, config_a)
    with pytest.raises(exceptions.UserError, match="no longer empty"):
        await coordinator._rollback_created(plan, RuntimeError("original failure"))

    # Collection and plan are kept for a human to decide.
    assert coll_a.is_dir()
    assert (coll_a / "item.txt").is_file()
    raw = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    assert raw["a"]["created"] is True


@pytest.mark.asyncio
async def test_unsupported_delete_asks_for_manual_cleanup(tmp_path, monkeypatch):
    class _FakeStorage:
        async def list(self):
            if False:  # make this an async generator
                yield ()

        async def delete_collection(self):
            raise NotImplementedError

    async def fake_factory(config, create=True, *, connector):
        return _FakeStorage()

    monkeypatch.setattr(
        create_collection, "storage_instance_from_config", fake_factory
    )

    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    plan["a"]["created"] = True
    plan["a"]["args"] = {
        "type": "filesystem",
        "collection": COLLECTION,
        "instance_name": "foo",
    }
    _write_plan(tmp_path, plan)

    config = _fs_config(tmp_path, "foo")
    coordinator = _coordinator(tmp_path, config, config)
    with pytest.raises(exceptions.UserError, match="manually"):
        await coordinator._rollback_created(plan, RuntimeError("original failure"))

    raw = load_create_plan(str(tmp_path / "status"), PAIR, COLLECTION)
    assert raw["a"]["created"] is True


@pytest.mark.asyncio
async def test_rollback_tolerates_externally_removed_collection(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    plan = new_create_plan(COLLECTION, COLLECTION, COLLECTION)
    plan["a"]["created"] = True
    plan["a"]["args"] = {
        "type": "filesystem",
        "path": str(tmp_path / "foo" / COLLECTION),
        "fileext": ".txt",
        "collection": COLLECTION,
        "instance_name": "foo",
    }
    _write_plan(tmp_path, plan)

    coordinator = _coordinator(tmp_path, config_a, config_a)
    # Must not raise: the collection directory was deleted out of band.
    await coordinator._rollback_created(plan, RuntimeError("original failure"))

    # The stale marker is cleared and the plan removed.
    assert not os.path.exists(_plan_file(tmp_path))


@pytest.mark.asyncio
async def test_null_remote_side_is_not_created(tmp_path):
    config_a = _fs_config(tmp_path, "foo")
    config_b = _fs_config(tmp_path, "bar")
    coordinator = _coordinator(tmp_path, config_a, config_b)

    a_args, b_args = await coordinator.ensure_collection(
        COLLECTION,
        COLLECTION,
        None,
        _get_discovered(config_a),
        _get_discovered(config_b),
    )

    assert a_args["collection"] == COLLECTION
    assert b_args["collection"] is None
    assert (tmp_path / "foo" / COLLECTION).is_dir()
    assert not os.path.exists(_plan_file(tmp_path))
