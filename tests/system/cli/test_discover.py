from __future__ import annotations

import json
from textwrap import dedent

import pytest

from vdirsyncer import exceptions
from vdirsyncer.cli.create_collection import new_create_plan
from vdirsyncer.cli.utils import storage_names
from vdirsyncer.storage.base import Storage
from vdirsyncer.storage.filesystem import FilesystemStorage


class _TransientFailStorage(FilesystemStorage):
    """Fails the first ``transient_failures`` creation attempts."""

    storage_name = "flaky_filesystem"
    transient_failures = 0

    @classmethod
    async def create_collection(cls, collection, **kwargs):
        if cls.transient_failures:
            cls.transient_failures -= 1
            raise OSError("simulated transient I/O failure")
        return await super().create_collection(collection, **kwargs)


class _UnsupportedCreateStorage(FilesystemStorage):
    """Never supports creating collections."""

    storage_name = "unsupported_filesystem"

    @classmethod
    async def create_collection(cls, collection, **kwargs):
        raise NotImplementedError("simulated unsupported storage")


@pytest.fixture
def flaky_storages(monkeypatch):
    _TransientFailStorage.transient_failures = 0
    monkeypatch.setitem(
        storage_names._storages, "flaky_filesystem", _TransientFailStorage
    )
    monkeypatch.setitem(
        storage_names._storages,
        "unsupported_filesystem",
        _UnsupportedCreateStorage,
    )
    return _TransientFailStorage, _UnsupportedCreateStorage


def test_discover_command(tmpdir, runner):
    runner.write_with_general(
        dedent(
            """
    [storage foo]
    type = "filesystem"
    path = "{0}/foo/"
    fileext = ".txt"

    [storage bar]
    type = "filesystem"
    path = "{0}/bar/"
    fileext = ".txt"

    [pair foobar]
    a = "foo"
    b = "bar"
    collections = ["from a"]
    """
        ).format(str(tmpdir))
    )

    foo = tmpdir.mkdir("foo")
    bar = tmpdir.mkdir("bar")

    for x in "abc":
        foo.mkdir(x)
        bar.mkdir(x)
    bar.mkdir("d")

    result = runner.invoke(["discover"])
    assert not result.exception

    foo.mkdir("d")
    result = runner.invoke(["sync"])
    assert not result.exception
    lines = result.output.splitlines()
    assert "Syncing foobar/a" in lines
    assert "Syncing foobar/b" in lines
    assert "Syncing foobar/c" in lines
    assert "Syncing foobar/d" not in result.output

    result = runner.invoke(["discover"])
    assert not result.exception

    result = runner.invoke(["sync"])
    assert not result.exception
    assert "Syncing foobar/a" in lines
    assert "Syncing foobar/b" in lines
    assert "Syncing foobar/c" in lines
    assert "Syncing foobar/d" in result.output

    # Check for redundant data that is already in the config. This avoids
    # copying passwords from the config too.
    assert "fileext" not in tmpdir.join("status").join("foobar.collections").read()


def test_discover_different_collection_names(tmpdir, runner):
    foo = tmpdir.mkdir("foo")
    bar = tmpdir.mkdir("bar")
    runner.write_with_general(
        dedent(
            """
    [storage foo]
    type = "filesystem"
    fileext = ".txt"
    path = "{foo}"

    [storage bar]
    type = "filesystem"
    fileext = ".txt"
    path = "{bar}"

    [pair foobar]
    a = "foo"
    b = "bar"
    collections = [
        ["coll1", "coll_a1", "coll_b1"],
        "coll2"
     ]
    """
        ).format(foo=str(foo), bar=str(bar))
    )

    result = runner.invoke(["discover"], input="y\n" * 6)
    assert not result.exception

    coll_a1 = foo.join("coll_a1")
    coll_b1 = bar.join("coll_b1")

    assert coll_a1.exists()
    assert coll_b1.exists()

    result = runner.invoke(["sync"])
    assert not result.exception

    foo_txt = coll_a1.join("foo.txt")
    foo_txt.write("BEGIN:VCALENDAR\nUID:foo\nEND:VCALENDAR")

    result = runner.invoke(["sync"])
    assert not result.exception

    assert foo_txt.exists()
    assert coll_b1.join("foo.txt").exists()


def test_discover_direct_path(tmpdir, runner):
    foo = tmpdir.join("foo")
    bar = tmpdir.join("bar")

    runner.write_with_general(
        dedent(
            """
    [storage foo]
    type = "filesystem"
    fileext = ".txt"
    path = "{foo}"

    [storage bar]
    type = "filesystem"
    fileext = ".txt"
    path = "{bar}"

    [pair foobar]
    a = "foo"
    b = "bar"
    collections = null
    """
        ).format(foo=str(foo), bar=str(bar))
    )

    result = runner.invoke(["discover"], input="y\n" * 2)
    assert not result.exception

    result = runner.invoke(["sync"])
    assert not result.exception

    assert foo.exists()
    assert bar.exists()


def test_null_collection_with_named_collection(tmpdir, runner):
    runner.write_with_general(
        dedent(
            f"""
    [pair foobar]
    a = "foo"
    b = "bar"
    collections = [["baz", "baz", null]]

    [storage foo]
    type = "filesystem"
    path = "{tmpdir!s}/foo/"
    fileext = ".txt"

    [storage bar]
    type = "singlefile"
    path = "{tmpdir!s}/bar.txt"
    """
        )
    )

    result = runner.invoke(["discover"], input="y\n" * 2)
    assert not result.exception

    foo = tmpdir.join("foo")
    foobaz = foo.join("baz")
    assert foo.exists()
    assert foobaz.exists()

    bar = tmpdir.join("bar.txt")
    assert bar.exists()

    foobaz.join("lol.txt").write("BEGIN:VCARD\nUID:HAHA\nEND:VCARD")

    result = runner.invoke(["sync"])
    assert not result.exception

    assert "HAHA" in bar.read()


@pytest.mark.parametrize(
    ("a_requires", "b_requires"),
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
)
def test_collection_required(a_requires, b_requires, tmpdir, runner, monkeypatch):
    class TestStorage(Storage):
        storage_name = "test"

        def __init__(self, require_collection, **kw):
            if require_collection:
                assert not kw.get("collection")
                raise exceptions.CollectionRequired

        async def get(self, href: str):
            raise NotImplementedError

        async def list(self) -> list[tuple]:
            raise NotImplementedError

    from vdirsyncer.cli.utils import storage_names

    monkeypatch.setitem(storage_names._storages, "test", TestStorage)

    runner.write_with_general(
        dedent(
            f"""
    [pair foobar]
    a = "foo"
    b = "bar"
    collections = null

    [storage foo]
    type = "test"
    require_collection = {json.dumps(a_requires)}

    [storage bar]
    type = "test"
    require_collection = {json.dumps(b_requires)}
    """
        )
    )

    result = runner.invoke(["discover"])
    if a_requires or b_requires:
        assert result.exception
        assert (
            "One or more storages don't support `collections = null`." in result.output
        )


def test_showconfig(tmpdir, runner):
    runner.write_with_general(
        dedent(
            """
    [storage foo]
    type = "filesystem"
    path = "{0}/foo/"
    fileext = ".txt"

    [storage bar]
    type = "filesystem"
    path = "{0}/bar/"
    fileext = ".txt"

    [pair foobar]
    a = "foo"
    b = "bar"
    collections = ["from a"]
    """
        ).format(str(tmpdir))
    )

    result = runner.invoke(["showconfig"])
    assert not result.exception
    assert json.loads(result.output) == {
        "storages": [
            {
                "type": "filesystem",
                "path": f"{tmpdir}/foo/",
                "fileext": ".txt",
                "instance_name": "foo",
            },
            {
                "type": "filesystem",
                "path": f"{tmpdir}/bar/",
                "fileext": ".txt",
                "instance_name": "bar",
            },
        ]
    }


def _write_implicit_pair_config(runner, tmpdir, bar_type):
    runner.write_with_general(
        dedent(
            f"""
    [pair foobar]
    a = "foo"
    b = "bar"
    collections = ["mycoll"]
    implicit = "create"

    [storage foo]
    type = "filesystem"
    path = "{tmpdir}/foo/"
    fileext = ".txt"

    [storage bar]
    type = "{bar_type}"
    path = "{tmpdir}/bar/"
    fileext = ".txt"
    """
        )
    )


def test_partial_creation_resumes_on_rerun(flaky_storages, tmpdir, runner):
    flaky_storage, _ = flaky_storages
    flaky_storage.transient_failures = 1
    _write_implicit_pair_config(runner, tmpdir, "flaky_filesystem")

    result = runner.invoke(["discover"])
    assert result.exception
    assert "simulated transient I/O failure" in result.output

    # Side A was created, side B was not. The plan is persisted, but the
    # collection must not enter the usable cache yet.
    assert tmpdir.join("foo", "mycoll").check(dir=True)
    assert not tmpdir.join("bar", "mycoll").check()
    plan_file = tmpdir.join("status", "foobar", "mycoll.create_plan")
    assert plan_file.check(file=True)
    assert not tmpdir.join("status", "foobar.collections").check()

    # Re-running discovery only completes the missing side.
    result = runner.invoke(["discover"])
    assert not result.exception, result.output
    assert tmpdir.join("bar", "mycoll").check(dir=True)
    assert not plan_file.check()
    assert tmpdir.join("status", "foobar.collections").check(file=True)

    # No duplicate empty collections, and the pair now syncs.
    assert [x.basename for x in tmpdir.join("foo").listdir()] == ["mycoll"]
    assert [x.basename for x in tmpdir.join("bar").listdir()] == ["mycoll"]
    result = runner.invoke(["sync"])
    assert not result.exception, result.output


def test_terminal_failure_rolls_back_empty_created_collection(
    flaky_storages, tmpdir, runner
):
    _write_implicit_pair_config(runner, tmpdir, "unsupported_filesystem")

    result = runner.invoke(["discover"])
    assert result.exception
    assert "Unable to find or create collection" in result.output

    # The empty collection created on A is rolled back, nothing remains.
    assert not tmpdir.join("foo", "mycoll").check()
    assert not tmpdir.join("bar", "mycoll").check()
    assert not tmpdir.join("status", "foobar", "mycoll.create_plan").check()
    assert not tmpdir.join("status", "foobar.collections").check()


def test_repeated_failures_leave_no_duplicate_collections(
    flaky_storages, tmpdir, runner
):
    flaky_storage, _ = flaky_storages
    flaky_storage.transient_failures = 3
    _write_implicit_pair_config(runner, tmpdir, "flaky_filesystem")

    for _ in range(3):
        result = runner.invoke(["discover"])
        assert result.exception
        # A exists exactly once while B keeps failing; no duplicates accrue.
        assert [x.basename for x in tmpdir.join("foo").listdir()] == ["mycoll"]
        assert not tmpdir.join("bar", "mycoll").check()

    result = runner.invoke(["discover"])
    assert not result.exception, result.output
    assert [x.basename for x in tmpdir.join("foo").listdir()] == ["mycoll"]
    assert [x.basename for x in tmpdir.join("bar").listdir()] == ["mycoll"]


def test_preexisting_collection_survives_failed_creation(
    flaky_storages, tmpdir, runner
):
    preexisting = tmpdir.mkdir("foo").mkdir("mycoll")
    preexisting.join("item.txt").write("BEGIN:VCARD\nUID:item\nEND:VCARD")

    _write_implicit_pair_config(runner, tmpdir, "unsupported_filesystem")

    result = runner.invoke(["discover"])
    assert result.exception

    # The pre-existing collection and its item are never deleted.
    assert preexisting.join("item.txt").check(file=True)
    assert not tmpdir.join("bar", "mycoll").check()
    assert not tmpdir.join("status", "foobar", "mycoll.create_plan").check()


def test_corrupt_create_plan_fails_explicitly(flaky_storages, tmpdir, runner):
    _write_implicit_pair_config(runner, tmpdir, "flaky_filesystem")
    tmpdir.mkdir("status").mkdir("foobar").join("mycoll.create_plan").write(
        "{not valid json"
    )

    result = runner.invoke(["discover"])
    assert result.exception
    assert "corrupt" in result.output

    # Nothing is created or deleted while the plan is corrupt.
    assert not tmpdir.join("foo", "mycoll").check()
    assert not tmpdir.join("bar", "mycoll").check()


def test_create_plan_config_drift_fails_explicitly(
    flaky_storages, tmpdir, runner
):
    _write_implicit_pair_config(runner, tmpdir, "flaky_filesystem")
    # The plan remembers a different A-side collection name than the config.
    plan = new_create_plan("mycoll", "other_a", "mycoll")
    tmpdir.mkdir("status").mkdir("foobar").join("mycoll.create_plan").write(
        json.dumps(plan)
    )

    result = runner.invoke(["discover"])
    assert result.exception
    assert "resolves it to" in result.output

    assert not tmpdir.join("foo", "mycoll").check()
    assert not tmpdir.join("bar", "mycoll").check()
