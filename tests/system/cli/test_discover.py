from __future__ import annotations

import json
import os
from textwrap import dedent

import pytest

from vdirsyncer import exceptions
from vdirsyncer.storage.base import Storage


def _load_roster(tmpdir, pair="foobar"):
    path = tmpdir.join("status").join(f"{pair}.collections")
    return json.loads(path.read())


def _roster_entries(tmpdir, pair="foobar"):
    return {entry["name"]: entry for entry in _load_roster(tmpdir, pair)["collections"]}


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


def _patch_storage_side_down(monkeypatch, down_path):
    """Make FilesystemStorage.discover fail for one storage path only."""
    from vdirsyncer.storage.filesystem import FilesystemStorage

    original = FilesystemStorage.discover
    down_path = os.path.normpath(down_path)

    async def discover(cls, path, **kwargs):
        if os.path.normpath(path) == down_path:
            raise RuntimeError("storage side temporarily unreachable")
        async for item in original(path=path, **kwargs):
            yield item

    monkeypatch.setattr(FilesystemStorage, "discover", classmethod(discover))


_FILESYSTEM_PAIR = """
[storage foo]
type = "filesystem"
path = "{base}/foo/"
fileext = ".txt"

[storage bar]
type = "filesystem"
path = "{base}/bar/"
fileext = ".txt"

[pair foobar]
a = "foo"
b = "bar"
"""


def test_discover_marks_missing_pending_and_is_idempotent(tmpdir, runner):
    runner.write_with_general(
        dedent(
            _FILESYSTEM_PAIR
            + """
collections = ["from a"]
"""
        ).format(base=str(tmpdir))
    )

    for name in "abc":
        tmpdir.ensure(f"foo/{name}", dir=True)
        tmpdir.ensure(f"bar/{name}", dir=True)

    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert set(entries) == {"a", "b", "c"}
    assert all(entry["state"] == "active" for entry in entries.values())

    # The collection disappears from both healthy sides.
    tmpdir.join("foo/c").remove()
    tmpdir.join("bar/c").remove()

    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert entries["c"]["state"] == "pending"
    assert entries["a"]["state"] == "active"
    assert entries["b"]["state"] == "active"
    assert "marked pending" in result.output

    # Pending collections are kept on disk but excluded from synchronization.
    result = runner.invoke(["sync"])
    assert not result.exception
    assert "Syncing foobar/c" not in result.output
    assert "Syncing foobar/a" in result.output

    # Re-running with the same discovery result must not change the roster.
    roster_file = tmpdir.join("status").join("foobar.collections")
    before = roster_file.read()
    result = runner.invoke(["discover"])
    assert not result.exception
    assert roster_file.read() == before

    # A reappearing collection is promoted back to active.
    tmpdir.ensure("foo/c", dir=True)
    tmpdir.ensure("bar/c", dir=True)
    result = runner.invoke(["discover"])
    assert not result.exception
    assert _roster_entries(tmpdir)["c"]["state"] == "active"


def test_discover_keeps_records_when_one_side_is_down(tmpdir, runner, monkeypatch):
    runner.write_with_general(
        dedent(
            _FILESYSTEM_PAIR
            + """
collections = ["from a"]
implicit = "create"
"""
        ).format(base=str(tmpdir))
    )

    for name in "abc":
        tmpdir.ensure(f"foo/{name}", dir=True)
        tmpdir.ensure(f"bar/{name}", dir=True)

    assert not runner.invoke(["discover"]).exception

    _patch_storage_side_down(monkeypatch, str(tmpdir.join("bar")))

    # Side a gains a collection; side a (the asserting side) loses another.
    tmpdir.ensure("foo/d", dir=True)
    tmpdir.join("foo/c").remove()

    result = runner.invoke(["discover"])
    assert not result.exception
    assert "keeping its previously known collections" in result.output

    entries = _roster_entries(tmpdir)
    assert entries["a"]["state"] == "active"
    assert entries["b"]["state"] == "active"
    # A newly discovered collection is added even though the other side is down.
    assert entries["d"]["state"] == "active"
    assert entries["d"]["b"]["collection"] == "d"
    # The healthy asserting side can still confirm a disappearance.
    assert entries["c"]["state"] == "pending"

    monkeypatch.undo()
    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert entries["d"]["state"] == "active"
    assert entries["c"]["state"] == "pending"


def test_discover_manual_mapping_is_pinned(tmpdir, runner, monkeypatch):
    runner.write_with_general(
        dedent(
            _FILESYSTEM_PAIR
            + """
collections = [["manual", "ma", "mb"], "plain"]
implicit = "create"
"""
        ).format(base=str(tmpdir))
    )

    tmpdir.ensure("foo/ma", dir=True)
    tmpdir.ensure("bar/mb", dir=True)
    tmpdir.ensure("foo/plain", dir=True)
    tmpdir.ensure("bar/plain", dir=True)

    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert entries["manual"]["origin"] == "manual"
    assert entries["manual"]["state"] == "active"
    manual_before = entries["manual"]

    # Side b becomes unreachable and its manual collection even vanishes:
    # the manual mapping must survive completely untouched.
    _patch_storage_side_down(monkeypatch, str(tmpdir.join("bar")))
    tmpdir.join("bar/mb").remove()
    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert entries["manual"] == manual_before
    assert entries["manual"]["state"] == "active"
    assert entries["plain"]["state"] == "active"

    # After recovery the mapping is still a pinned, active manual entry.
    monkeypatch.undo()
    result = runner.invoke(["discover"])
    assert not result.exception
    entries = _roster_entries(tmpdir)
    assert entries["manual"]["origin"] == "manual"
    assert entries["manual"]["state"] == "active"


def test_failed_discovery_does_not_leave_partial_roster(tmpdir, runner):
    runner.write_with_general(
        dedent(
            _FILESYSTEM_PAIR
            + """
collections = ["from a"]
"""
        ).format(base=str(tmpdir))
    )

    for name in "abc":
        tmpdir.ensure(f"foo/{name}", dir=True)
        tmpdir.ensure(f"bar/{name}", dir=True)

    assert not runner.invoke(["discover"]).exception

    status_dir = tmpdir.join("status")
    roster_file = status_dir.join("foobar.collections")
    before = roster_file.read()

    # Add a non-existent explicit collection and refuse to create it, which
    # aborts discovery before the roster is persisted.
    config = runner.cfg.read().replace(
        'collections = ["from a"]', 'collections = ["from a", "ghost"]'
    )
    runner.cfg.write(config)

    result = runner.invoke(["discover"], input="n\n")
    assert result.exception

    # The previous roster is byte-identical, with no partial state or temp files.
    assert roster_file.read() == before
    roster = json.loads(roster_file.read())
    assert [entry["name"] for entry in roster["collections"]] == ["a", "b", "c"]
    leftovers = [
        entry.basename
        for entry in status_dir.listdir()
        if entry.basename != "foobar.collections"
    ]
    assert leftovers == []
