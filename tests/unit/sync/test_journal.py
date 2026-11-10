"""Tests for the recoverable action journal and crash recovery."""

from __future__ import annotations

import contextlib
import os
import sqlite3

import pytest

from tests import blow_up
from vdirsyncer.storage.memory import MemoryStorage
from vdirsyncer.sync import sync as sync_func
from vdirsyncer.sync.exceptions import AmbiguousRemoteResult
from vdirsyncer.sync.exceptions import JournalCorrupted
from vdirsyncer.sync.journal import ActionJournal
from vdirsyncer.sync.status import SqliteStatus
from vdirsyncer.vobject import Item


class InstrumentedStorage(MemoryStorage):
    """MemoryStorage with call counters and configurable crash injection.

    ``fail_always`` makes the mutating call raise before anything is written.
    ``fail_after_apply`` writes the change first and then raises, simulating a
    network timeout where the server actually applied the request.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.upload_calls = []
        self.update_calls = []
        self.delete_calls = []
        self.fail_always = set()
        self.fail_after_apply = set()

    async def upload(self, item):
        self.upload_calls.append(item.ident)
        if item.ident in self.fail_always:
            raise RuntimeError("simulated failure before upload")
        result = await super().upload(item)
        if item.ident in self.fail_after_apply:
            raise RuntimeError("simulated failure after upload")
        return result

    async def update(self, href, item, etag):
        self.update_calls.append(item.ident)
        if item.ident in self.fail_always:
            raise RuntimeError("simulated failure before update")
        result = await super().update(href, item, etag)
        if item.ident in self.fail_after_apply:
            raise RuntimeError("simulated failure after update")
        return result

    async def delete(self, href, etag):
        ident = self.items[href][1].ident
        self.delete_calls.append(ident)
        if ident in self.fail_always:
            raise RuntimeError("simulated failure before delete")
        await super().delete(href, etag)
        if ident in self.fail_after_apply:
            raise RuntimeError("simulated failure after delete")


def raw_items(storage):
    return {item.raw for _etag, item in storage.items.values()}


@pytest.fixture
def status_paths(tmp_path):
    status_path = tmp_path / "pair.items"
    return str(status_path), str(status_path) + ".journal"


async def run_sync(a, b, status_path, **kwargs):
    with contextlib.closing(SqliteStatus(status_path)) as status:
        await sync_func(a, b, status, **kwargs)


def open_journal(journal_path):
    return ActionJournal(journal_path)


@pytest.mark.asyncio
async def test_journal_is_used_and_cleared(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))

    await run_sync(a, b, status_path)

    assert os.path.exists(journal_path)
    assert os.stat(journal_path).st_mode & 0o777 == 0o600
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()
        assert journal.iter_live() == []


@pytest.mark.asyncio
async def test_interrupted_uploads_do_not_get_repeated(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    await a.upload(Item("UID:2"))
    # Both uploads are applied remotely, but the client only sees an error
    # for the second one.
    b.fail_after_apply = {"2"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # The status transaction rolled back, the journal did not.
    with contextlib.closing(SqliteStatus(status_path)) as status:
        assert list(status.iter_old()) == []
    with open_journal(journal_path) as journal:
        entries = journal.iter_live()
        assert [e.ident for e in entries] == ["1", "2"]
        assert [e.state for e in entries] == ["done", "pending"]

    b.fail_after_apply = set()
    await run_sync(a, b, status_path)

    # Exactly one upload per item, no duplicates on the storage.
    assert sorted(b.upload_calls) == ["1", "2"]
    assert len(b.items) == 2
    assert raw_items(a) == raw_items(b)

    with contextlib.closing(SqliteStatus(status_path)) as status:
        assert set(status.iter_old()) == {"1", "2"}
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_failed_upload_is_retried_once(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    await a.upload(Item("UID:2"))
    # The second upload never reaches the storage.
    b.fail_always = {"2"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    assert len(b.items) == 1
    b.fail_always = set()
    await run_sync(a, b, status_path)

    assert sorted(b.upload_calls) == ["1", "2", "2"]
    assert len(b.items) == 2
    assert raw_items(a) == raw_items(b)
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_interrupted_update_adopts_remote_etag(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    href_a, etag_a = await a.upload(Item("UID:1"))
    await run_sync(a, b, status_path)

    updated = Item("UID:1\nversion: 2")
    await a.update(href_a, updated, etag_a)
    b.fail_after_apply = {"1"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # The update was applied remotely; the second run must not update again.
    b.fail_after_apply = set()
    await run_sync(a, b, status_path)

    assert b.update_calls == ["1"]
    assert raw_items(a) == raw_items(b) == {updated.raw}
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_pending_update_is_retried(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    href_a, etag_a = await a.upload(Item("UID:1"))
    await run_sync(a, b, status_path)

    updated = Item("UID:1\nversion: 2")
    await a.update(href_a, updated, etag_a)
    b.fail_always = {"1"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    assert raw_items(b) == {"UID:1"}
    calls_before_retry = list(b.update_calls)
    b.fail_always = set()
    await run_sync(a, b, status_path)

    assert b.update_calls == calls_before_retry + ["1"]
    assert raw_items(a) == raw_items(b) == {updated.raw}
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_interrupted_delete_is_not_repeated(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage(fileext=".a")
    b = InstrumentedStorage(fileext=".b")
    await a.upload(Item("UID:1"))
    await a.upload(Item("UID:2"))
    await run_sync(a, b, status_path)

    b.fail_after_apply = {"1"}
    await a.delete("1.a", (await a.get("1.a"))[1])

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    assert "1.b" not in b.items
    b.fail_after_apply = set()
    await run_sync(a, b, status_path)

    # No second delete attempt, nothing was re-uploaded.
    assert b.delete_calls == ["1"]
    assert sorted(b.upload_calls) == ["1", "2"]
    assert raw_items(a) == raw_items(b) == {"UID:2"}
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_ambiguous_same_uid_aborts_and_keeps_data(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    b.fail_after_apply = {"1"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # A different item with the same UID now exists on the target storage.
    b.items["1"] = (b.items["1"][0], Item("UID:1\nsomeone else was here"))
    b.fail_after_apply = set()
    b.upload = b.update = b.delete = blow_up

    with pytest.raises(AmbiguousRemoteResult):
        await run_sync(a, b, status_path)

    # The ambiguous remote data and the source item are both untouched.
    assert raw_items(b) == {"UID:1\nsomeone else was here"}
    assert raw_items(a) == {"UID:1"}
    # The journal is retained for manual inspection.
    with open_journal(journal_path) as journal:
        assert journal.has_entries()


@pytest.mark.asyncio
async def test_corrupted_journal_fails_without_writes(status_paths):
    status_path, journal_path = status_paths
    a = MemoryStorage()
    b = MemoryStorage()
    await a.upload(Item("UID:1"))
    with open(journal_path, "wb") as f:
        f.write(b"this is definitely not a sqlite database")

    b.upload = b.update = b.delete = blow_up
    with pytest.raises(JournalCorrupted):
        await run_sync(a, b, status_path)

    assert raw_items(a) == {"UID:1"}
    assert not b.items


@pytest.mark.asyncio
async def test_empty_journal_file_is_corruption(status_paths):
    status_path, journal_path = status_paths
    a = MemoryStorage()
    b = MemoryStorage()
    await a.upload(Item("UID:1"))
    open(journal_path, "wb").close()

    with pytest.raises(JournalCorrupted):
        await run_sync(a, b, status_path)


@pytest.mark.asyncio
async def test_manual_items_survive_recovery(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    # Item manually placed on the target storage, never part of any sync.
    await b.upload(Item("UID:manual"))
    b.fail_after_apply = {"1"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    b.fail_after_apply = set()
    await run_sync(a, b, status_path)

    assert {"1", "manual"} <= set(b.items)
    assert raw_items(a) == raw_items(b)
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_pending_update_with_vanished_target_is_abandoned(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    href_a, etag_a = await a.upload(Item("UID:1"))
    await run_sync(a, b, status_path)

    await a.update(href_a, Item("UID:1\nversion: 2"), etag_a)
    b.fail_always = {"1"}
    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # The target item disappears before the retry (manual removal).
    b.items.clear()
    b.fail_always = set()
    # Storage B is now empty, the StorageEmpty safety check applies.
    await run_sync(a, b, status_path, force_delete=True)

    assert raw_items(a) == raw_items(b) == {"UID:1\nversion: 2"}
    # The stale update was not retried; the planner uploaded fresh content.
    assert b.update_calls == ["1"]
    assert b.upload_calls == ["1", "1"]
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_stale_delete_against_modified_item_is_abandoned(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage(fileext=".a")
    b = InstrumentedStorage(fileext=".b")
    await a.upload(Item("UID:1"))
    await a.upload(Item("UID:2"))
    await run_sync(a, b, status_path)

    await a.delete("1.a", (await a.get("1.a"))[1])
    b.fail_always = {"1"}
    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # The item slated for deletion was modified independently in the meantime.
    b.items["1.b"] = ("changed-etag", Item("UID:1\nmodified externally"))
    b.fail_always = set()
    await run_sync(a, b, status_path)

    # The unfamiliar data was never deleted; it propagates to the other side.
    assert b.delete_calls == ["1"]
    assert raw_items(a) == raw_items(b) == {"UID:2", "UID:1\nmodified externally"}
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_diverged_update_target_aborts(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    href_a, etag_a = await a.upload(Item("UID:1"))
    await run_sync(a, b, status_path)

    await a.update(href_a, Item("UID:1\nversion: 2"), etag_a)
    b.fail_always = {"1"}
    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)

    # The target item was changed independently: neither etag nor content
    # match the journaled update.
    b.items["1"] = ("other-etag", Item("UID:1\nindependent edit"))
    b.fail_always = set()
    b.upload = b.update = b.delete = blow_up

    with pytest.raises(AmbiguousRemoteResult):
        await run_sync(a, b, status_path)

    assert raw_items(b) == {"UID:1\nindependent edit"}
    with open_journal(journal_path) as journal:
        assert journal.has_entries()


@pytest.mark.asyncio
async def test_superseded_pending_action_uses_new_content(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    b.fail_always = {"1"}

    with pytest.raises(RuntimeError):
        await run_sync(a, b, status_path)
    assert not b.items

    # The source item changes before the retry; the old intent is stale.
    a.items["1"] = ("new-etag", Item("UID:1\nversion: 2"))
    b.fail_always = set()
    await run_sync(a, b, status_path)

    assert raw_items(b) == {"UID:1\nversion: 2"}
    assert b.upload_calls == ["1", "1"]
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


@pytest.mark.asyncio
async def test_pending_records_survive_error_callback(status_paths):
    status_path, journal_path = status_paths
    a = InstrumentedStorage()
    b = InstrumentedStorage()
    await a.upload(Item("UID:1"))
    b.fail_always = {"1"}

    errors = []
    with contextlib.closing(SqliteStatus(status_path)) as status:
        await sync_func(a, b, status, error_callback=errors.append)
    assert len(errors) == 1

    with open_journal(journal_path) as journal:
        entries = journal.iter_live()
        assert len(entries) == 1 and entries[0].state == "pending"

    b.fail_always = set()
    await run_sync(a, b, status_path)
    assert raw_items(a) == raw_items(b)
    with open_journal(journal_path) as journal:
        assert not journal.has_entries()


def test_journal_fingerprint_reuse_and_supersede():
    journal = ActionJournal(":memory:")
    seq = journal.before_write(
        ident="1",
        action="upload",
        side="b",
        item_hash="h1",
        href=None,
        etag=None,
    )
    assert (
        journal.before_write(
            ident="1",
            action="upload",
            side="b",
            item_hash="h1",
            href=None,
            etag=None,
        )
        == seq
    )

    new_seq = journal.before_write(
        ident="1",
        action="upload",
        side="b",
        item_hash="h2",
        href=None,
        etag=None,
    )
    assert new_seq != seq
    entries = journal.iter_live()
    assert len(entries) == 1
    assert entries[0].item_hash == "h2"
    journal.close()


def test_journal_detects_duplicate_records(tmp_path):
    path = str(tmp_path / "journal.sqlite")
    journal = ActionJournal(path)
    seq = journal.before_write(
        ident="1",
        action="upload",
        side="b",
        item_hash="h1",
        href=None,
        etag=None,
    )
    journal.close()

    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO action_journal (ident, action, side, item_hash, state) "
        "VALUES ('1', 'upload', 'b', 'h1', 'pending')"
    )
    conn.commit()
    conn.close()
    with pytest.raises(JournalCorrupted):
        ActionJournal(path)

    conn = sqlite3.connect(path)
    conn.execute("DELETE FROM action_journal WHERE seq != ?", (seq,))
    conn.execute(
        "INSERT INTO action_journal (ident, action, side, item_hash, state) "
        "VALUES ('1', 'upload', 'b', 'different-hash', 'pending')"
    )
    conn.commit()
    conn.close()
    with pytest.raises(AmbiguousRemoteResult):
        ActionJournal(path)


def test_journal_finalize_keeps_pending_records():
    journal = ActionJournal(":memory:")
    seq = journal.before_write(
        ident="1",
        action="delete",
        side="b",
        item_hash=None,
        href="h",
        etag="e",
    )
    journal.finalize()
    assert len(journal.iter_live()) == 1

    journal.mark_done(seq, "h", "e")
    journal.finalize()
    assert journal.iter_live() == []
    journal.close()
