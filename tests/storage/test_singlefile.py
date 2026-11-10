from __future__ import annotations

import contextlib
import json
import os

import pytest

from tests import assert_item_equals
from vdirsyncer import exceptions
from vdirsyncer.storage.memory import MemoryStorage
from vdirsyncer.storage.singlefile import SingleFileStorage
from vdirsyncer.storage.singlefile import _build_pending_record
from vdirsyncer.storage.singlefile import _PendingJournal
from vdirsyncer.storage.singlefile import _record_digest
from vdirsyncer.sync import sync
from vdirsyncer.sync.status import SqliteStatus
from vdirsyncer.vobject import Item

from . import StorageTests


class TestSingleFileStorage(StorageTests):
    storage_class = SingleFileStorage
    supports_metadata = False

    @pytest.fixture
    def get_storage_args(self, tmpdir):
        async def inner(collection="test"):
            rv = {"path": str(tmpdir.join("%s.txt")), "collection": collection}
            if collection is not None:
                rv = await self.storage_class.create_collection(**rv)
            return rv

        return inner


def _event(uid, summary=None):
    return Item(
        "\r\n".join(
            (
                "BEGIN:VCALENDAR",
                "VERSION:2.0",
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"SUMMARY:{summary or uid}",
                "END:VEVENT",
                "END:VCALENDAR",
            )
        )
    )


def _read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def _bump_externally(path):
    """Rewrite the same bytes so the storage file's etag (mtime) changes."""
    content = _read_bytes(path)
    with open(path, "wb") as f:
        f.write(content)


def _append_externally(path, item):
    """Simulate another program appending an item to the storage file."""
    with open(path, "ab") as f:
        f.write(item.raw.encode("utf-8"))


async def _interrupted_session(storage, *awaitables):
    """Run mutations inside ``at_once`` while the file is changed externally.

    The write precondition fails, so the session ends with
    ``PreconditionFailed`` and its changes have to land in the pending journal.
    """
    with pytest.raises(exceptions.PreconditionFailed) as excinfo:
        async with storage.at_once():
            for awaitable in awaitables:
                await awaitable
            _bump_externally(storage.path)
    # Guard against other PreconditionFailed subclasses (e.g. AlreadyExisting).
    assert type(excinfo.value) is exceptions.PreconditionFailed


class TestSingleFilePendingChanges:
    @pytest.fixture
    def path(self, tmpdir):
        return str(tmpdir.ensure("calendar.ics"))

    @pytest.fixture
    def storage(self, path):
        return SingleFileStorage(path=path)

    @pytest.mark.asyncio
    async def test_normal_write_leaves_no_journal(self, storage, path):
        await storage.upload(_event("a"))
        assert not _PendingJournal(path).exists()

    @pytest.mark.asyncio
    async def test_interruption_without_changes_keeps_no_journal(self, storage, path):
        with pytest.raises(exceptions.PreconditionFailed):
            async with storage.at_once():
                _bump_externally(path)
        assert not _PendingJournal(path).exists()

    @pytest.mark.asyncio
    async def test_stale_temp_files_are_cleaned_up(self, storage, path):
        journal = _PendingJournal(path)
        os.makedirs(journal.dir, exist_ok=True)
        with open(os.path.join(journal.dir, ".tmp-abandoned"), "wb") as f:
            f.write(b"partial write")

        async with storage.at_once():
            await storage.upload(_event("a"))

        assert not journal.exists()
        assert_item_equals((await storage.get("a"))[0], _event("a"))

    @pytest.mark.asyncio
    async def test_interrupted_write_persists_pending_record(self, storage, path):
        item_a = _event("a")
        await _interrupted_session(storage, storage.upload(item_a))

        # The confirmed file is untouched...
        assert _read_bytes(path) == b""

        # ...but the intended change survived as a checksummed record.
        journal = _PendingJournal(path)
        assert journal.exists()
        records = journal.load()
        assert len(records) == 1
        record = records[0]
        assert record["version"] == 1
        assert record["sequence"] == 1
        assert record["prev_digest"] is None
        assert record["digest"].startswith("sha256:")
        assert isinstance(record["base_etag"], str)

        operations = record["operations"]
        assert [(op["href"], op["op"]) for op in operations] == [("a", "add")]
        assert operations[0]["hash"] == item_a.hash
        assert Item(operations[0]["item"]).hash == item_a.hash

    @pytest.mark.asyncio
    async def test_two_interruptions_keep_ordered_chain(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a")))

        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("b")))

        journal = _PendingJournal(path)
        assert sorted(os.listdir(journal.dir)) == [
            "00000001.json",
            "00000002.json",
        ]
        records = journal.load()
        assert [record["sequence"] for record in records] == [1, 2]
        assert records[1]["prev_digest"] == records[0]["digest"]
        assert [op["href"] for record in records for op in record["operations"]] == [
            "a",
            "b",
        ]

    @pytest.mark.asyncio
    async def test_replay_merges_distinct_uids_and_clears_journal(self, storage, path):
        item_a = _event("a")
        item_b = _event("b")
        item_c = _event("c")
        async with storage.at_once():
            await storage.upload(item_a)
            await storage.upload(item_b)
            await storage.upload(item_c)

        # First interrupted sync: update a, delete b.
        storage = SingleFileStorage(path=path)
        item_a2 = _event("a", "a-updated")
        await _interrupted_session(
            storage,
            storage.update("a", item_a2, (await storage.get("a"))[1]),
            storage.delete("b", (await storage.get("b"))[1]),
        )

        # Second interrupted sync: update c, add d.
        storage = SingleFileStorage(path=path)
        item_c2 = _event("c", "c-updated")
        item_d = _event("d")
        await _interrupted_session(
            storage,
            storage.update("c", item_c2, (await storage.get("c"))[1]),
            storage.upload(item_d),
        )

        # The confirmed file still only contains the original three items.
        storage = SingleFileStorage(path=path)
        async with storage.at_once():
            pass
        hrefs = [href async for href, etag in storage.list()]
        assert hrefs == ["a", "c", "d"]
        assert_item_equals((await storage.get("a"))[0], item_a2)
        assert_item_equals((await storage.get("c"))[0], item_c2)
        assert_item_equals((await storage.get("d"))[0], item_d)
        with pytest.raises(exceptions.NotFoundError):
            await storage.get("b")

        # Applied records are gone, and a re-run doesn't apply them again.
        assert not _PendingJournal(path).exists()
        storage = SingleFileStorage(path=path)
        assert [href async for href, etag in storage.list()] == ["a", "c", "d"]
        assert not _PendingJournal(path).exists()

    @pytest.mark.asyncio
    async def test_external_changes_survive_replay(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a")))

        external_item = _event("external")
        _append_externally(path, external_item)

        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("b")))

        storage = SingleFileStorage(path=path)
        async with storage.at_once():
            pass

        hrefs = {href async for href, etag in storage.list()}
        assert hrefs == {"a", "b", "external"}
        assert not _PendingJournal(path).exists()

    @pytest.mark.asyncio
    async def test_same_uid_divergent_outcomes_reject_entire_batch(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a", "v1")))

        # The second session already sees the replayed v1 item, so its
        # divergent intention is expressed as an update to v2.
        storage = SingleFileStorage(path=path)
        await _interrupted_session(
            storage,
            storage.update("a", _event("a", "v2"), (await storage.get("a"))[1]),
        )

        assert _read_bytes(path) == b""
        storage = SingleFileStorage(path=path)
        with pytest.raises(exceptions.PendingChangesError):
            async with storage.at_once():
                pass

        # Nothing was applied and every record is retained.
        assert _read_bytes(path) == b""
        assert len(_PendingJournal(path).load()) == 2

    @pytest.mark.asyncio
    async def test_same_uid_update_vs_delete_rejects_entire_batch(self, storage, path):
        async with storage.at_once():
            await storage.upload(_event("x"))

        storage = SingleFileStorage(path=path)
        item_x2 = _event("x", "x-updated")
        await _interrupted_session(
            storage,
            storage.update("x", item_x2, (await storage.get("x"))[1]),
        )

        storage = SingleFileStorage(path=path)
        await _interrupted_session(
            storage,
            storage.delete("x", (await storage.get("x"))[1]),
        )

        before = _read_bytes(path)
        storage = SingleFileStorage(path=path)
        with pytest.raises(exceptions.PendingChangesError):
            async with storage.at_once():
                pass
        assert _read_bytes(path) == before
        assert len(_PendingJournal(path).load()) == 2

    @pytest.mark.asyncio
    async def test_corrupt_checksum_preserves_file_and_all_records(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a")))
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("b")))

        journal = _PendingJournal(path)
        record_paths = journal._record_paths()
        with open(record_paths[1]) as f:
            record = json.load(f)
        record["operations"][0]["item"] = record["operations"][0]["item"].replace(
            "SUMMARY:b", "SUMMARY:tampered"
        )
        with open(record_paths[1], "w") as f:
            json.dump(record, f)

        before = _read_bytes(path)
        storage = SingleFileStorage(path=path)
        with pytest.raises(exceptions.PendingChangesError) as excinfo:
            async with storage.at_once():
                pass
        assert "checksum" in str(excinfo.value)

        assert _read_bytes(path) == before
        assert os.path.exists(record_paths[0])
        assert os.path.exists(record_paths[1])

    @pytest.mark.asyncio
    async def test_broken_hash_chain_preserves_everything(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a")))
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("b")))

        journal = _PendingJournal(path)
        record_path = journal._record_paths()[1]
        with open(record_path) as f:
            record = json.load(f)
        # Keep the per-record checksum valid so only the chain is broken.
        record["prev_digest"] = "sha256:" + "0" * 64
        record["digest"] = _record_digest(
            {key: value for key, value in record.items() if key != "digest"}
        )
        with open(record_path, "w") as f:
            json.dump(record, f)

        before = _read_bytes(path)
        storage = SingleFileStorage(path=path)
        with pytest.raises(exceptions.PendingChangesError) as excinfo:
            async with storage.at_once():
                pass
        assert "hash chain" in str(excinfo.value)

        assert _read_bytes(path) == before
        assert len(journal._record_paths()) == 2

    @pytest.mark.asyncio
    async def test_missing_record_in_chain_is_detected(self, path):
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("a")))
        storage = SingleFileStorage(path=path)
        await _interrupted_session(storage, storage.upload(_event("b")))

        journal = _PendingJournal(path)
        os.rename(
            os.path.join(journal.dir, "00000002.json"),
            os.path.join(journal.dir, "00000003.json"),
        )

        storage = SingleFileStorage(path=path)
        with pytest.raises(exceptions.PendingChangesError):
            async with storage.at_once():
                pass
        assert len(journal._record_paths()) == 2

    @pytest.mark.asyncio
    async def test_replay_is_idempotent_after_missed_cleanup(self, path):
        storage = SingleFileStorage(path=path)
        item_a = _event("a")
        await _interrupted_session(storage, storage.upload(item_a))

        storage = SingleFileStorage(path=path)
        async with storage.at_once():
            pass
        assert not _PendingJournal(path).exists()

        # Simulate a crash between the file write and the journal cleanup:
        # the already-applied record shows up again.
        journal = _PendingJournal(path)
        os.makedirs(journal.dir, exist_ok=True)
        record = _build_pending_record(
            1,
            None,
            None,
            [
                {
                    "href": "a",
                    "op": "add",
                    "hash": item_a.hash,
                    "item": item_a.raw,
                }
            ],
        )
        with open(os.path.join(journal.dir, "00000001.json"), "w") as f:
            json.dump(record, f)

        storage = SingleFileStorage(path=path)
        async with storage.at_once():
            pass

        hrefs = [href async for href, etag in storage.list()]
        assert hrefs == ["a"]
        assert not _PendingJournal(path).exists()

    @pytest.mark.asyncio
    async def test_full_sync_replays_pending_changes(self, path, monkeypatch):
        import vdirsyncer.storage.singlefile as singlefile_module

        singlefile = SingleFileStorage(path=path)
        memory = MemoryStorage()
        item = _event("sync-uid")
        await memory.upload(item)

        # Force the first sync's write precondition to fail: the etag lookup
        # done while listing returns the real value, the one done while
        # writing returns a stale value. The interrupted changes must then be
        # journaled instead of being lost.
        real_get_etag = singlefile_module.get_etag_from_file
        calls = 0

        def flaky_get_etag(target_path):
            nonlocal calls
            calls += 1
            return "0;0" if calls == 2 else real_get_etag(target_path)

        monkeypatch.setattr(singlefile_module, "get_etag_from_file", flaky_get_etag)
        with contextlib.closing(SqliteStatus(":memory:")) as status:
            with pytest.raises(exceptions.PreconditionFailed):
                await sync(memory, singlefile, status)
            assert len(_PendingJournal(path).load()) == 1

            monkeypatch.undo()
            await sync(
                memory,
                singlefile,
                status,
                conflict_resolution="a wins",
            )

        assert not _PendingJournal(path).exists()
        assert [href async for href, etag in singlefile.list()] == ["sync-uid"]
