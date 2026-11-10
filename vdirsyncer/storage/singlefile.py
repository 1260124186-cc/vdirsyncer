from __future__ import annotations

import collections
import contextlib
import functools
import glob
import hashlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Iterable
from datetime import datetime
from datetime import timezone

from vdirsyncer import exceptions
from vdirsyncer.utils import atomic_write
from vdirsyncer.utils import checkfile
from vdirsyncer.utils import expand_path
from vdirsyncer.utils import get_etag_from_file
from vdirsyncer.utils import uniq
from vdirsyncer.vobject import Item
from vdirsyncer.vobject import hash_item
from vdirsyncer.vobject import join_collection
from vdirsyncer.vobject import split_collection

from .base import Storage

logger = logging.getLogger(__name__)

#: Sidecar directory, next to the storage file, holding pending change records.
_PENDING_DIR_SUFFIX = ".pending"
_RECORD_FILENAME_TEMPLATE = "{:08d}.json"
_RECORD_FILENAME_RE = re.compile(r"\d{8}\.json$")
_RECORD_VERSION = 1
_DIGEST_PREFIX = "sha256:"

_OP_ADD = "add"
_OP_UPDATE = "update"
_OP_DELETE = "delete"
_KNOWN_OPS = frozenset((_OP_ADD, _OP_UPDATE, _OP_DELETE))


def _writing_op(f):
    """Implement at_once for write operations.

    Wrap an operation which writes to the storage, implementing `at_once` if it has been
    requested. Changes are stored in-memory until the at_once block finishes, at which
    time they are all written at once.
    """

    @functools.wraps(f)
    async def inner(self, *args, **kwargs):
        if self._items is None or not self._at_once:
            async for _ in self.list():
                pass
        assert self._items is not None
        rv = await f(self, *args, **kwargs)
        if not self._at_once:
            self._write()
        return rv

    return inner


def _record_digest(payload):
    """Checksum over the canonical JSON encoding of a record payload."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return _DIGEST_PREFIX + hashlib.sha256(encoded).hexdigest()


def _build_pending_record(sequence, base_etag, prev_digest, operations):
    """Build a self-describing, checksummed pending change record."""
    payload = {
        "version": _RECORD_VERSION,
        "sequence": sequence,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "base_etag": base_etag,
        "prev_digest": prev_digest,
        "operations": operations,
    }
    record = dict(payload)
    record["digest"] = _record_digest(payload)
    return record


def _diff_operations(base_items, target_items):
    """Turn two item snapshots into an ordered list of per-href operations.

    ``base_items`` is the confirmed file content the session started from,
    ``target_items`` the content the session wanted to write. Only hrefs the
    session actually touched show up, so externally modified items stay
    untouched during replay.
    """
    operations = []
    for href, (_item, etag) in base_items.items():
        if href not in target_items:
            operations.append(
                {"href": href, "op": _OP_DELETE, "hash": None, "item": None}
            )
        else:
            new_item, new_etag = target_items[href]
            if new_etag != etag:
                operations.append(
                    {
                        "href": href,
                        "op": _OP_UPDATE,
                        "hash": new_item.hash,
                        "item": new_item.raw,
                    }
                )

    for href, (item, _etag) in target_items.items():
        if href not in base_items:
            operations.append(
                {
                    "href": href,
                    "op": _OP_ADD,
                    "hash": item.hash,
                    "item": item.raw,
                }
            )

    return operations


def _check_record_consistency(records):
    """Cross-record rule: records touching the same href must agree on the
    exact outcome (operation and target content hash).

    Returns a list of human-readable problems; an empty list means the
    records are consistent. Any problem causes the whole batch to be refused.
    """
    outcomes = {}
    problems = []
    for record in records:
        for op in record["operations"]:
            href = op["href"]
            outcome = (op["op"], op["hash"])
            previous = outcomes.get(href)
            if previous is not None and previous != outcome:
                problems.append(
                    f"Item {href!r} has conflicting pending outcomes: "
                    f"{previous[0]}({previous[1]}) vs {outcome[0]}({outcome[1]})"
                )
            else:
                outcomes[href] = outcome
    return problems


def _apply_pending_operations(items, records, storage_path):
    """Fold verified records over the current confirmed item snapshot.

    ``items`` is mutated in place. Replaying an already-applied record is a
    no-op, so crashes between the file write and the journal cleanup don't
    duplicate changes. Any mismatch aborts the whole batch before the storage
    file is touched.
    """
    problems = []
    for record in records:
        for op in record["operations"]:
            href = op["href"]
            kind = op["op"]

            if kind == _OP_DELETE:
                items.pop(href, None)
                continue

            item = Item(op["item"])
            if kind == _OP_ADD:
                if href not in items:
                    items[href] = item, item.hash
                elif items[href][1] != op["hash"]:
                    problems.append(
                        f"Item {href!r} can't be added: it already exists with "
                        "different content."
                    )
            else:
                assert kind == _OP_UPDATE
                if href not in items:
                    problems.append(
                        f"Item {href!r} can't be updated: it no longer exists."
                    )
                elif items[href][1] != op["hash"]:
                    # Replace the value in place to preserve item ordering.
                    items[href] = item, item.hash

    if problems:
        raise exceptions.PendingChangesError(
            f"Pending changes for {storage_path!r} can't be applied to the "
            "current file.",
            path=storage_path,
            problems=problems,
        )


class _PendingJournal:
    """Append-only journal of changes that couldn't be committed.

    The journal lives in a sidecar directory next to the storage file. Each
    record is a standalone JSON file whose name encodes its position in the
    sequence. Records form a hash chain (``prev_digest``) and carry a payload
    checksum (``digest``), so truncation, reordering, tampering and partial
    writes are all detected on load.
    """

    def __init__(self, storage_path):
        self.storage_path = storage_path
        self.dir = storage_path + _PENDING_DIR_SUFFIX

    def exists(self):
        return os.path.isdir(self.dir)

    def _record_paths(self):
        if not self.exists():
            return []
        names = (
            name for name in os.listdir(self.dir) if _RECORD_FILENAME_RE.match(name)
        )
        return [os.path.join(self.dir, name) for name in sorted(names)]

    def load(self):
        """Read and verify every record, in sequence order.

        :raises PendingChangesError: if a record is unreadable, its checksum
            doesn't match, the hash chain is broken, the sequence has gaps or
            an embedded operation is invalid. The journal and the storage
            file are never modified when this happens.
        """
        problems = []
        parsed_records = []
        for path in self._record_paths():
            name = os.path.basename(path)
            try:
                with open(path, "rb") as f:
                    record = json.loads(f.read().decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
                problems.append(f"Record {name} is unreadable: {e}")
                parsed_records.append(None)
                continue

            if not isinstance(record, dict):
                problems.append(f"Record {name} doesn't contain a JSON object.")
                parsed_records.append(None)
            else:
                parsed_records.append((name, record))

        records = []
        prev_digest = None
        for index, parsed in enumerate(parsed_records):
            expected_sequence = index + 1
            if parsed is None:
                # An unreadable record breaks the chain for its successors.
                prev_digest = None
                continue

            name, record = parsed
            record_problems = []

            if name != _RECORD_FILENAME_TEMPLATE.format(expected_sequence):
                record_problems.append(
                    f"filename {name!r} doesn't match its position in the chain"
                )

            if record.get("version") != _RECORD_VERSION:
                record_problems.append(
                    f"unsupported record version {record.get('version')!r}"
                )

            sequence = record.get("sequence")
            if sequence != expected_sequence:
                record_problems.append(
                    f"expected sequence {expected_sequence}, found {sequence!r}"
                )

            digest = record.get("digest")
            if not isinstance(digest, str) or not digest.startswith(_DIGEST_PREFIX):
                record_problems.append("missing or malformed digest")
            elif (
                _record_digest(
                    {key: value for key, value in record.items() if key != "digest"}
                )
                != digest
            ):
                record_problems.append("payload checksum mismatch")

            if record.get("prev_digest") != prev_digest:
                record_problems.append(
                    "broken hash chain: prev_digest "
                    f"{record.get('prev_digest')!r} doesn't match predecessor "
                    f"digest {prev_digest!r}"
                )

            if not isinstance(record.get("base_etag"), (str, type(None))):
                record_problems.append("base_etag must be a string or null")
            if not isinstance(record.get("created_at"), str):
                record_problems.append("created_at must be a string")

            operations = record.get("operations")
            if not isinstance(operations, list):
                record_problems.append("operations must be a list")
            else:
                seen_hrefs = set()
                for op_index, op in enumerate(operations):
                    location = f"operation {op_index}"
                    if not isinstance(op, dict):
                        record_problems.append(f"{location} is not an object")
                        continue

                    href = op.get("href")
                    kind = op.get("op")
                    if not isinstance(href, str) or not href:
                        record_problems.append(f"{location} has an invalid href")
                    elif href in seen_hrefs:
                        record_problems.append(f"{location} repeats href {href!r}")
                    else:
                        seen_hrefs.add(href)

                    if kind not in _KNOWN_OPS:
                        record_problems.append(
                            f"{location} has invalid operation {kind!r}"
                        )
                        continue

                    raw_item = op.get("item")
                    op_hash = op.get("hash")
                    if kind == _OP_DELETE:
                        if raw_item is not None or op_hash is not None:
                            record_problems.append(
                                f"{location} is a delete but carries item data"
                            )
                        continue

                    if not isinstance(raw_item, str) or not isinstance(op_hash, str):
                        record_problems.append(f"{location} is missing item data")
                        continue

                    try:
                        item = Item(raw_item)
                        actual_hash = hash_item(raw_item)
                    except (AssertionError, ValueError) as e:
                        record_problems.append(
                            f"{location} contains an invalid item: {e}"
                        )
                        continue

                    if actual_hash != op_hash:
                        record_problems.append(f"{location} item checksum mismatch")
                    if isinstance(href, str) and item.ident != href:
                        record_problems.append(
                            f"{location} href doesn't match the item's ident"
                        )

            if record_problems:
                problems.extend(f"Record {name}: {p}" for p in record_problems)
            else:
                records.append(record)

            prev_digest = digest if isinstance(digest, str) else None

        if problems:
            raise exceptions.PendingChangesError(
                f"The pending change journal for {self.storage_path!r} is "
                "corrupt and can't be replayed.",
                path=self.storage_path,
                problems=problems,
            )

        return records

    def append(self, operations, base_etag):
        """Persist a new record atomically.

        The existing journal is verified first, so a corrupt journal can
        never be extended silently.
        """
        records = self.load()
        sequence = len(records) + 1
        prev_digest = records[-1]["digest"] if records else None
        record = _build_pending_record(sequence, base_etag, prev_digest, operations)

        os.makedirs(self.dir, exist_ok=True)
        destination = os.path.join(self.dir, _RECORD_FILENAME_TEMPLATE.format(sequence))
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=self.dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, destination)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

        return record

    def clear(self):
        """Remove all records and the sidecar directory once empty.

        Leftover temp files from a crashed append are removed as well.
        """
        for name in os.listdir(self.dir):
            if _RECORD_FILENAME_RE.match(name) or name.startswith(".tmp-"):
                os.unlink(os.path.join(self.dir, name))
        with contextlib.suppress(OSError):
            os.rmdir(self.dir)


class SingleFileStorage(Storage):
    storage_name = "singlefile"
    _repr_attributes = ("path",)

    _write_mode = "wb"
    _append_mode = "ab"
    _read_mode = "rb"

    _items = None
    _base_items = None
    _replayed_items = None
    _last_etag = None
    _pending_records = None

    def __init__(self, path, encoding="utf-8", **kwargs):
        super().__init__(**kwargs)
        path = os.path.abspath(expand_path(path))
        checkfile(path, create=False)

        self.path = path
        self.encoding = encoding
        self._at_once = False

    @classmethod
    async def discover(cls, path, **kwargs):
        if kwargs.pop("collection", None) is not None:
            raise TypeError("collection argument must not be given.")

        path = os.path.abspath(expand_path(path))
        try:
            path_glob = path % "*"
        except TypeError:
            # If not exactly one '%s' is present, we cannot discover
            # collections because we wouldn't know which name to assign.
            raise NotImplementedError

        placeholder_pos = path.index("%s")

        for subpath in glob.iglob(path_glob):
            if os.path.isfile(subpath):
                args = dict(kwargs)
                args["path"] = subpath

                collection_end = (
                    placeholder_pos + 2 + len(subpath) - len(path)  # length of '%s'
                )
                collection = subpath[placeholder_pos:collection_end]
                args["collection"] = collection

                yield args

    @classmethod
    async def create_collection(cls, collection, **kwargs):
        path = os.path.abspath(expand_path(kwargs["path"]))

        if collection is not None:
            try:
                path = path % (collection,)
            except TypeError:
                raise ValueError(
                    "Exactly one %s required in path if collection is not null."
                )

        checkfile(path, create=True)
        kwargs["path"] = path
        kwargs["collection"] = collection
        return kwargs

    async def list(self):
        items = collections.OrderedDict()

        try:
            self._last_etag = get_etag_from_file(self.path)
            with open(self.path, self._read_mode) as f:
                text = f.read().decode(self.encoding)
        except OSError as e:
            import errno

            if e.errno != errno.ENOENT:  # file not found
                raise OSError(e)
            text = None

        if text:
            for raw_item in split_collection(text):
                item = Item(raw_item)
                items[item.ident] = item, item.hash

        # Snapshot of the confirmed file content.
        base_items = collections.OrderedDict(items)

        # Replay changes left behind by earlier interrupted syncs. This never
        # touches the file itself; a corrupt or inconsistent journal aborts the
        # whole session before any write can happen.
        journal = _PendingJournal(self.path)
        records = journal.load() if journal.exists() else []
        if records:
            problems = _check_record_consistency(records)
            if problems:
                raise exceptions.PendingChangesError(
                    f"Pending change records for {self.path!r} disagree; "
                    "refusing to merge the whole batch.",
                    path=self.path,
                    problems=problems,
                )
            logger.info(
                "Replaying %d pending change record(s) for %r",
                len(records),
                self.path,
            )
            _apply_pending_operations(items, records, self.path)

        # Snapshot after replay: diffing this against the session's target
        # state yields only this session's own changes, so a newly appended
        # record never duplicates what older records already describe.
        replayed_items = collections.OrderedDict(items)

        self._items = items
        self._base_items = base_items
        self._replayed_items = replayed_items
        self._pending_records = records

        for href, (_item, etag) in self._items.items():
            yield href, etag

    async def get(self, href) -> tuple[Item, str]:
        if self._items is None or not self._at_once:
            async for _ in self.list():
                pass

        assert self._items is not None  # type assertion
        try:
            return self._items[href]
        except KeyError:
            raise exceptions.NotFoundError(href)

    async def get_multi(self, hrefs: Iterable[str]):
        async with self.at_once():
            for href in uniq(hrefs):
                item, etag = await self.get(href)
                yield href, item, etag

    @_writing_op
    async def upload(self, item):
        href = item.ident
        if href in self._items:
            raise exceptions.AlreadyExistingError(existing_href=href)

        self._items[href] = item, item.hash
        return href, item.hash

    @_writing_op
    async def update(self, href, item, etag):
        if href not in self._items:
            raise exceptions.NotFoundError(href)

        _, actual_etag = self._items[href]
        if etag != actual_etag:
            raise exceptions.WrongEtagError(etag, actual_etag)

        self._items[href] = item, item.hash
        return item.hash

    @_writing_op
    async def delete(self, href, etag):
        if href not in self._items:
            raise exceptions.NotFoundError(href)

        _, actual_etag = self._items[href]
        if etag != actual_etag:
            raise exceptions.WrongEtagError(etag, actual_etag)

        del self._items[href]

    def _write(self):
        try:
            journal = _PendingJournal(self.path)
            if self._last_etag is not None and self._last_etag != get_etag_from_file(
                self.path
            ):
                # The file changed externally after it was read. Instead of
                # discarding this session's changes, journal them as an ordered
                # record so the next run can replay them onto the latest
                # confirmed file. Diffing against the post-replay snapshot
                # ensures this record only contains this session's changes.
                operations = _diff_operations(self._replayed_items, self._items)
                if operations:
                    record = journal.append(operations, self._last_etag)
                    logger.warning(
                        "File %r was modified externally; saved %d pending "
                        "change(s) as record %d for replay on the next run.",
                        self.path,
                        len(operations),
                        record["sequence"],
                    )
                raise exceptions.PreconditionFailed(
                    f"Some other program modified the file {self.path!r}. Re-run the "
                    "synchronization and make sure absolutely no other program is "
                    "writing into the same file."
                )
            text = join_collection(item.raw for item, etag in self._items.values())
            with atomic_write(self.path, mode="wb", overwrite=True) as f:
                f.write(text.encode(self.encoding))

            if journal.exists():
                # The main file now contains every replayed change. If this
                # cleanup fails (e.g. a crash), the next run replays the same
                # records as no-ops and retries the cleanup.
                try:
                    journal.clear()
                except OSError as e:
                    logger.warning(
                        "Could not remove pending change journal for %r after "
                        "a successful write: %s",
                        self.path,
                        e,
                    )
        finally:
            self._items = None
            self._base_items = None
            self._replayed_items = None
            self._last_etag = None
            self._pending_records = None

    @contextlib.asynccontextmanager
    async def at_once(self):
        async for _ in self.list():
            pass
        self._at_once = True
        try:
            yield self
            self._write()
        finally:
            self._at_once = False
