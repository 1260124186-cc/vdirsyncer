"""Persistent checkpoint for a single ``vdirsyncer sync`` invocation.

The sync command discovers all collections it is supposed to synchronize and
first writes the complete *plan* (the collection identities and their storage
mappings) to a run record. Every time a collection finishes, its explicit
result (``success`` or ``failed``) is appended to that record.

If a run is interrupted, the next invocation can resume it: collections that
were already synchronized successfully are skipped, while pending and failed
ones are retried. The record is only reused while its plan fingerprint still
matches the current configuration, so newly added targets or changed storage
mappings always start a fresh run. A record that is corrupted, or whose
collection identities are inconsistent, aborts the whole run without touching
the per-collection sync statuses already on disk.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING
from typing import Any

from vdirsyncer import exceptions
from vdirsyncer.utils import atomic_write

from .utils import STATUS_PERMISSIONS
from .utils import assert_permissions
from .utils import get_status_name
from .utils import prepare_status_path

if TYPE_CHECKING:
    from .config import CollectionConfig

#: Name of the run record inside the status directory. The leading dot makes
#: sure it can never collide with a pair name (pair names are alphanumeric).
RUN_RECORD_FILENAME = ".sync_run.json"

#: Bump when the on-disk format changes in an incompatible way.
RUN_RECORD_VERSION = 1

STATE_RUNNING = "running"
STATE_COMPLETE = "complete"
_STATES = frozenset((STATE_RUNNING, STATE_COMPLETE))

RESULT_SUCCESS = "success"
RESULT_FAILED = "failed"
_RESULTS = frozenset((RESULT_SUCCESS, RESULT_FAILED))

_PLAN_FIELDS = ("pair", "collection", "config_a", "config_b")


class RunRecordError(exceptions.UserError):
    """The run record is corrupted or internally inconsistent.

    Raising this error aborts the whole sync run; existing per-collection
    status files are deliberately left untouched.
    """


def get_run_record_path(status_path: str) -> str:
    return os.path.join(status_path, RUN_RECORD_FILENAME)


def make_plan_entry(collection: CollectionConfig) -> dict[str, Any]:
    """Build one fingerprintable plan entry for a collection."""
    return {
        "pair": collection.pair.name,
        "collection": collection.name,
        "config_a": collection.config_a,
        "config_b": collection.config_b,
        "result": None,
    }


def entry_identity(entry: dict[str, Any]) -> str:
    return get_status_name(entry["pair"], entry["collection"])


def _plan_payload(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {field: entry[field] for field in _PLAN_FIELDS}
        for entry in sorted(entries, key=entry_identity)
    ]


def compute_fingerprint(entries: list[dict[str, Any]]) -> str:
    """Stable hash over the full collection plan.

    It covers the collection identities and the storage mapping of each
    collection, so adding a target or changing a storage config invalidates
    the fingerprint. Per-collection run results are intentionally excluded.
    """
    payload = json.dumps(_plan_payload(entries), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_new_record(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": RUN_RECORD_VERSION,
        "state": STATE_RUNNING,
        "fingerprint": compute_fingerprint(entries),
        "collections": [dict(entry) for entry in entries],
    }


def save_run_record(path: str, record: dict[str, Any]) -> None:
    prepare_status_path(path)
    with atomic_write(path, mode="w", overwrite=True) as f:
        json.dump(record, f, indent=2)
    os.chmod(path, STATUS_PERMISSIONS)


def load_run_record(path: str) -> dict[str, Any]:
    """Load and strictly validate a run record.

    Any structural problem, unknown format or identity inconsistency raises
    :class:`RunRecordError` so the caller can stop the run instead of making
    assumptions about its contents.
    """
    assert_permissions(path, STATUS_PERMISSIONS)
    try:
        with open(path) as f:
            record = json.load(f)
    except (OSError, ValueError) as e:
        raise RunRecordError(
            f"Unable to read sync run record {path}: {e}. "
            "Fix or remove the file before syncing again; existing "
            "per-collection statuses are left untouched."
        )

    _validate_record(record, path)
    return record


def _validate_record(record: Any, path: str) -> None:
    def fail(reason: str) -> None:
        raise RunRecordError(
            f"Corrupt sync run record {path}: {reason}. Fix or remove the "
            "file before syncing again; existing per-collection statuses "
            "are left untouched."
        )

    if not isinstance(record, dict):
        fail("top-level value is not an object")
    if record.get("version") != RUN_RECORD_VERSION:
        fail(f"unsupported version {record.get('version')!r}")
    if record.get("state") not in _STATES:
        fail(f"invalid state {record.get('state')!r}")
    if not isinstance(record.get("fingerprint"), str):
        fail("missing or invalid fingerprint")

    entries = record.get("collections")
    if not isinstance(entries, list):
        fail("collections plan is not a list")

    seen_identities: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            fail(f"collection entry {index} is not an object")
        if not isinstance(entry.get("pair"), str) or not entry["pair"]:
            fail(f"collection entry {index} has an invalid pair name")
        if entry.get("collection") is not None and not isinstance(
            entry.get("collection"), str
        ):
            fail(f"collection entry {index} has an invalid collection name")
        if not isinstance(entry.get("config_a"), dict) or not isinstance(
            entry.get("config_b"), dict
        ):
            fail(f"collection entry {index} has an invalid storage mapping")
        if entry.get("result") not in (None, *_RESULTS):
            fail(f"collection entry {index} has an invalid result")

        identity = entry_identity(entry)
        if identity in seen_identities:
            fail(f"duplicate collection identity {identity!r}")
        seen_identities.add(identity)

    # The fingerprint binds the stored plan to its contents. A mismatch means
    # the record was tampered with or partially rewritten, i.e. the stored
    # collection identities/mappings are not trustworthy.
    try:
        expected_fingerprint = compute_fingerprint(entries)
    except (KeyError, TypeError) as e:
        fail(f"malformed collection entry: {e}")
    if expected_fingerprint != record["fingerprint"]:
        fail("plan fingerprint does not match its contents")


class SyncRun:
    """Owns the lifecycle of one sync run record.

    Use :meth:`open` at startup to either resume an interrupted run or start
    a fresh one, :meth:`should_sync` to decide whether a collection still
    needs work, :meth:`mark_result` after each collection and :meth:`finish`
    once all tasks have settled.
    """

    def __init__(
        self,
        path: str,
        entries: list[dict[str, Any]],
        record: dict[str, Any],
        *,
        resumed: bool,
    ) -> None:
        self.path = path
        self.entries = entries
        self.record = record
        self.resumed = resumed
        self._entries_by_identity = {
            entry_identity(entry): entry for entry in record["collections"]
        }

    @classmethod
    def open(cls, status_path: str, entries: list[dict[str, Any]]) -> SyncRun:
        """Open an existing run record or create a new one.

        * A corrupt or inconsistent record aborts with
          :class:`RunRecordError`.
        * A running record whose plan matches is resumed: successful
          collections are skipped on this run.
        * A completed record, or a running record whose plan changed (new
          targets or changed storage mappings), is replaced by a fresh plan.
        """
        path = get_run_record_path(status_path)
        if os.path.exists(path):
            record = load_run_record(path)
            plan_unchanged = record["fingerprint"] == compute_fingerprint(entries)
            if record["state"] == STATE_RUNNING and plan_unchanged:
                return cls(path, entries, record, resumed=True)

        record = build_new_record(entries)
        run = cls(path, entries, record, resumed=False)
        run._save()
        return run

    def should_sync(self, identity: str) -> bool:
        """Return ``False`` only for collections already synced successfully."""
        entry = self._entries_by_identity.get(identity)
        if entry is None:
            # Fingerprint equality guarantees this cannot happen for a valid
            # record, so reaching here means the record is inconsistent.
            raise RunRecordError(
                f"Corrupt sync run record {self.path}: collection "
                f"{identity!r} is missing from the plan. Existing "
                "per-collection statuses are left untouched."
            )
        return entry["result"] != RESULT_SUCCESS

    def mark_result(self, identity: str, success: bool) -> None:
        """Persist the explicit result of one finished collection."""
        entry = self._entries_by_identity[identity]
        entry["result"] = RESULT_SUCCESS if success else RESULT_FAILED
        self._save()

    def finish(self) -> None:
        """Converge the record to ``complete`` once every entry is settled."""
        if all(entry["result"] is not None for entry in self.record["collections"]):
            self.record["state"] = STATE_COMPLETE
        self._save()

    def has_failure(self) -> bool:
        return any(
            entry["result"] == RESULT_FAILED for entry in self.record["collections"]
        )

    def _save(self) -> None:
        save_run_record(self.path, self.record)
