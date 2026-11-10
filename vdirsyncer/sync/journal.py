"""
A recoverable write-ahead journal for synchronization actions.

Every mutating storage operation (``upload``, ``update``, ``delete``) is
recorded in this journal *before* the request is sent to the storage. Each
record is committed in its own short database transaction, independently of
the (single, long-running) status transaction, so it survives process
termination even when the status changes are rolled back.

On the next sync run, the journal entries are compared against the actual
state of the storages before any new write happens:

- An action whose effect is already visible remotely is only reflected in the
  sync status, the storage itself is not written to again.
- An action that provably never reached the storage is executed again.
- An action that has become definitively invalid is abandoned safely.
- Any situation that cannot be decided (damaged journal or several plausible
  remote results for the same UID) aborts the whole run without deleting
  anything.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from dataclasses import dataclass

from .exceptions import AmbiguousRemoteResult
from .exceptions import JournalCorrupted

SCHEMA_VERSION = 1

ACTION_UPLOAD = "upload"
ACTION_UPDATE = "update"
ACTION_DELETE = "delete"
ACTIONS = (ACTION_UPLOAD, ACTION_UPDATE, ACTION_DELETE)

SIDES = ("a", "b")

STATE_PENDING = "pending"
STATE_DONE = "done"
STATE_ABANDONED = "abandoned"
STATES = (STATE_PENDING, STATE_DONE, STATE_ABANDONED)


@dataclass(frozen=True)
class JournalEntry:
    """One recoverable action record."""

    seq: int
    ident: str
    action: str
    side: str
    item_hash: str | None
    href: str | None
    etag: str | None
    state: str
    result_href: str | None
    result_etag: str | None
    reason: str | None

    @property
    def fingerprint(self) -> tuple:
        """The parts that uniquely identify the intended write."""
        return (
            self.ident,
            self.action,
            self.side,
            self.item_hash,
            self.href,
            self.etag,
        )


class ActionJournal:
    """SQLite-backed write-ahead log of sync actions.

    The journal lives in its own database file next to the status database so
    that committing an entry never takes part in the status transaction's
    rollback.
    """

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.closed = False

        try:
            must_create = self._needs_creation(path)
            self._c = sqlite3.connect(path)
            self._c.isolation_level = None
            self._c.row_factory = sqlite3.Row
            # Every journal commit has to survive a power loss, not just a
            # Python exception.
            self._c.execute("PRAGMA synchronous=FULL")

            if must_create:
                self._create_schema()
            else:
                self._verify_schema()
                self._verify_rows()
        except JournalCorrupted:
            raise
        except sqlite3.DatabaseError as e:
            raise JournalCorrupted(
                path=path, reason=f"journal database is unreadable: {e}"
            )

    @staticmethod
    def _needs_creation(path: str) -> bool:
        if path == ":memory:":
            return True
        if not os.path.exists(path):
            return True
        # A zero-byte file can only be the remnant of a crashed creation.
        if os.path.getsize(path) == 0:
            raise JournalCorrupted(path=path, reason="journal file is empty")
        return False

    def _create_schema(self) -> None:
        with self._tx():
            self._c.execute(
                'CREATE TABLE journal_meta ( "version" INTEGER PRIMARY KEY )'
            )
            self._c.execute(
                "INSERT INTO journal_meta (version) VALUES (?)", (SCHEMA_VERSION,)
            )
            self._c.execute(
                """CREATE TABLE action_journal (
                "seq" INTEGER PRIMARY KEY AUTOINCREMENT,
                "ident" TEXT NOT NULL,
                "action" TEXT NOT NULL,
                "side" TEXT NOT NULL,
                "item_hash" TEXT,
                "href" TEXT,
                "etag" TEXT,
                "state" TEXT NOT NULL,
                "result_href" TEXT,
                "result_etag" TEXT,
                "reason" TEXT
            ); """
            )
        if self.path != ":memory:":
            os.chmod(self.path, 0o600)

    def _verify_schema(self) -> None:
        integrity = self._c.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise JournalCorrupted(path=self.path, reason="integrity check failed")

        try:
            version = self._c.execute("SELECT version FROM journal_meta").fetchall()
        except sqlite3.DatabaseError as e:
            raise JournalCorrupted(
                path=self.path, reason=f"journal schema is missing: {e}"
            )
        if [row[0] for row in version] != [SCHEMA_VERSION]:
            raise JournalCorrupted(
                path=self.path,
                reason=f"unsupported journal schema version: {version}",
            )

        columns = {
            row["name"] for row in self._c.execute("PRAGMA table_info(action_journal)")
        }
        expected = {
            "seq",
            "ident",
            "action",
            "side",
            "item_hash",
            "href",
            "etag",
            "state",
            "result_href",
            "result_etag",
            "reason",
        }
        if not expected.issubset(columns):
            raise JournalCorrupted(
                path=self.path,
                reason=f"missing journal columns: {sorted(expected - columns)}",
            )

    def _verify_rows(self) -> None:
        """Validate every persisted record and detect undecidable duplicates."""
        entries = self._fetch("SELECT * FROM action_journal ORDER BY seq")

        live_by_target: dict[tuple[str, str], list[JournalEntry]] = {}
        for entry in entries:
            self._validate_entry(entry)
            if entry.state != STATE_ABANDONED:
                live_by_target.setdefault((entry.ident, entry.side), []).append(entry)

        for (ident, side), group in live_by_target.items():
            if len(group) == 1:
                continue
            fingerprints = {entry.fingerprint for entry in group}
            if len(fingerprints) == 1:
                raise JournalCorrupted(
                    path=self.path,
                    reason=(
                        f"duplicate journal records for item {ident!r} on side {side!r}"
                    ),
                )
            raise AmbiguousRemoteResult(
                ident=ident,
                side=side,
                reason=(
                    "several conflicting journaled actions exist for the "
                    "same item ident"
                ),
            )

    @staticmethod
    def _validate_entry(entry: JournalEntry) -> None:
        if not entry.ident or not isinstance(entry.ident, str):
            raise JournalCorrupted(reason="journal record has an empty ident")
        if entry.action not in ACTIONS:
            raise JournalCorrupted(reason=f"unknown journal action: {entry.action!r}")
        if entry.side not in SIDES:
            raise JournalCorrupted(reason=f"unknown journal side: {entry.side!r}")
        if entry.state not in STATES:
            raise JournalCorrupted(reason=f"unknown journal state: {entry.state!r}")

        if entry.action in (ACTION_UPLOAD, ACTION_UPDATE) and not entry.item_hash:
            raise JournalCorrupted(
                reason=(
                    f"journaled {entry.action} for {entry.ident!r} has no content hash"
                )
            )
        if entry.action in (ACTION_UPDATE, ACTION_DELETE) and not entry.href:
            raise JournalCorrupted(
                reason=(f"journaled {entry.action} for {entry.ident!r} has no href")
            )
        if entry.state == STATE_DONE and not entry.result_href:
            raise JournalCorrupted(
                reason=(
                    f"journaled {entry.action} for {entry.ident!r} is marked "
                    "done without a remote result"
                )
            )

    @contextlib.contextmanager
    def _tx(self):
        try:
            self._c.execute("BEGIN IMMEDIATE TRANSACTION")
            yield
            self._c.execute("COMMIT")
        except BaseException:
            self._c.execute("ROLLBACK")
            raise

    def _fetch(self, query: str, params=()) -> list[JournalEntry]:
        rows = self._c.execute(query, params).fetchall()
        return [
            JournalEntry(
                seq=row["seq"],
                ident=row["ident"],
                action=row["action"],
                side=row["side"],
                item_hash=row["item_hash"],
                href=row["href"],
                etag=row["etag"],
                state=row["state"],
                result_href=row["result_href"],
                result_etag=row["result_etag"],
                reason=row["reason"],
            )
            for row in rows
        ]

    def has_entries(self) -> bool:
        return bool(
            self._c.execute(
                "SELECT 1 FROM action_journal WHERE state != ? LIMIT 1",
                (STATE_ABANDONED,),
            ).fetchone()
        )

    def iter_live(self) -> list[JournalEntry]:
        """All records that still describe actionable state, ordered by age."""
        return self._fetch(
            "SELECT * FROM action_journal WHERE state != ? ORDER BY seq",
            (STATE_ABANDONED,),
        )

    def find_live(self, ident: str, side: str) -> JournalEntry | None:
        entries = self._fetch(
            "SELECT * FROM action_journal WHERE ident = ? AND side = ? AND state != ?",
            (ident, side, STATE_ABANDONED),
        )
        if not entries:
            return None
        # Uniqueness is enforced by _verify_rows and before_write.
        assert len(entries) == 1
        return entries[0]

    def before_write(
        self,
        *,
        ident: str,
        action: str,
        side: str,
        item_hash: str | None,
        href: str | None,
        etag: str | None,
    ) -> int:
        """Record the intent to write, returning the record's sequence number.

        A still-live record with the exact same fingerprint is reused, so a
        retried action never gets a duplicate entry. A live record with a
        different fingerprint is abandoned first: the reconciliation at the
        start of the run already proved that pending records never reached
        the storage, while done records were already verified against it.
        """
        assert action in ACTIONS
        assert side in SIDES

        with self._tx():
            existing = self.find_live(ident, side)
            if existing is not None:
                same_fingerprint = (
                    existing.action == action
                    and existing.item_hash == item_hash
                    and existing.href == href
                    and existing.etag == etag
                )
                if same_fingerprint:
                    return existing.seq
                self._set_state(
                    existing.seq,
                    STATE_ABANDONED,
                    reason="superseded by a newer sync action",
                )

            cur = self._c.execute(
                """INSERT INTO action_journal (
                    ident, action, side, item_hash, href, etag, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    ident,
                    action,
                    side,
                    item_hash,
                    href,
                    etag,
                    STATE_PENDING,
                ),
            )
            assert cur.lastrowid is not None
            return cur.lastrowid

    def mark_done(self, seq: int, result_href: str, result_etag: str | None) -> None:
        assert result_href is not None
        with self._tx():
            cur = self._c.execute(
                "UPDATE action_journal SET state = ?, result_href = ?, "
                "result_etag = ?, reason = NULL WHERE seq = ?",
                (STATE_DONE, result_href, result_etag, seq),
            )
            assert cur.rowcount == 1

    def mark_abandoned(self, seq: int, reason: str) -> None:
        with self._tx():
            self._set_state(seq, STATE_ABANDONED, reason=reason)

    def reset_to_pending(self, seq: int, reason: str) -> None:
        """Move a ``done`` record back to pending when the storage contradicts
        its recorded result."""
        with self._tx():
            cur = self._c.execute(
                "UPDATE action_journal SET state = ?, result_href = NULL, "
                "result_etag = NULL, reason = ? WHERE seq = ?",
                (STATE_PENDING, reason, seq),
            )
            assert cur.rowcount == 1

    def _set_state(self, seq: int, state: str, reason: str | None) -> None:
        cur = self._c.execute(
            "UPDATE action_journal SET state = ?, reason = ? WHERE seq = ?",
            (state, reason, seq),
        )
        assert cur.rowcount == 1

    def finalize(self) -> None:
        """Forget all records of a cleanly finished sync run.

        Pending records (only possible when storage errors were swallowed by
        an error callback) are kept on disk for the next run.
        """
        with self._tx():
            pending = self._c.execute(
                "SELECT 1 FROM action_journal WHERE state = ? LIMIT 1",
                (STATE_PENDING,),
            ).fetchone()
            if pending is None:
                self._c.execute("DELETE FROM action_journal")

    def close(self) -> None:
        if not self.closed:
            self._c.close()
            self.closed = True

    def __enter__(self) -> ActionJournal:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
