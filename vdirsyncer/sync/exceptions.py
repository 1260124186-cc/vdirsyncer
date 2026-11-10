from __future__ import annotations

from vdirsyncer import exceptions


class SyncError(exceptions.Error):
    """Errors related to synchronization."""


class SyncConflict(SyncError):
    """
    Two items changed since the last sync, they now have different contents and
    no conflict resolution method was given.

    :param ident: The ident of the item.
    :param href_a: The item's href on side A.
    :param href_b: The item's href on side B.
    """

    ident = None
    href_a = None
    href_b = None


class IdentConflict(SyncError):
    """
    Multiple items on the same storage have the same UID.

    :param storage: The affected storage.
    :param hrefs: List of affected hrefs on `storage`.
    """

    storage = None
    _hrefs = None

    @property
    def hrefs(self):
        return self._hrefs

    @hrefs.setter
    def hrefs(self, val):
        new_val = set(val)
        assert len(new_val) > 1, val
        self._hrefs = new_val


class StorageEmpty(SyncError):
    """
    One storage unexpectedly got completely empty between two synchronizations.
    The first argument is the empty storage.

    :param empty_storage: The empty
        :py:class:`vdirsyncer.storage.base.Storage`.
    """

    empty_storage = None


class BothReadOnly(SyncError):
    """
    Both storages are marked as read-only. Synchronization is therefore not
    possible.
    """


class PartialSync(SyncError):
    """
    Attempted change on read-only storage.
    """

    storage = None


class IdentAlreadyExists(SyncError):
    """Like IdentConflict, but for internal state. If this bubbles up, we don't
    have a data race, but a bug."""

    old_href = None
    new_href = None

    def to_ident_conflict(self, storage):
        return IdentConflict(storage=storage, hrefs=[self.old_href, self.new_href])


class JournalError(SyncError):
    """Baseclass for errors related to the recoverable action journal."""


class JournalCorrupted(JournalError):
    """
    The action journal (the write-ahead log of pending synchronization
    actions) is corrupted or unreadable and cannot be interpreted safely.

    The synchronization is aborted before any storage is written to, no data
    is deleted.

    :param path: The path of the corrupted journal file, if any.
    :param reason: Human-readable description of the corruption.
    """

    path = None
    reason = None


class AmbiguousRemoteResult(JournalError):
    """
    A journaled action for an item ident cannot be mapped to a single outcome
    on the remote storage, for example because the remote now contains an item
    with the same UID but different content.

    The synchronization is aborted before any further storage is written to.
    Existing data, including items that were added manually, is never modified
    or deleted.

    :param ident: The ident (usually the UID) of the affected item.
    :param side: The storage side (``"a"`` or ``"b"``) on which the ambiguity
        was detected.
    :param reason: Human-readable description of what could not be decided.
    """

    ident = None
    side = None
    reason = None
