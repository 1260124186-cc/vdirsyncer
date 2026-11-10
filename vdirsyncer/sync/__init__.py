"""
The `sync` function in `vdirsyncer.sync` can be called on two instances of
`Storage` to synchronize them. Apart from the defined errors, this is the only
public API of this module.

The algorithm is based on the blogpost "How OfflineIMAP works" by Edward Z.
Yang: http://blog.ezyang.com/2012/08/how-offlineimap-works/

Some modifications to it are explained in
https://unterwaditzer.net/2016/sync-algorithm.html
"""

from __future__ import annotations

import contextlib
import itertools
import logging

from vdirsyncer.exceptions import UserError
from vdirsyncer.storage.base import Storage
from vdirsyncer.utils import uniq
from vdirsyncer.vobject import Item

from .exceptions import AmbiguousRemoteResult
from .exceptions import BothReadOnly
from .exceptions import IdentAlreadyExists
from .exceptions import PartialSync
from .exceptions import StorageEmpty
from .exceptions import SyncConflict
from .journal import ACTION_DELETE
from .journal import ACTION_UPDATE
from .journal import ACTION_UPLOAD
from .journal import ActionJournal
from .status import ItemMetadata
from .status import SqliteStatus
from .status import SubStatus

sync_logger = logging.getLogger(__name__)


class _StorageInfo:
    """A wrapper class that holds prefetched items, the status and other
    things."""

    def __init__(self, storage: Storage, status: SubStatus):
        self.storage = storage
        self.status = status
        self._item_cache = {}  # type: ignore[var-annotated]

    async def prepare_new_status(self) -> bool:
        storage_nonempty = False
        prefetch = []

        def _store_props(ident: str, props: ItemMetadata) -> None:
            try:
                self.status.insert_ident(ident, props)
            except IdentAlreadyExists as e:
                raise e.to_ident_conflict(self.storage)

        async for href, etag in self.storage.list():  # type: ignore[attr-defined]
            storage_nonempty = True
            ident, meta = self.status.get_by_href(href)

            if meta is None or meta.href != href or meta.etag != etag:
                # Either the item is completely new, or updated
                # In both cases we should prefetch
                prefetch.append(href)
            else:
                # Metadata is completely identical
                _store_props(ident, meta)

        # Prefetch items
        if prefetch:
            async for href, item, etag in self.storage.get_multi(prefetch):
                _store_props(
                    item.ident,
                    ItemMetadata(href=href, hash=item.hash, etag=etag),
                )
                self.set_item_cache(item.ident, item)

        return storage_nonempty

    def is_changed(self, ident: str) -> bool:
        old_meta = self.status.get(ident)
        if old_meta is None:  # new item
            return True

        new_meta = self.status.get_new(ident)

        return (
            new_meta.etag != old_meta.etag  # etag changed
            # item actually changed
            and (old_meta.hash is None or new_meta.hash != old_meta.hash)
        )

    def set_item_cache(self, ident, item) -> None:
        actual_hash = self.status.get_new(ident).hash
        assert actual_hash == item.hash
        self._item_cache[ident] = item

    def get_item_cache(self, ident: str) -> Item:
        return self._item_cache[ident]


async def sync(
    storage_a: Storage,
    storage_b: Storage,
    status: SqliteStatus,
    conflict_resolution=None,
    force_delete=False,
    error_callback=None,
    partial_sync="revert",
) -> None:
    """Synchronizes two storages.

    :param storage_a: The first storage
    :param storage_b: The second storage
    :param status: {ident: (href_a, etag_a, href_b, etag_b)}
        metadata about the two storages for detection of changes. Will be
        modified by the function and should be passed to it at the next sync.
        If this is the first sync, an empty dictionary should be provided.
    :param conflict_resolution: A function that, given two conflicting item
        versions A and B, returns a new item with conflicts resolved. The UID
        must be the same. The strings `"a wins"` and `"b wins"` are also
        accepted to mean that side's version will always be taken. If none
        is provided, the sync function will raise :py:exc:`SyncConflict`.
    :param force_delete: When one storage got completely emptied between two
        syncs, :py:exc:`StorageEmpty` is raised for
        safety. Setting this parameter to ``True`` disables this safety
        measure.
    :param error_callback: Instead of raising errors when executing actions,
        call the given function with an `Exception` as the only argument.
    :param partial_sync: What to do when doing sync actions on read-only
        storages.

        - ``error``: Raise an error.
        - ``ignore``: Those actions are simply skipped.
        - ``revert`` (default): Revert changes on other side.
    """
    if storage_a.read_only and storage_b.read_only:
        raise BothReadOnly

    if conflict_resolution == "a wins":

        def conflict_resolution(a, b):
            return a
    elif conflict_resolution == "b wins":

        def conflict_resolution(a, b):
            return b

    status_nonempty = bool(next(status.iter_old(), None))
    journal = status.get_journal()

    with status.transaction():
        a_info = _StorageInfo(storage_a, SubStatus(status, "a"))
        b_info = _StorageInfo(storage_b, SubStatus(status, "b"))

        a_nonempty = await a_info.prepare_new_status()
        b_nonempty = await b_info.prepare_new_status()

        if status_nonempty and not force_delete:
            if a_nonempty and not b_nonempty:
                raise StorageEmpty(empty_storage=storage_b)
            elif not a_nonempty and b_nonempty:
                raise StorageEmpty(empty_storage=storage_a)

        # Recover actions left behind by an interrupted run before anything is
        # written to the storages. Raises on undecidable situations.
        if journal.has_entries():
            reconcile_journal(journal, a_info, b_info)

        actions = list(_get_actions(a_info, b_info))

        async with storage_a.at_once(), storage_b.at_once():
            for action in actions:
                try:
                    await action.run(
                        a_info, b_info, conflict_resolution, partial_sync, journal
                    )
                except Exception as e:
                    if error_callback:
                        error_callback(e)
                    else:
                        raise

    # The status transaction committed successfully. Pending records are only
    # kept when storage errors were swallowed by `error_callback`.
    journal.finalize()


class Action:
    async def _run_impl(self, a, b, journal):  # pragma: no cover
        raise NotImplementedError

    async def run(self, a, b, conflict_resolution, partial_sync, journal):
        with self.auto_rollback(a, b):
            if self.dest.storage.read_only:
                if partial_sync == "error":
                    raise PartialSync(self.dest.storage)
                elif partial_sync == "ignore":
                    self.rollback(a, b)
                    return
                else:
                    assert partial_sync == "revert"

            await self._run_impl(a, b, journal)

    @staticmethod
    def _side_of(dest, a, b) -> str:
        if dest is a:
            return "a"
        assert dest is b
        return "b"

    @contextlib.contextmanager
    def auto_rollback(self, a, b):
        try:
            yield
        except BaseException:
            self.rollback(a, b)
            raise

    def rollback(self, a, b):
        a.status.parent.rollback(self.ident)


class Upload(Action):
    def __init__(self, item, dest):
        self.item = item
        self.ident = item.ident
        self.dest = dest

    async def _run_impl(self, a, b, journal):
        if self.dest.storage.read_only:
            href = etag = None
        else:
            sync_logger.info(
                f"Copying (uploading) item {self.ident} to {self.dest.storage}"
            )
            side = self._side_of(self.dest, a, b)
            seq = journal.before_write(
                ident=self.ident,
                action=ACTION_UPLOAD,
                side=side,
                item_hash=self.item.hash,
                href=None,
                etag=None,
            )
            href, etag = await self.dest.storage.upload(self.item)
            assert href is not None
            journal.mark_done(seq, href, etag)

        self.dest.status.insert_ident(
            self.ident, ItemMetadata(href=href, hash=self.item.hash, etag=etag)
        )


class Update(Action):
    def __init__(self, item, dest):
        self.item = item
        self.ident = item.ident
        self.dest = dest

    async def _run_impl(self, a, b, journal):
        if self.dest.storage.read_only:
            meta = ItemMetadata(hash=self.item.hash)
        else:
            sync_logger.info(
                f"Copying (updating) item {self.ident} to {self.dest.storage}"
            )
            side = self._side_of(self.dest, a, b)
            meta = self.dest.status.get_new(self.ident)
            seq = journal.before_write(
                ident=self.ident,
                action=ACTION_UPDATE,
                side=side,
                item_hash=self.item.hash,
                href=meta.href,
                etag=meta.etag,
            )
            new_etag = await self.dest.storage.update(meta.href, self.item, meta.etag)
            journal.mark_done(seq, meta.href, new_etag)
            meta.etag = new_etag

        self.dest.status.update_ident(self.ident, meta)


class Delete(Action):
    def __init__(self, ident, dest):
        self.ident = ident
        self.dest = dest

    async def _run_impl(self, a, b, journal):
        meta = self.dest.status.get_new(self.ident)
        if self.dest.storage.read_only or self.dest.storage.no_delete:
            sync_logger.debug(
                f"Skipping deletion of item {self.ident} from {self.dest.storage}"
            )
        else:
            sync_logger.info(f"Deleting item {self.ident} from {self.dest.storage}")
            side = self._side_of(self.dest, a, b)
            seq = journal.before_write(
                ident=self.ident,
                action=ACTION_DELETE,
                side=side,
                item_hash=None,
                href=meta.href,
                etag=meta.etag,
            )
            await self.dest.storage.delete(meta.href, meta.etag)
            journal.mark_done(seq, meta.href, meta.etag)

        self.dest.status.remove_ident(self.ident)


class ResolveConflict(Action):
    def __init__(self, ident):
        self.ident = ident

    async def run(self, a, b, conflict_resolution, partial_sync, journal):
        with self.auto_rollback(a, b):
            sync_logger.info(f"Doing conflict resolution for item {self.ident}...")

            meta_a = a.status.get_new(self.ident)
            meta_b = b.status.get_new(self.ident)

            if meta_a.hash == meta_b.hash:
                sync_logger.info("...same content on both sides.")
            elif conflict_resolution is None:
                raise SyncConflict(
                    ident=self.ident, href_a=meta_a.href, href_b=meta_b.href
                )
            elif callable(conflict_resolution):
                item_a = a.get_item_cache(self.ident)
                item_b = b.get_item_cache(self.ident)
                new_item = conflict_resolution(item_a, item_b)
                if new_item.hash != meta_a.hash:
                    await Update(new_item, a).run(
                        a,
                        b,
                        conflict_resolution,
                        partial_sync,
                        journal,
                    )
                if new_item.hash != meta_b.hash:
                    await Update(new_item, b).run(
                        a,
                        b,
                        conflict_resolution,
                        partial_sync,
                        journal,
                    )
            else:
                raise UserError(
                    f"Invalid conflict resolution mode: {conflict_resolution!r}"
                )


def _get_actions(a_info: _StorageInfo, b_info: _StorageInfo):
    for ident in uniq(
        itertools.chain(
            a_info.status.parent.iter_new(), a_info.status.parent.iter_old()
        )
    ):
        a = a_info.status.get_new(ident)
        b = b_info.status.get_new(ident)

        if a and b:
            a_changed = a_info.is_changed(ident)
            b_changed = b_info.is_changed(ident)
            if a_changed and b_changed:
                # item was modified on both sides
                # OR: missing status
                yield ResolveConflict(ident)
            elif a_changed and not b_changed:
                # item was only modified in a
                yield Update(a_info.get_item_cache(ident), b_info)
            elif not a_changed and b_changed:
                # item was only modified in b
                yield Update(b_info.get_item_cache(ident), a_info)
        elif a and not b:
            if a_info.is_changed(ident):
                # was deleted from b but modified on a
                # OR: new item was created in a
                yield Upload(a_info.get_item_cache(ident), b_info)
            else:
                # was deleted from b and not modified on a
                yield Delete(ident, a_info)
        elif not a and b:
            if b_info.is_changed(ident):
                # was deleted from a but modified on b
                # OR: new item was created in b
                yield Upload(b_info.get_item_cache(ident), a_info)
            else:
                # was deleted from a and not changed on b
                yield Delete(ident, b_info)


def reconcile_journal(
    journal: ActionJournal, a_info: _StorageInfo, b_info: _StorageInfo
) -> None:
    """Reconcile journal records left behind by an interrupted run.

    Called after both storages were listed (so the "new" status reflects the
    actual remote state) and before any storage is written to. Every record is
    classified as confirmed (only the sync status is patched), still pending
    (left for the freshly computed action plan to retry) or safely abandoned.
    Anything that cannot be decided aborts the run with
    :class:`AmbiguousRemoteResult`; no data is ever deleted here.
    """
    infos = {"a": a_info, "b": b_info}
    sync_logger.info("Recovering interrupted sync from action journal")

    for entry in journal.iter_live():
        info = infos[entry.side]
        # The item currently visible on the target storage, if any.
        remote = info.status.get_new(entry.ident)

        if entry.action == ACTION_UPLOAD:
            _reconcile_upload(journal, entry, info, remote)
        elif entry.action == ACTION_UPDATE:
            _reconcile_update(journal, entry, info, remote)
        else:
            _reconcile_delete(journal, entry, remote)


def _reconcile_upload(journal, entry, info, remote) -> None:
    if remote is None:
        # The item never reached the storage, or was removed again afterwards.
        # In both cases there is nothing to deduplicate; retry the upload.
        if entry.state == "done":
            journal.reset_to_pending(
                entry.seq, "marked done, but item is absent from storage"
            )
        return

    if remote.hash != entry.item_hash:
        raise AmbiguousRemoteResult(
            ident=entry.ident,
            side=entry.side,
            reason=(
                "an item with the same ident already exists on the target "
                "storage, but its content does not match the journaled upload"
            ),
        )

    # The upload is confirmed remotely (the href may differ if the storage
    # moved it). Only patch the status, never write again.
    info.status.reconcile_ident(entry.ident, remote)
    if entry.state != "done":
        journal.mark_done(entry.seq, remote.href, remote.etag)


def _reconcile_update(journal, entry, info, remote) -> None:
    if remote is None:
        # The target item is gone, so this update can never apply. It is safe
        # to abandon; the freshly computed action plan decides what to do.
        journal.mark_abandoned(entry.seq, "target item no longer exists")
        return

    if remote.hash == entry.item_hash:
        # The intended content is already on the storage (href/etag may have
        # moved or changed on the server side).
        info.status.reconcile_ident(entry.ident, remote)
        if entry.state != "done":
            journal.mark_done(entry.seq, remote.href, remote.etag)
        return

    if remote.href == entry.href and remote.etag == entry.etag:
        # The preconditions are untouched: the update simply never ran. The
        # action plan will re-execute it.
        if entry.state == "done":
            journal.reset_to_pending(
                entry.seq, "marked done, but target content was not updated"
            )
        return

    raise AmbiguousRemoteResult(
        ident=entry.ident,
        side=entry.side,
        reason=(
            "the target item changed independently since the journaled "
            "update; it is unclear whether the update already took effect"
        ),
    )


def _reconcile_delete(journal, entry, remote) -> None:
    if remote is None:
        # Nothing is left to delete (the delete succeeded or the item was
        # removed otherwise).
        if entry.state != "done":
            journal.mark_done(entry.seq, entry.href, entry.etag)
        return

    if remote.href == entry.href and remote.etag == entry.etag:
        # The item is exactly as expected: the delete never ran. The action
        # plan will re-execute it.
        if entry.state == "done":
            journal.reset_to_pending(entry.seq, "marked done, but item still exists")
        return

    # The item changed or moved. Deleting it would destroy unfamiliar data,
    # so the stale journaled delete is abandoned and the plan re-evaluates.
    journal.mark_abandoned(
        entry.seq,
        "target item changed or moved since the journaled delete",
    )
