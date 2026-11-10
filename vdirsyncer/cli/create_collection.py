"""Recoverable, two-sided collection creation.

Creating a collection normally touches two independent storages (the ``a`` and
``b`` side of a pair). If the first side is created and the second one fails,
a plain, non-persisted creation attempt leaves an empty orphan collection
behind and may create duplicates or confuse synchronization on the next run.

The :class:`CollectionCreationCoordinator` writes a *creation plan* to the
status directory before touching any storage. The plan records, for each side,
which remote collection is expected and whether vdirsyncer created it during
the current, possibly interrupted creation. That information makes creation
resumable and allows rolling back only empty collections that vdirsyncer
created itself -- pre-existing collections are never deleted.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os

import aiostream

from vdirsyncer import exceptions

from .utils import get_status_path
from .utils import save_status
from .utils import storage_instance_from_config

logger = logging.getLogger(__name__)

# Bump when the on-disk plan format changes incompatibly.
CREATE_PLAN_VERSION = 1
SIDES = ("a", "b")


def _plan_path(status_path, pair_name, collection):
    return get_status_path(
        status_path, pair_name, collection, data_type="create_plan"
    )


def new_create_plan(collection, collection_a, collection_b):
    """Build a fresh plan for one configured collection name."""
    return {
        "version": CREATE_PLAN_VERSION,
        "collection": collection,
        "a": {"collection": collection_a, "created": False, "args": None},
        "b": {"collection": collection_b, "created": False, "args": None},
    }


def _corrupt(pair_name, collection, path, reason):
    raise exceptions.UserError(
        f"Creation plan for collection {json.dumps(collection)} of pair "
        f"{pair_name!r} is corrupt ({reason}). Refusing to create or delete "
        f"anything; inspect or remove the plan file {path} manually."
    )


def parse_create_plan(
    raw,
    *,
    status_path,
    pair_name,
    collection,
    collection_a,
    collection_b,
):
    """Validate a plan loaded from disk and check it against the config.

    Structural problems raise a UserError, and so does a plan whose recorded
    remote collection names no longer match what the current configuration
    resolves the same configured collection name to.
    """
    path = _plan_path(status_path, pair_name, collection)
    expected_remotes = {"a": collection_a, "b": collection_b}

    if not isinstance(raw, dict):
        _corrupt(pair_name, collection, path, "not a JSON object")

    if raw.get("version") != CREATE_PLAN_VERSION:
        _corrupt(pair_name, collection, path, "unsupported plan version")

    if raw.get("collection") != collection:
        _corrupt(pair_name, collection, path, "collection name mismatch")

    for side in SIDES:
        entry = raw.get(side)
        if not isinstance(entry, dict) or not isinstance(
            entry.get("created"), bool
        ):
            _corrupt(pair_name, collection, path, f"malformed {side!r} entry")
        if "collection" not in entry:
            _corrupt(pair_name, collection, path, f"malformed {side!r} entry")
        args = entry.get("args")
        if args is not None and not isinstance(args, dict):
            _corrupt(pair_name, collection, path, f"malformed {side!r} args")

        if entry["collection"] != expected_remotes[side]:
            raise exceptions.UserError(
                f"Creation plan for collection {json.dumps(collection)} of "
                f"pair {pair_name!r} records the {side}-side collection "
                f"{json.dumps(entry['collection'])}, but the current config "
                f"resolves it to {json.dumps(expected_remotes[side])}. "
                "Refusing to create or delete anything; reconcile the config "
                f"with the plan file {path} manually."
            )

    return raw


def load_create_plan(status_path, pair_name, collection):
    """Return the persisted plan, or ``None`` when no plan exists.

    A plan that cannot be parsed is reported as a UserError instead of being
    silently ignored.
    """
    path = _plan_path(status_path, pair_name, collection)
    if not os.path.exists(path):
        return None

    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        _corrupt(pair_name, collection, path, f"unreadable file: {e}")


def _persistable_args(args):
    """Strip runtime-only objects (e.g. connectors) before JSON persistence."""
    rv = dict(args)
    rv.pop("connector", None)
    return rv


class CollectionCreationCoordinator:
    """Coordinate creation of one collection across both storages.

    A single coordinator instance is used for all collections of a pair and
    keeps the creation flow resumable through on-disk plans.
    """

    def __init__(
        self, *, status_path, pair, connector, handle_collection_not_found
    ):
        self._status_path = status_path
        self._pair = pair
        self._connector = connector
        self._handle_collection_not_found = handle_collection_not_found

    async def ensure_collection(
        self,
        collection,
        collection_a,
        collection_b,
        get_a_discovered,
        get_b_discovered,
    ):
        """Make sure both sides of one collection exist and return their args.

        Only returns once both sides are available. Any terminal failure rolls
        back the empty collections vdirsyncer created before the error is
        re-raised.
        """
        plan = self._load_or_initialize(collection, collection_a, collection_b)

        getters = {"a": get_a_discovered, "b": get_b_discovered}
        remotes = {"a": collection_a, "b": collection_b}
        base_configs = {
            "a": self._pair.config_a,
            "b": self._pair.config_b,
        }

        try:
            args = {}
            for side in SIDES:
                args[side] = await self._ensure_side(
                    side,
                    plan,
                    remotes[side],
                    base_configs[side],
                    getters[side],
                )
        except exceptions.UserError as original:
            # This side cannot be made available. Only undo empty collections
            # this creation produced; pre-existing ones stay untouched.
            await self._rollback_created(plan, original)
            raise

        # Both sides exist: the collection is safe to enter into the usable
        # collections cache, so the recovery plan is no longer needed.
        self._delete_plan(collection)
        return args["a"], args["b"]

    def _load_or_initialize(self, collection, collection_a, collection_b):
        raw = load_create_plan(self._status_path, self._pair.name, collection)
        if raw is None:
            plan = new_create_plan(collection, collection_a, collection_b)
            self._write_plan(collection, plan)
            return plan

        return parse_create_plan(
            raw,
            status_path=self._status_path,
            pair_name=self._pair.name,
            collection=collection,
            collection_a=collection_a,
            collection_b=collection_b,
        )

    async def _ensure_side(self, side, plan, remote_name, base_config,
                           get_discovered):
        if remote_name is None:
            args = dict(base_config)
            args["collection"] = None
            return args

        entry = plan[side]

        discovered = await get_discovered()
        if remote_name in discovered:
            # The collection already exists. It was either there all along
            # (never mark as created) or produced by an interrupted earlier
            # run (plan keeps the ``created`` rollback marker).
            args = dict(discovered[remote_name])
            entry["args"] = _persistable_args(args)
            self._write_plan(plan["collection"], plan)
            return args

        if entry["created"]:
            logger.warning(
                "Plan claims the %s-side collection %r was already created, "
                "but it is gone; recreating it.",
                side,
                remote_name,
            )

        # Absent remotely: create it. A terminal failure raises UserError,
        # which the caller turns into a rollback; transient errors propagate
        # and leave the plan in place for the next run.
        args = await self._handle_collection_not_found(base_config, remote_name)
        entry["created"] = True
        entry["args"] = _persistable_args(args)
        self._write_plan(plan["collection"], plan)
        return args

    async def _rollback_created(self, plan, original):
        collection = plan["collection"]

        for side in SIDES:
            entry = plan[side]
            if not entry["created"]:
                continue

            remote = entry["collection"]
            args = entry.get("args")
            if remote is None or not isinstance(args, dict):
                continue

            try:
                storage = await storage_instance_from_config(
                    args, create=False, connector=self._connector
                )
            except exceptions.CollectionNotFound:
                # The collection was removed externally after we created it;
                # there is nothing left to roll back.
                logger.warning(
                    "Plan claims the %s-side collection %r was created, but "
                    "it no longer exists; marking it as removed.",
                    side,
                    remote,
                )
                entry["created"] = False
                entry["args"] = None
                self._write_plan(collection, plan)
                continue

            items = await aiostream.stream.list(storage.list())
            if items:
                raise exceptions.UserError(
                    f"The {side}-side collection {json.dumps(remote)} was "
                    "created by vdirsyncer, but it is no longer empty. "
                    "Refusing to delete it; please decide manually what to do "
                    f"with the collection on storage {args.get('instance_name')!r}."
                ) from original

            try:
                await storage.delete_collection()
            except NotImplementedError:
                raise exceptions.UserError(
                    f"Cannot automatically remove the empty {side}-side "
                    f"collection {json.dumps(remote)} on storage "
                    f"{args.get('instance_name')!r}: this storage does not "
                    "support deleting collections. Please delete it manually."
                ) from original

            entry["created"] = False
            entry["args"] = None
            self._write_plan(collection, plan)

        if not any(plan[side]["created"] for side in SIDES):
            self._delete_plan(collection)

    def _write_plan(self, collection, plan):
        save_status(
            base_path=self._status_path,
            pair=self._pair.name,
            data_type="create_plan",
            data=plan,
            collection=collection,
        )

    def _delete_plan(self, collection):
        path = _plan_path(self._status_path, self._pair.name, collection)
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
