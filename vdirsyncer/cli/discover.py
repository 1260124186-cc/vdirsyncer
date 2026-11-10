from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sys

import aiohttp
import aiostream

from vdirsyncer import exceptions

from .utils import handle_collection_not_found
from .utils import handle_storage_init_error
from .utils import load_status
from .utils import save_status
from .utils import storage_class_from_config
from .utils import storage_instance_from_config

# Increase whenever upgrade potentially breaks discovery cache and collections
# should be re-discovered
DISCOVERY_CACHE_VERSION = 1

# Version of the persisted roster layout (per-entry origin/state metadata).
# Legacy layouts (see ``_normalize_cache_entry``) are still read and used as a
# baseline, so bumping this does not invalidate existing caches.
ROSTER_VERSION = 2

ORIGIN_MANUAL = "manual"
ORIGIN_DISCOVERED = "discovered"

STATE_ACTIVE = "active"
STATE_PENDING = "pending"

logger = logging.getLogger(__name__)


def _get_collections_cache_key(pair):
    m = hashlib.sha256()
    j = json.dumps(
        [
            DISCOVERY_CACHE_VERSION,
            pair.collections,
            pair.config_a,
            pair.config_b,
        ],
        sort_keys=True,
    )
    m.update(j.encode("utf-8"))
    return m.hexdigest()


async def collections_for_pair(
    status_path,
    pair,
    from_cache=True,
    list_collections=False,
    *,
    connector: aiohttp.TCPConnector,
):
    """Determine all configured collections for a given pair. Takes care of
    shortcut expansion and result caching.

    Discovery is incremental and reconciled against the roster saved by the
    last successful run:

    * Newly discovered collections are added.
    * A collection that is no longer reported by a healthy side is marked
      ``pending`` instead of being removed. It stays in the cache but is
      excluded from synchronization.
    * If a side cannot be reached, its last-known records are kept untouched.
    * Manually configured collections are always pinned and never removed or
      marked pending.

    The whole roster is rebuilt in memory and written in a single atomic
    operation, so an interrupted run can never leave a partial roster behind.

    :param status_path: The path to the status directory.
    :param from_cache: Whether to load from cache (aborting on cache miss) or
        discover and save to cache.

    :returns: iterable of (collection, (a_args, b_args)), active collections
        only.
    """
    cache_key = _get_collections_cache_key(pair)
    if from_cache:
        rv = load_status(status_path, pair.name, data_type="collections")
        if rv and rv.get("cache_key", None) == cache_key:
            return list(
                _expand_collections_cache(
                    rv["collections"], pair.config_a, pair.config_b
                )
            )
        if rv:
            raise exceptions.UserError(
                "Detected change in config file, "
                f"please run `vdirsyncer discover {pair.name}`."
            )
        raise exceptions.UserError(
            f"Please run `vdirsyncer discover {pair.name}`  before synchronization."
        )

    logger.info(f"Discovering collections for pair {pair.name}")

    a_discover_result = DiscoverResult(pair.config_a, connector=connector)
    b_discover_result = DiscoverResult(pair.config_b, connector=connector)

    if list_collections:
        # TODO: We should gather data and THEN print, so it can be async.
        await _print_collections(
            pair.config_a["instance_name"],
            a_discover_result.get_self,
            connector=connector,
        )
        await _print_collections(
            pair.config_b["instance_name"],
            b_discover_result.get_self,
            connector=connector,
        )

    # The last successfully saved roster is the baseline we reconcile
    # against; it lets us survive transient failures and remember collections
    # that temporarily vanished.
    baseline_entries = _load_baseline(status_path, pair)
    baseline_by_name = {entry["name"]: entry for entry in baseline_entries}

    a_discovered, healthy_a = await a_discover_result.get_self_safe()
    b_discovered, healthy_b = await b_discover_result.get_self_safe()

    if not healthy_a:
        logger.warning(
            f"Failed to discover collections for storage "
            f"{pair.config_a['instance_name']}; keeping its previously known "
            "collections unchanged."
        )
    if not healthy_b:
        logger.warning(
            f"Failed to discover collections for storage "
            f"{pair.config_b['instance_name']}; keeping its previously known "
            "collections unchanged."
        )

    async def _handle_collection_not_found(
        config, collection, e=None, implicit_create=False
    ):
        return await handle_collection_not_found(
            config, collection, e=e, implicit_create=pair.implicit == "create"
        )

    manual_specs, discover_from_a, discover_from_b = _classify_shortcuts(
        pair.collections
    )
    manual_names = {name for name, _, _ in manual_specs}

    # Manually configured collections are pinned: they always survive, no
    # matter what discovery reports.
    manual_entries = []
    for name, collection_a, collection_b in manual_specs:
        prior_entry = baseline_by_name.get(name)
        a_args = await _resolve_side(
            side="a",
            collection=collection_a,
            config=pair.config_a,
            discovered=a_discovered,
            healthy=healthy_a,
            prior_entry=prior_entry,
            handle_not_found=_handle_collection_not_found,
        )
        b_args = await _resolve_side(
            side="b",
            collection=collection_b,
            config=pair.config_b,
            discovered=b_discovered,
            healthy=healthy_b,
            prior_entry=prior_entry,
            handle_not_found=_handle_collection_not_found,
        )
        manual_entries.append(
            {
                "name": name,
                "origin": ORIGIN_MANUAL,
                "state": STATE_ACTIVE,
                "a": a_args,
                "b": b_args,
            }
        )

    # Collect everything the healthy sides currently report. Names that also
    # appear as manual entries are owned by the manual mapping and skipped.
    fresh: dict = {}
    if discover_from_a:
        for name, args in a_discovered.items():
            if name not in manual_names:
                fresh.setdefault(name, [None, None])[0] = args
    if discover_from_b:
        for name, args in b_discovered.items():
            if name not in manual_names:
                fresh.setdefault(name, [None, None])[1] = args

    fresh_discovered = {}
    for name, sides in fresh.items():
        prior_entry = baseline_by_name.get(name)
        a_args, b_args = sides
        if a_args is None:
            a_args = await _resolve_side(
                side="a",
                collection=name,
                config=pair.config_a,
                discovered=a_discovered,
                healthy=healthy_a,
                prior_entry=prior_entry,
                handle_not_found=_handle_collection_not_found,
            )
        if b_args is None:
            b_args = await _resolve_side(
                side="b",
                collection=name,
                config=pair.config_b,
                discovered=b_discovered,
                healthy=healthy_b,
                prior_entry=prior_entry,
                handle_not_found=_handle_collection_not_found,
            )
        fresh_discovered[name] = (a_args, b_args)

    roster = reconcile_roster(
        manual_entries=manual_entries,
        fresh_discovered=fresh_discovered,
        baseline_entries=baseline_entries,
        discovered_a=a_discovered,
        discovered_b=b_discovered,
        healthy_a=healthy_a,
        healthy_b=healthy_b,
        discover_from_a=discover_from_a,
        discover_from_b=discover_from_b,
    )

    active = [
        (entry["name"], (entry["a"], entry["b"]))
        for entry in roster
        if entry["state"] != STATE_PENDING
    ]

    # Only the fully reconciled roster is ever persisted, and in one atomic
    # write. A failure before this point leaves the previous roster intact.
    await _sanity_check_collections(active, connector=connector)

    save_status(
        base_path=status_path,
        pair=pair.name,
        data_type="collections",
        data={
            "version": ROSTER_VERSION,
            "collections": list(
                _compress_collections_cache(roster, pair.config_a, pair.config_b)
            ),
            "cache_key": cache_key,
        },
    )

    for entry in roster:
        if entry["state"] == STATE_PENDING:
            logger.warning(
                "Collection {} is no longer reported by the server and was "
                "marked pending. It is kept in the discovery cache but "
                "excluded from synchronization. Re-create the collection or "
                "remove it from the pair configuration to clear this state.".format(
                    json.dumps(entry["name"])
                )
            )

    return active


async def _sanity_check_collections(collections, *, connector):
    tasks = []

    for _, (a_args, b_args) in collections:
        tasks.append(storage_instance_from_config(a_args, connector=connector))
        tasks.append(storage_instance_from_config(b_args, connector=connector))

    await asyncio.gather(*tasks)


def _deduplicate(x, y):
    rv = {}
    for key, value in x.items():
        if key not in y or y[key] != value:
            rv[key] = value

    return rv


def _normalize_cache_entry(raw):
    """Normalize a persisted roster entry to a common dict shape.

    Accepts the current layout (dict with name/origin/state/a/b) and the
    legacy layout (``[name, [a_delta, b_delta]]``).
    """
    if isinstance(raw, dict):
        return {
            "name": raw["name"],
            "origin": raw.get("origin", ORIGIN_DISCOVERED),
            "state": raw.get("state", STATE_ACTIVE),
            "a": raw.get("a", {}),
            "b": raw.get("b", {}),
        }

    name, (a_delta, b_delta) = raw
    return {
        "name": name,
        "origin": ORIGIN_DISCOVERED,
        "state": STATE_ACTIVE,
        "a": a_delta,
        "b": b_delta,
    }


def _load_baseline(status_path, pair):
    """Load the previous roster as full-arg entries (all states)."""
    raw = load_status(status_path, pair.name, data_type="collections")
    entries = []
    for item in raw.get("collections", []):
        entry = _normalize_cache_entry(item)
        a = dict(pair.config_a)
        a.update(entry["a"])
        b = dict(pair.config_b)
        b.update(entry["b"])
        entries.append({**entry, "a": a, "b": b})
    return entries


def _compress_collections_cache(entries, config_a, config_b):
    for entry in entries:
        yield {
            "name": entry["name"],
            "origin": entry["origin"],
            "state": entry["state"],
            "a": _deduplicate(entry["a"], config_a),
            "b": _deduplicate(entry["b"], config_b),
        }


def _expand_collections_cache(collections, config_a, config_b):
    for raw in collections:
        entry = _normalize_cache_entry(raw)
        if entry["state"] == STATE_PENDING:
            # Pending collections are retained on disk but must not be synced.
            continue

        a = dict(config_a)
        a.update(entry["a"])

        b = dict(config_b)
        b.update(entry["b"])

        yield entry["name"], (a, b)


def _classify_shortcuts(shortcuts):
    """Split ``pair.collections`` into manual specs and discovery flags.

    :returns: (manual_specs, discover_from_a, discover_from_b) where
        ``manual_specs`` is a list of ``(name, collection_a, collection_b)``
        tuples in configuration order.
    """
    if shortcuts is None:
        shortcuts = [None]

    manual_specs = []
    discover_from_a = False
    discover_from_b = False

    for shortcut in shortcuts:
        if shortcut == "from a":
            discover_from_a = True
        elif shortcut == "from b":
            discover_from_b = True
        elif isinstance(shortcut, list):
            name, collection_a, collection_b = shortcut
            manual_specs.append((name, collection_a, collection_b))
        else:
            # An explicit collection name (or null) used on both sides.
            manual_specs.append((shortcut, shortcut, shortcut))

    return manual_specs, discover_from_a, discover_from_b


def _absence_confirmed(
    name,
    *,
    discover_from_a,
    discover_from_b,
    healthy_a,
    healthy_b,
    discovered_a,
    discovered_b,
):
    """Decide whether ``name`` can be treated as really gone.

    A side that drives discovery can only confirm absence when it responded
    successfully. As long as one asserting side is unreachable, disappearance
    is not confirmed and the previous record is preserved.
    """
    assertions = []
    if discover_from_a:
        assertions.append((healthy_a, name in discovered_a))
    if discover_from_b:
        assertions.append((healthy_b, name in discovered_b))

    if not assertions:
        # No side is configured to auto-discover this entry anymore.
        return True

    return all(healthy and not present for healthy, present in assertions)


def reconcile_roster(
    *,
    manual_entries,
    fresh_discovered,
    baseline_entries,
    discovered_a,
    discovered_b,
    healthy_a,
    healthy_b,
    discover_from_a,
    discover_from_b,
):
    """Purely reconcile a fresh discovery against the previous roster.

    The function performs no I/O and is deterministic: feeding it the same
    inputs always yields an equivalent roster, including stable entry states
    and ordering, so repeated discoveries are idempotent.

    :param manual_entries: Resolved pinned entries (always kept active).
    :param fresh_discovered: Mapping of name -> (a_args, b_args) currently
        reported by auto-discovery.
    :param baseline_entries: Entries saved by the last successful run, with
        full args and origin/state metadata.
    """
    baseline_by_name = {entry["name"]: entry for entry in baseline_entries}
    manual_names = {entry["name"] for entry in manual_entries}

    roster = [dict(entry) for entry in manual_entries]

    candidate_names = (set(fresh_discovered) - manual_names) | {
        entry["name"]
        for entry in baseline_entries
        if entry["origin"] == ORIGIN_DISCOVERED and entry["name"] not in manual_names
    }

    # ``None`` can only occur on manual (null) entries, which are handled
    # above; the key keeps sorting total regardless.
    for name in sorted(candidate_names, key=lambda value: (value is None, value)):
        prior = baseline_by_name.get(name)

        if name in fresh_discovered:
            a_args, b_args = fresh_discovered[name]
            if prior is not None:
                a_args = a_args or prior["a"]
                b_args = b_args or prior["b"]
            roster.append(
                {
                    "name": name,
                    "origin": ORIGIN_DISCOVERED,
                    "state": STATE_ACTIVE,
                    "a": a_args,
                    "b": b_args,
                }
            )
            continue

        if prior is None:
            continue

        confirmed = _absence_confirmed(
            name,
            discover_from_a=discover_from_a,
            discover_from_b=discover_from_b,
            healthy_a=healthy_a,
            healthy_b=healthy_b,
            discovered_a=discovered_a,
            discovered_b=discovered_b,
        )
        state = STATE_PENDING if confirmed else prior["state"]
        roster.append(
            {
                "name": name,
                "origin": ORIGIN_DISCOVERED,
                "state": state,
                "a": prior["a"],
                "b": prior["b"],
            }
        )

    return roster


async def _resolve_side(
    *,
    side,
    collection,
    config,
    discovered,
    healthy,
    prior_entry,
    handle_not_found,
):
    """Resolve the storage args for one side of a collection.

    On a healthy side the freshly discovered args are used, falling back to
    the existing not-found handling (prompt/create/error) when absent. On an
    unreachable side the last-known baseline args are reused, or plain config
    args when there is no baseline, so the record survives the outage.
    """
    if collection is None:
        args = dict(config)
        args["collection"] = None
        return args

    if healthy:
        try:
            return discovered[collection]
        except KeyError:
            return await handle_not_found(config, collection)

    if prior_entry is not None:
        prior_args = prior_entry["a" if side == "a" else "b"]
        if prior_args:
            return dict(prior_args)

    args = dict(config)
    args["collection"] = collection
    return args


class DiscoverResult:
    def __init__(self, config, *, connector):
        self._cls, _ = storage_class_from_config(config)

        if self._cls.__name__ in [
            "CardDAVStorage",
            "CalDAVStorage",
            "GoogleCalendarStorage",
            "GoogleContactsStorage",
        ]:
            assert connector is not None
            config["connector"] = connector

        self._config = config
        self._discovered = None
        self._error = None

    async def get_self_safe(self):
        """Discover without raising.

        :returns: ``(discovered, healthy)``; ``healthy`` is ``False`` when the
            storage could not be reached, in which case ``discovered`` is an
            empty dict.
        """
        if self._discovered is None and self._error is None:
            await self._discover()

        if self._error is not None:
            return {}, False

        return self._discovered, True

    async def get_self(self):
        discovered, healthy = await self.get_self_safe()
        if not healthy:
            raise self._error
        return discovered

    async def _discover(self):
        try:
            discovered = await aiostream.stream.list(self._cls.discover(**self._config))
        except NotImplementedError:
            self._discovered = {}
        except Exception:
            # Preserve the historical error conversion (TypeErrors from bad
            # storage parameters become UserErrors) for get_self() callers.
            try:
                handle_storage_init_error(self._cls, self._config)
            except Exception as e:
                self._error = e
            else:  # pragma: no cover - handle_storage_init_error always raises
                self._error = RuntimeError("Storage discovery failed.")
        else:
            storage_type = self._config["type"]
            rv = {}
            for args in discovered:
                args["type"] = storage_type
                rv[args["collection"]] = args
            self._discovered = rv


async def expand_collections(
    shortcuts,
    config_a,
    config_b,
    get_a_discovered,
    get_b_discovered,
    _handle_collection_not_found,
):
    handled_collections = set()

    if shortcuts is None:
        shortcuts = [None]

    for shortcut in shortcuts:
        if shortcut == "from a":
            collections = await get_a_discovered()
        elif shortcut == "from b":
            collections = await get_b_discovered()
        else:
            collections = [shortcut]

        for collection in collections:
            if isinstance(collection, list):
                collection, collection_a, collection_b = collection
            else:
                collection_a = collection_b = collection

            if collection in handled_collections:
                continue
            handled_collections.add(collection)

            a_args = await _collection_from_discovered(
                get_a_discovered,
                collection_a,
                config_a,
                _handle_collection_not_found,
            )
            b_args = await _collection_from_discovered(
                get_b_discovered,
                collection_b,
                config_b,
                _handle_collection_not_found,
            )

            yield collection, (a_args, b_args)


async def _collection_from_discovered(
    get_discovered, collection, config, _handle_collection_not_found
):
    if collection is None:
        args = dict(config)
        args["collection"] = None
        return args

    try:
        return (await get_discovered())[collection]
    except KeyError:
        return await _handle_collection_not_found(config, collection)


async def _print_collections(
    instance_name: str,
    get_discovered,
    *,
    connector: aiohttp.TCPConnector,
):
    try:
        discovered = await get_discovered()
    except exceptions.UserError:
        raise
    except Exception:
        # Unless discovery failed due to a user-inflicted error (instanceof
        # UserError), we don't even know if the storage supports discovery
        # properly. So we can't abort.
        import traceback

        logger.debug("".join(traceback.format_tb(sys.exc_info()[2])))
        logger.warning(
            f"Failed to discover collections for {instance_name}, use `-vdebug` "
            "to see the full traceback."
        )
        return
    logger.info(f"{instance_name}:")
    tasks = []
    for args in discovered.values():
        tasks.append(_print_single_collection(args, instance_name, connector))

    await asyncio.gather(*tasks)


async def _print_single_collection(args, instance_name, connector):
    collection = args["collection"]
    if collection is None:
        return

    args["instance_name"] = instance_name
    try:
        storage = await storage_instance_from_config(
            args,
            create=False,
            connector=connector,
        )
        displayname = await storage.get_meta("displayname")
    except Exception:
        displayname = ""

    logger.info(
        "  - {}{}".format(
            json.dumps(collection),
            f' ("{displayname}")' if displayname and displayname != collection else "",
        )
    )
