from __future__ import annotations

import logging

from . import exceptions
from .storage.base import normalize_meta_value

logger = logging.getLogger(__name__)

#: Side identifiers recorded as the source of a successful write.
SOURCE_A = "a"
SOURCE_B = "b"

#: The value was adopted without a single clear source, e.g. because both
#: sides already converged on the same value or because no provenance
#: information existed. Such entries are never force-propagated.
SOURCE_UNKNOWN = None


class MetaSyncError(exceptions.Error):
    pass


class MetaSyncConflict(MetaSyncError):
    key = None


def status_set_key(status, key, value):
    if value is None:
        status.pop(key, None)
    else:
        status[key] = value


def _is_provenance_entry(entry) -> bool:
    """New-style status entries carry source/generation provenance."""
    return isinstance(entry, dict)


def _load_entry(entry):
    """Return ``(value, source, generation)`` for a raw status entry.

    Legacy entries are plain values without any provenance. They report
    ``source=None`` and ``generation=None`` so the caller can fall back to
    the original, status-only conflict strategy.
    """
    if _is_provenance_entry(entry):
        generation = entry.get("generation")
        if generation is not None:
            generation = int(generation)
        return (
            normalize_meta_value(entry.get("value")),
            entry.get("source"),
            generation,
        )
    return normalize_meta_value(entry), SOURCE_UNKNOWN, None


def _record(status, key, value, source, generation):
    """Persist a provenance entry after a successful write or adoption."""
    value = normalize_meta_value(value)
    if value is None:
        status.pop(key, None)
    else:
        status[key] = {
            "value": value,
            "source": source,
            "generation": generation,
        }


async def metasync(storage_a, storage_b, status, keys, conflict_resolution=None):
    async def _commit(value, source):
        # Every actual transition is recorded with its source and a new
        # generation. Legacy plain-value entries are upgraded on their first
        # transition; untouched entries keep their original shape.
        _record(status, key, value, source, next_generation)

    async def _a_to_b():
        logger.info(f"Copying {key} to {storage_b}")
        await storage_b.set_meta(key, a)
        await _commit(a, SOURCE_A)

    async def _b_to_a():
        logger.info(f"Copying {key} to {storage_a}")
        await storage_a.set_meta(key, b)
        await _commit(b, SOURCE_B)

    async def _resolve_conflict():
        if a == b:
            # Both sides hold the same value. If the status already records
            # exactly that value, there is nothing to do and especially no
            # reason to bump the generation. Otherwise the value was adopted
            # without a single clear source.
            if a == s:
                return
            await _commit(a, SOURCE_UNKNOWN)
        elif conflict_resolution == "a wins":
            await _a_to_b()
        elif conflict_resolution == "b wins":
            await _b_to_a()
        else:
            if callable(conflict_resolution):
                logger.warning("Custom commands don't work on metasync.")
            elif conflict_resolution is not None:
                raise exceptions.UserError("Invalid conflict resolution setting.")
            # Raise before touching any storage or status, so rerunning the
            # metasync reproduces this conflict instead of overwriting data.
            raise MetaSyncConflict(key)

    for key in keys:
        a = await storage_a.get_meta(key)
        b = await storage_b.get_meta(key)
        raw_entry = status.get(key)
        s, source, generation = _load_entry(raw_entry)
        next_generation = (generation or 0) + 1
        logger.debug(f"Key: {key}")
        logger.debug(f"A: {a}")
        logger.debug(f"B: {b}")
        logger.debug(f"S: {s}")
        logger.debug(f"Source: {source}")
        logger.debug(f"Generation: {generation}")

        if (a != s and b != s) or storage_a.read_only or storage_b.read_only:
            # Both sides diverged from the last known value (multiple
            # concurrent sources), a storage is read-only, or the status
            # predates provenance tracking: defer to the configured conflict
            # strategy instead of guessing a direction.
            await _resolve_conflict()
        elif a != s and b == s:
            # Exactly one clear source: side A carries the newer value,
            # possibly because it was edited outside of vdirsyncer.
            await _a_to_b()
        elif a == s and b != s:
            # Exactly one clear source: side B carries the newer value,
            # possibly because it was edited outside of vdirsyncer.
            await _b_to_a()
        else:
            assert a == b

    # Keys that were removed from the configuration only lose their local
    # tracking state; their values on the storages are never rewritten.
    for key in set(status) - set(keys):
        del status[key]
