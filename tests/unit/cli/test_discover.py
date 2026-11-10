from __future__ import annotations

import aiostream
import pytest

from vdirsyncer.cli.discover import ORIGIN_DISCOVERED
from vdirsyncer.cli.discover import ORIGIN_MANUAL
from vdirsyncer.cli.discover import STATE_ACTIVE
from vdirsyncer.cli.discover import STATE_PENDING
from vdirsyncer.cli.discover import DiscoverResult
from vdirsyncer.cli.discover import _absence_confirmed
from vdirsyncer.cli.discover import _classify_shortcuts
from vdirsyncer.cli.discover import _expand_collections_cache
from vdirsyncer.cli.discover import _normalize_cache_entry
from vdirsyncer.cli.discover import expand_collections
from vdirsyncer.cli.discover import reconcile_roster
from vdirsyncer.cli.utils import storage_names

missing = object()


def _args(side, collection):
    return {"type": "fs", "side": side, "collection": collection}


def _entry(name, state=STATE_ACTIVE, origin=ORIGIN_DISCOVERED):
    return {
        "name": name,
        "origin": origin,
        "state": state,
        "a": _args("a", name),
        "b": _args("b", name),
    }


def _reconcile(**overrides):
    defaults = {
        "manual_entries": [],
        "fresh_discovered": {},
        "baseline_entries": [],
        "discovered_a": {},
        "discovered_b": {},
        "healthy_a": True,
        "healthy_b": True,
        "discover_from_a": True,
        "discover_from_b": False,
    }
    defaults.update(overrides)
    return reconcile_roster(**defaults)


def _names(roster):
    return [entry["name"] for entry in roster]


def _by_name(roster):
    return {entry["name"]: entry for entry in roster}


@pytest.mark.parametrize(
    ("shortcuts", "expected"),
    [
        (
            ["from a"],
            [
                (
                    "c1",
                    (
                        {"type": "fooboo", "custom_arg": "a1", "collection": "c1"},
                        {"type": "fooboo", "custom_arg": "b1", "collection": "c1"},
                    ),
                ),
                (
                    "c2",
                    (
                        {"type": "fooboo", "custom_arg": "a2", "collection": "c2"},
                        {"type": "fooboo", "custom_arg": "b2", "collection": "c2"},
                    ),
                ),
                (
                    "a3",
                    (
                        {"type": "fooboo", "custom_arg": "a3", "collection": "a3"},
                        missing,
                    ),
                ),
            ],
        ),
        (
            ["from b"],
            [
                (
                    "c1",
                    (
                        {"type": "fooboo", "custom_arg": "a1", "collection": "c1"},
                        {"type": "fooboo", "custom_arg": "b1", "collection": "c1"},
                    ),
                ),
                (
                    "c2",
                    (
                        {"type": "fooboo", "custom_arg": "a2", "collection": "c2"},
                        {"type": "fooboo", "custom_arg": "b2", "collection": "c2"},
                    ),
                ),
                (
                    "b3",
                    (
                        missing,
                        {"type": "fooboo", "custom_arg": "b3", "collection": "b3"},
                    ),
                ),
            ],
        ),
        (
            ["from a", "from b"],
            [
                (
                    "c1",
                    (
                        {"type": "fooboo", "custom_arg": "a1", "collection": "c1"},
                        {"type": "fooboo", "custom_arg": "b1", "collection": "c1"},
                    ),
                ),
                (
                    "c2",
                    (
                        {"type": "fooboo", "custom_arg": "a2", "collection": "c2"},
                        {"type": "fooboo", "custom_arg": "b2", "collection": "c2"},
                    ),
                ),
                (
                    "a3",
                    (
                        {"type": "fooboo", "custom_arg": "a3", "collection": "a3"},
                        missing,
                    ),
                ),
                (
                    "b3",
                    (
                        missing,
                        {"type": "fooboo", "custom_arg": "b3", "collection": "b3"},
                    ),
                ),
            ],
        ),
        (
            [["c12", "c1", "c2"]],
            [
                (
                    "c12",
                    (
                        {"type": "fooboo", "custom_arg": "a1", "collection": "c1"},
                        {"type": "fooboo", "custom_arg": "b2", "collection": "c2"},
                    ),
                ),
            ],
        ),
        (
            None,
            [
                (
                    None,
                    (
                        {"type": "fooboo", "storage_side": "a", "collection": None},
                        {"type": "fooboo", "storage_side": "b", "collection": None},
                    ),
                )
            ],
        ),
        (
            [None],
            [
                (
                    None,
                    (
                        {"type": "fooboo", "storage_side": "a", "collection": None},
                        {"type": "fooboo", "storage_side": "b", "collection": None},
                    ),
                )
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_expand_collections(shortcuts, expected):
    config_a = {"type": "fooboo", "storage_side": "a"}

    config_b = {"type": "fooboo", "storage_side": "b"}

    async def get_discovered_a():
        return {
            "c1": {"type": "fooboo", "custom_arg": "a1", "collection": "c1"},
            "c2": {"type": "fooboo", "custom_arg": "a2", "collection": "c2"},
            "a3": {"type": "fooboo", "custom_arg": "a3", "collection": "a3"},
        }

    async def get_discovered_b():
        return {
            "c1": {"type": "fooboo", "custom_arg": "b1", "collection": "c1"},
            "c2": {"type": "fooboo", "custom_arg": "b2", "collection": "c2"},
            "b3": {"type": "fooboo", "custom_arg": "b3", "collection": "b3"},
        }

    async def handle_not_found(config, collection):
        return missing

    assert sorted(
        await aiostream.stream.list(
            expand_collections(
                shortcuts,
                config_a,
                config_b,
                get_discovered_a,
                get_discovered_b,
                handle_not_found,
            )
        )
    ) == sorted(expected)


def test_reconcile_adds_new_collections_and_refreshes_existing():
    baseline = [_entry("c1")]
    fresh = {
        "c1": (_args("a", "c1"), _args("b", "c1")),
        "c2": (_args("a", "c2"), _args("b", "c2")),
    }

    roster = _reconcile(
        fresh_discovered=fresh,
        baseline_entries=baseline,
        discovered_a={"c1": _args("a", "c1"), "c2": _args("a", "c2")},
    )

    assert _names(roster) == ["c1", "c2"]
    assert all(entry["state"] == STATE_ACTIVE for entry in roster)
    assert all(entry["origin"] == ORIGIN_DISCOVERED for entry in roster)


def test_reconcile_confirmed_disappearance_becomes_pending_and_keeps_args():
    prior_c2 = _entry("c2")
    roster = _reconcile(
        fresh_discovered={"c1": (_args("a", "c1"), _args("b", "c1"))},
        baseline_entries=[_entry("c1"), prior_c2],
        discovered_a={"c1": _args("a", "c1")},
    )

    by_name = _by_name(roster)
    assert by_name["c1"]["state"] == STATE_ACTIVE
    pending = by_name["c2"]
    assert pending["state"] == STATE_PENDING
    # The last-known args are retained instead of being dropped.
    assert pending["a"] == prior_c2["a"]
    assert pending["b"] == prior_c2["b"]


def test_reconcile_unreachable_asserting_side_keeps_record_active():
    roster = _reconcile(
        fresh_discovered={"c1": (_args("a", "c1"), _args("b", "c1"))},
        baseline_entries=[_entry("c1"), _entry("c2")],
        discovered_a={},
        healthy_a=False,
    )

    by_name = _by_name(roster)
    assert by_name["c2"]["state"] == STATE_ACTIVE
    assert by_name["c2"]["a"] == _args("a", "c2")


def test_reconcile_other_side_down_does_not_block_healthy_confirmation():
    # Both sides drive discovery; b is down but a (healthy) no longer lists
    # c2. Because one asserting side is unreachable, deletion is not confirmed.
    roster = _reconcile(
        fresh_discovered={"c1": (_args("a", "c1"), _args("b", "c1"))},
        baseline_entries=[_entry("c1"), _entry("c2")],
        discovered_a={"c1": _args("a", "c1")},
        discovered_b={},
        healthy_a=True,
        healthy_b=False,
        discover_from_a=True,
        discover_from_b=True,
    )
    assert _by_name(roster)["c2"]["state"] == STATE_ACTIVE

    # With both sides healthy and neither listing c2 it becomes pending.
    roster = _reconcile(
        fresh_discovered={"c1": (_args("a", "c1"), _args("b", "c1"))},
        baseline_entries=[_entry("c1"), _entry("c2")],
        discovered_a={"c1": _args("a", "c1")},
        discovered_b={"c1": _args("b", "c1")},
        healthy_a=True,
        healthy_b=True,
        discover_from_a=True,
        discover_from_b=True,
    )
    assert _by_name(roster)["c2"]["state"] == STATE_PENDING


def test_reconcile_pending_collection_is_promoted_when_seen_again():
    roster = _reconcile(
        fresh_discovered={"c1": (_args("a", "c1"), _args("b", "c1"))},
        baseline_entries=[_entry("c1", state=STATE_PENDING)],
        discovered_a={"c1": _args("a", "c1")},
    )
    assert _by_name(roster)["c1"]["state"] == STATE_ACTIVE


def test_reconcile_pending_is_stable_while_still_absent():
    baseline = [_entry("c1", state=STATE_PENDING)]
    kwargs = {
        "fresh_discovered": {},
        "baseline_entries": baseline,
        "discovered_a": {},
    }
    first = _reconcile(**kwargs)
    second = _reconcile(
        baseline_entries=first,
        **{k: v for k, v in kwargs.items() if k != "baseline_entries"},
    )
    assert first == second
    assert _by_name(first)["c1"]["state"] == STATE_PENDING


def test_reconcile_manual_entries_are_always_pinned():
    manual = [
        {
            "name": "m",
            "origin": ORIGIN_MANUAL,
            "state": STATE_ACTIVE,
            "a": _args("a", "ma"),
            "b": _args("b", "mb"),
        }
    ]

    # Healthy sides do not report the manual collection at all.
    roster = _reconcile(manual_entries=manual, discovered_a={}, discovered_b={})
    pinned = _by_name(roster)["m"]
    assert pinned["origin"] == ORIGIN_MANUAL
    assert pinned["state"] == STATE_ACTIVE

    # An unreachable side does not change the manual mapping either.
    roster = _reconcile(
        manual_entries=manual,
        discovered_a={},
        discovered_b={},
        healthy_a=False,
        healthy_b=False,
    )
    assert _by_name(roster)["m"] == manual[0]

    # A manual name must never be duplicated by auto-discovery.
    roster = _reconcile(
        manual_entries=manual,
        fresh_discovered={"m": (_args("a", "m"), _args("b", "m"))},
        discovered_a={"m": _args("a", "m")},
    )
    assert _names(roster) == ["m"]
    assert _by_name(roster)["m"]["origin"] == ORIGIN_MANUAL


def test_reconcile_is_idempotent_for_identical_discovery():
    baseline = [_entry("c1"), _entry("c2")]
    kwargs = {
        "fresh_discovered": {"c1": (_args("a", "c1"), _args("b", "c1"))},
        "discovered_a": {"c1": _args("a", "c1")},
    }
    first = _reconcile(baseline_entries=baseline, **kwargs)
    second = _reconcile(baseline_entries=first, **kwargs)
    third = _reconcile(baseline_entries=second, **kwargs)

    assert first == second == third
    assert [entry["state"] for entry in first] == [
        STATE_ACTIVE,
        STATE_PENDING,
    ]


def test_reconcile_ordering_is_deterministic():
    manual = [
        {
            "name": "manual_z",
            "origin": ORIGIN_MANUAL,
            "state": STATE_ACTIVE,
            "a": _args("a", "manual_z"),
            "b": _args("b", "manual_z"),
        }
    ]
    fresh = {name: (_args("a", name), _args("b", name)) for name in ("z", "a", "m")}
    roster = _reconcile(
        manual_entries=manual,
        fresh_discovered=fresh,
        discovered_a=dict(fresh),
    )
    # Manual entries first (config order), discovered names sorted.
    assert _names(roster) == ["manual_z", "a", "m", "z"]


def test_absence_confirmed_matrix():
    common = {
        "name": "c",
        "discover_from_a": True,
        "discover_from_b": False,
        "discovered_a": {},
        "discovered_b": {},
    }
    # Healthy asserting side without the collection -> confirmed.
    assert _absence_confirmed(healthy_a=True, healthy_b=True, **common) is True
    # Unreachable asserting side -> cannot confirm.
    assert _absence_confirmed(healthy_a=False, healthy_b=True, **common) is False
    # Healthy side still reporting it -> not gone.
    assert (
        _absence_confirmed(
            healthy_a=True,
            healthy_b=True,
            discovered_a={"c": _args("a", "c")},
            **{k: v for k, v in common.items() if k != "discovered_a"},
        )
        is False
    )


def test_classify_shortcuts():
    manual, from_a, from_b = _classify_shortcuts(None)
    assert manual == [(None, None, None)]
    assert not (from_a or from_b)

    manual, from_a, from_b = _classify_shortcuts(
        ["from a", "from b", "plain", ["x", "xa", "xb"]]
    )
    assert from_a and from_b
    assert manual == [("plain", "plain", "plain"), ("x", "xa", "xb")]


def test_cache_entry_normalization_and_pending_filtering():
    legacy = ["legacy", [{"collection": "legacy"}, {}]]
    entry = _normalize_cache_entry(legacy)
    assert entry == {
        "name": "legacy",
        "origin": ORIGIN_DISCOVERED,
        "state": STATE_ACTIVE,
        "a": {"collection": "legacy"},
        "b": {},
    }

    config_a = {"type": "fs", "side": "a"}
    config_b = {"type": "fs", "side": "b"}
    expanded = list(
        _expand_collections_cache(
            [
                legacy,
                {
                    "name": "gone",
                    "origin": ORIGIN_DISCOVERED,
                    "state": STATE_PENDING,
                    "a": {"collection": "gone"},
                    "b": {},
                },
                {
                    "name": "kept",
                    "origin": ORIGIN_MANUAL,
                    "state": STATE_ACTIVE,
                    "a": {"collection": "kept"},
                    "b": {},
                },
            ],
            config_a,
            config_b,
        )
    )
    assert [name for name, _ in expanded] == ["legacy", "kept"]


class _OKStorage:
    @classmethod
    async def discover(cls, **kwargs):
        yield {"collection": "c1", "k": "v"}


class _FailStorage:
    @classmethod
    async def discover(cls, **kwargs):
        raise RuntimeError("server unreachable")
        yield  # pragma: no cover


class _NotImplementedStorage:
    @classmethod
    async def discover(cls, **kwargs):
        raise NotImplementedError
        yield  # pragma: no cover


@pytest.mark.asyncio
async def test_discover_result_reports_per_side_health(monkeypatch):
    monkeypatch.setitem(storage_names._storages, "okstorage", _OKStorage)
    monkeypatch.setitem(storage_names._storages, "failstorage", _FailStorage)
    monkeypatch.setitem(storage_names._storages, "nistorage", _NotImplementedStorage)

    ok = DiscoverResult({"type": "okstorage"}, connector=None)
    discovered, healthy = await ok.get_self_safe()
    assert healthy is True
    assert set(discovered) == {"c1"}

    failing = DiscoverResult({"type": "failstorage"}, connector=None)
    assert await failing.get_self_safe() == ({}, False)
    with pytest.raises(RuntimeError):
        await failing.get_self()

    unsupported = DiscoverResult({"type": "nistorage"}, connector=None)
    assert await unsupported.get_self_safe() == ({}, True)
