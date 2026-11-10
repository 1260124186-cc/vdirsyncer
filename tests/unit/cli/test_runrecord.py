from __future__ import annotations

import copy
import json
import os

import pytest

from vdirsyncer.cli.runrecord import RESULT_FAILED
from vdirsyncer.cli.runrecord import RESULT_SUCCESS
from vdirsyncer.cli.runrecord import RUN_RECORD_VERSION
from vdirsyncer.cli.runrecord import STATE_COMPLETE
from vdirsyncer.cli.runrecord import STATE_RUNNING
from vdirsyncer.cli.runrecord import RunRecordError
from vdirsyncer.cli.runrecord import SyncRun
from vdirsyncer.cli.runrecord import build_new_record
from vdirsyncer.cli.runrecord import compute_fingerprint
from vdirsyncer.cli.runrecord import entry_identity
from vdirsyncer.cli.runrecord import get_run_record_path
from vdirsyncer.cli.runrecord import load_run_record
from vdirsyncer.cli.runrecord import save_run_record


def _entry(pair="foobar", collection="a", result=None, path_b="/tmp/bar"):
    return {
        "pair": pair,
        "collection": collection,
        "config_a": {
            "type": "filesystem",
            "instance_name": "foo",
            "path": "/tmp/foo",
            "collection": collection,
        },
        "config_b": {
            "type": "filesystem",
            "instance_name": "bar",
            "path": path_b,
            "collection": collection,
        },
        "result": result,
    }


def _plan():
    return [_entry(collection="a"), _entry(collection="b"), _entry(collection="c")]


def test_fingerprint_is_stable_and_order_independent():
    plan = _plan()
    assert compute_fingerprint(plan) == compute_fingerprint(copy.deepcopy(plan))
    assert compute_fingerprint(plan) == compute_fingerprint(list(reversed(plan)))


def test_fingerprint_ignores_results_but_covers_plan_and_mapping():
    plan = _plan()
    with_results = [dict(entry, result=RESULT_SUCCESS) for entry in plan]
    assert compute_fingerprint(plan) == compute_fingerprint(with_results)

    changed_target = _plan()
    changed_target[-1]["collection"] = "d"
    assert compute_fingerprint(plan) != compute_fingerprint(changed_target)

    changed_mapping = _plan()
    changed_mapping[0]["config_b"]["path"] = "/tmp/elsewhere"
    assert compute_fingerprint(plan) != compute_fingerprint(changed_mapping)

    changed_pair = _plan()
    changed_pair[0]["pair"] = "otherpair"
    assert compute_fingerprint(plan) != compute_fingerprint(changed_pair)


def test_build_new_record():
    record = build_new_record(_plan())
    assert record["version"] == RUN_RECORD_VERSION
    assert record["state"] == STATE_RUNNING
    assert isinstance(record["fingerprint"], str)
    assert len(record["collections"]) == 3
    assert [entry_identity(e) for e in record["collections"]] == [
        "foobar/a",
        "foobar/b",
        "foobar/c",
    ]
    assert all(e["result"] is None for e in record["collections"])


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "status" / ".sync_run.json"
    record = build_new_record(_plan())
    save_run_record(str(path), record)

    assert os.stat(path).st_mode & 0o777 == 0o600
    loaded = load_run_record(str(path))
    assert loaded == record


def test_load_corrupt_json_raises(tmp_path):
    path = tmp_path / ".sync_run.json"
    path.write_text("{not valid json")
    with pytest.raises(RunRecordError):
        load_run_record(str(path))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.update(version=999),
        lambda r: r.update(state="weird"),
        lambda r: r.update(fingerprint=123),
        lambda r: r.update(collections="nope"),
        lambda r: r["collections"][0].update(pair=""),
        lambda r: r["collections"][0].update(pair=42),
        lambda r: r["collections"][0].update(collection=42),
        lambda r: r["collections"][0].update(config_a="nope"),
        lambda r: r["collections"][0].update(config_b=[]),
        lambda r: r["collections"][0].update(result="maybe"),
        lambda r: r["collections"].append(dict(r["collections"][0])),
        lambda r: r.update(fingerprint="deadbeef"),
        lambda r: r["collections"][0]["config_b"].update(path="/changed"),
    ],
)
def test_load_rejects_inconsistent_records(tmp_path, mutate):
    path = tmp_path / ".sync_run.json"
    record = build_new_record(_plan())
    mutate(record)
    with open(path, "w") as f:
        json.dump(record, f)
    with pytest.raises(RunRecordError):
        load_run_record(str(path))


def test_load_rejects_non_object(tmp_path):
    path = tmp_path / ".sync_run.json"
    path.write_text(json.dumps(["nope"]))
    with pytest.raises(RunRecordError):
        load_run_record(str(path))


def test_open_without_existing_record_starts_fresh(tmp_path):
    entries = _plan()
    run = SyncRun.open(str(tmp_path), entries)
    assert run.resumed is False
    assert run.record["state"] == STATE_RUNNING
    assert os.path.exists(get_run_record_path(str(tmp_path)))
    assert all(run.should_sync(entry_identity(e)) for e in entries)


def test_open_resumes_running_record_with_matching_plan(tmp_path):
    path = get_run_record_path(str(tmp_path))
    record = build_new_record(_plan())
    record["collections"][0]["result"] = RESULT_SUCCESS
    record["collections"][1]["result"] = RESULT_FAILED
    save_run_record(path, record)

    run = SyncRun.open(str(tmp_path), _plan())
    assert run.resumed is True
    assert run.should_sync("foobar/a") is False
    assert run.should_sync("foobar/b") is True
    assert run.should_sync("foobar/c") is True
    assert run.has_failure() is True


def test_open_discards_completed_record(tmp_path):
    path = get_run_record_path(str(tmp_path))
    record = build_new_record(_plan())
    record["state"] = STATE_COMPLETE
    for entry in record["collections"]:
        entry["result"] = RESULT_SUCCESS
    save_run_record(path, record)

    run = SyncRun.open(str(tmp_path), _plan())
    assert run.resumed is False
    assert run.record["state"] == STATE_RUNNING
    assert all(e["result"] is None for e in run.record["collections"])
    assert all(run.should_sync(entry_identity(e)) for e in run.record["collections"])


def test_open_discards_running_record_with_changed_plan(tmp_path):
    path = get_run_record_path(str(tmp_path))
    record = build_new_record(_plan())
    for entry in record["collections"]:
        entry["result"] = RESULT_SUCCESS
    save_run_record(path, record)

    changed_plan = _plan()
    changed_plan[0]["config_b"]["path"] = "/tmp/changed"
    run = SyncRun.open(str(tmp_path), changed_plan)
    assert run.resumed is False
    assert all(e["result"] is None for e in run.record["collections"])
    assert all(run.should_sync(entry_identity(e)) for e in run.record["collections"])


def test_open_aborts_on_corrupt_record(tmp_path):
    get_run_record_path(str(tmp_path))
    path = tmp_path / ".sync_run.json"
    path.write_text("broken")
    with pytest.raises(RunRecordError):
        SyncRun.open(str(tmp_path), _plan())
    # The corrupt record must not be replaced.
    assert path.read_text() == "broken"


def test_mark_result_persists_and_finish_converges(tmp_path):
    run = SyncRun.open(str(tmp_path), _plan())
    run.mark_result("foobar/a", True)
    run.mark_result("foobar/b", False)

    loaded = load_run_record(get_run_record_path(str(tmp_path)))
    results = {entry_identity(e): e["result"] for e in loaded["collections"]}
    assert results == {
        "foobar/a": RESULT_SUCCESS,
        "foobar/b": RESULT_FAILED,
        "foobar/c": None,
    }

    # One collection is still pending: the run must not converge.
    run.finish()
    assert run.record["state"] == STATE_RUNNING

    run.mark_result("foobar/c", True)
    run.finish()
    assert run.record["state"] == STATE_COMPLETE
    assert run.has_failure() is True

    loaded = load_run_record(get_run_record_path(str(tmp_path)))
    assert loaded["state"] == STATE_COMPLETE


def test_all_success_converges_without_failure(tmp_path):
    run = SyncRun.open(str(tmp_path), _plan())
    for identity in ("foobar/a", "foobar/b", "foobar/c"):
        run.mark_result(identity, True)
    run.finish()
    assert run.record["state"] == STATE_COMPLETE
    assert run.has_failure() is False


def test_should_sync_unknown_identity_aborts(tmp_path):
    run = SyncRun.open(str(tmp_path), _plan())
    with pytest.raises(RunRecordError):
        run.should_sync("foobar/missing")
