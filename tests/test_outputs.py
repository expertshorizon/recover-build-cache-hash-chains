import csv
import hashlib
import json
import os
import sqlite3

import agent_bridge
import pytest
from conftest import (
    DECOY_KINDS,
    GENESIS_HASH,
    HOLDOUT_DIR,
    LEXICOGRAPHIC_TRAP_KINDS,
    PAYLOAD_BOUND_OPS,
    SPEC_OPS,
    SPEC_TRANSITIONS,
    build_valid_chain,
    entry_row,
    lexicographic_trap_names,
    make_entry,
    spec_apply,
    spec_build_chain,
    spec_build_cascading_fork_chain,
    spec_build_concurrent_independent_forks_chain,
    spec_build_fork_chain,
    spec_build_global_truncation_chain,
    spec_build_interaction_chain,
    spec_build_misleading_name_fork_chain,
    spec_build_nested_fork_chain,
    spec_load_fragments,
    spec_recover,
    write_candidates_dir,
    write_fragment_csv,
)

BUNDLED_DATA_DIR = "/app/data/fragments"
HIDDEN_DATASET_NAMES = [
    "hidden_dataset",
    "hidden_dataset_2",
    "hidden_dataset_3",
    "hidden_dataset_4",
    "hidden_dataset_5",
    "hidden_dataset_6",
    "hidden_dataset_7",
    "hidden_dataset_8",
    "hidden_dataset_9",
    "hidden_dataset_10",
]
CSV_HEADER = "seq,artifact_id,op,content_hash,size_bytes,ts,prev_hash,chain_hash"


def read_pipeline_outputs(db_path, summary_path):
    conn = sqlite3.connect(db_path)
    chain_end_row = conn.execute("SELECT value FROM summary WHERE metric = 'chain_end'").fetchone()
    chain_end = chain_end_row[0] if chain_end_row is not None else None
    distinct_row = conn.execute("SELECT value FROM summary WHERE metric = 'distinct_artifacts'").fetchone()
    distinct_artifacts = distinct_row[0] if distinct_row is not None else None
    counts = {}
    for op in SPEC_OPS:
        row = conn.execute("SELECT value FROM summary WHERE metric = ?", (op,)).fetchone()
        counts[op] = row[0] if row is not None else None
    rows = conn.execute(
        "SELECT seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash, chain_hash FROM ledger ORDER BY seq"
    ).fetchall()
    recovered_seqs = [row[0] for row in rows]
    artifact_states = {
        row[0]: [row[1], row[2]]
        for row in conn.execute("SELECT artifact_id, state, last_seq FROM artifact_state")
    }
    per_artifact_seqs = {}
    for artifact_id, seq in conn.execute("SELECT artifact_id, seq FROM ledger ORDER BY seq"):
        per_artifact_seqs.setdefault(artifact_id, []).append(seq)
    conn.close()
    with open(summary_path) as fh:
        summary_json = json.load(fh)
    assert summary_json == {
        "chain_end": chain_end,
        "distinct_artifacts": distinct_artifacts,
        "counts": counts,
        "artifact_states": {artifact: value[0] for artifact, value in artifact_states.items()},
    }
    return {
        "chain_end": chain_end,
        "distinct_artifacts": distinct_artifacts,
        "counts": counts,
        "recovered_seqs": recovered_seqs,
        "rows": rows,
        "artifact_states": artifact_states,
        "summary_json": summary_json,
        "per_artifact_seqs": per_artifact_seqs,
        "db_path": db_path,
        "summary_path": summary_path,
    }


def run_pipeline_isolated(directory, timeout=30):
    scratch = agent_bridge.new_scratch_dir()
    db_path = os.path.join(scratch, "pipeline.db")
    summary_path = os.path.join(scratch, "pipeline_summary.json")
    agent_bridge.call_agent(
        "run_main_isolated",
        {"directory": str(directory), "db_path": db_path, "summary_path": summary_path},
        timeout=timeout,
    )
    return read_pipeline_outputs(db_path, summary_path)


def run_pipeline_isolated_paths(directory, db_path, summary_path, timeout=30):
    agent_bridge.call_agent(
        "run_main_isolated",
        {"directory": str(directory), "db_path": db_path, "summary_path": summary_path},
        timeout=timeout,
    )
    return read_pipeline_outputs(db_path, summary_path)


def compute_actual_and_expected(directory, timeout=30):
    expected = spec_recover(str(directory))
    actual = run_pipeline_isolated(directory, timeout=timeout)
    return actual, expected


def assert_matches_spec(directory, timeout=30):
    actual, expected = compute_actual_and_expected(directory, timeout=timeout)
    assert actual["chain_end"] == expected["chain_end"]
    assert actual["counts"] == expected["counts"]
    assert actual["recovered_seqs"] == expected["recovered_seqs"]
    assert actual["distinct_artifacts"] == expected["distinct_artifacts"]
    actual_states = {artifact_id: value[0] for artifact_id, value in actual["artifact_states"].items()}
    assert actual_states == expected["final_states"]
    recovered, _ = spec_build_chain(spec_load_fragments(str(directory)))
    assert actual["rows"] == [tuple(entry_row(row)) for row in recovered]
    last_seqs = {row["artifact_id"]: row["seq"] for row in recovered}
    assert {artifact: value[1] for artifact, value in actual["artifact_states"].items()} == last_seqs
    return actual, expected


@pytest.fixture(scope="session")
def bundled_pipeline():
    agent_bridge.call_agent("run_main", {}, timeout=60)
    return read_pipeline_outputs("/app/output/ledger.db", "/app/output/summary.json")


FORK_SINGLE_PARAMS = [
    (seed, 4 + seed % 5, 1 + seed % 6, (1 + seed % 6) + 1 + seed % 5, 1 + seed % 3, seed % 2 == 0)
    for seed in range(14)
]


@pytest.mark.parametrize("index", range(len(FORK_SINGLE_PARAMS)))
def test_max_reach_single_fork_prefers_longer_branch(tmp_path, index):
    """Longer branch always wins."""
    seed, warmup, dead_depth, true_tail, n_artifacts, dead_first = FORK_SINGLE_PARAMS[index]
    built = spec_build_fork_chain(
        seed, warmup, dead_depth, true_tail, n_artifacts=n_artifacts, dead_end_first=dead_first
    )
    write_candidates_dir(tmp_path, built["candidates"])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["expected_chain_end"]


CASCADING_FORK_PARAMS = [
    (seed, 2 + seed % 3, 3 + seed % 4, 2 + seed % 5, 3 + seed % 5 + 2, 1 + seed % 3)
    for seed in range(18)
]


@pytest.mark.parametrize("index", range(len(CASCADING_FORK_PARAMS)))
def test_max_reach_cascading_forks_resolve_correctly(tmp_path, index):
    """Repeated forks resolve correctly."""
    seed, n_forks, warmup, dead_depth, true_tail, n_artifacts = CASCADING_FORK_PARAMS[index]
    candidates, expected_end = spec_build_cascading_fork_chain(
        seed, n_forks=n_forks, warmup_length=warmup, dead_depth=dead_depth, true_tail=true_tail, n_artifacts=n_artifacts
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == expected_end


TIEBREAK_EQUAL_PARAMS = [
    (seed, 3 + seed % 4, 2 + seed % 5, 1 + seed % 3, seed % 2 == 0)
    for seed in range(12)
]


@pytest.mark.parametrize("index", range(len(TIEBREAK_EQUAL_PARAMS)))
def test_max_reach_tiebreak_among_equal_branches(tmp_path, index):
    """Equal reach breaks alphabetically."""
    seed, warmup, depth, n_artifacts, dead_first = TIEBREAK_EQUAL_PARAMS[index]
    built = spec_build_fork_chain(
        seed, warmup, depth, depth, n_artifacts=n_artifacts, dead_end_first=dead_first
    )
    write_candidates_dir(tmp_path, built["candidates"])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["expected_chain_end"]
    winner_op = built["dead_branch_op"] if built["dead_branch_name"] < built["true_branch_name"] else built["true_branch_op"]
    fork_row = [row for row in actual["rows"] if row[0] == built["fork_seq"]]
    assert len(fork_row) == 1
    assert fork_row[0][2] == winner_op


FORK_GLOBAL_PARAMS = [(seed, 2 + seed % 3, 3 + seed % 5, 4 + seed % 6) for seed in range(12)]


@pytest.mark.parametrize("index", range(len(FORK_GLOBAL_PARAMS)))
def test_max_reach_fork_with_other_artifacts_unaffected(tmp_path, index):
    """Fork affects only its artifact."""
    seed, n_artifacts, dead_depth, true_tail = FORK_GLOBAL_PARAMS[index]
    built = spec_build_fork_chain(
        seed, warmup_length=5, dead_end_depth=dead_depth, true_tail_length=true_tail,
        n_artifacts=n_artifacts, dead_end_first=True,
    )
    write_candidates_dir(tmp_path, built["candidates"])
    actual, expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["expected_chain_end"]
    assert set(actual["artifact_states"].keys()) == set(expected["final_states"].keys())


NESTED_FORK_PARAMS = [
    (seed, 2 + seed % 3, 2 + seed % 2, 10 + seed % 5)
    for seed in range(10)
]


@pytest.mark.parametrize("index", range(len(NESTED_FORK_PARAMS)))
def test_max_reach_nested_fork_requires_backtracking(tmp_path, index):
    """A fork's own branch hides a second fork that decides the real winner."""
    seed, long_prefix_len, inner_dead_depth, inner_true_depth = NESTED_FORK_PARAMS[index]
    candidates, expected_end = spec_build_nested_fork_chain(
        seed, warmup_length=3, long_prefix_len=long_prefix_len,
        inner_dead_depth=inner_dead_depth, inner_true_depth=inner_true_depth,
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == expected_end


MISLEADING_NAME_PARAMS = [(seed, 2 + seed % 3, 6 + seed % 4) for seed in range(4)]


@pytest.mark.parametrize("index", range(len(MISLEADING_NAME_PARAMS)))
def test_misleading_filenames_never_influence_winner_choice(tmp_path, index):
    """A decoy-sounding name can still be the true, longer-reaching branch."""
    seed, dead_depth, true_tail = MISLEADING_NAME_PARAMS[index]
    built = spec_build_misleading_name_fork_chain(seed, warmup_length=3, dead_depth=dead_depth, true_tail=true_tail)
    write_candidates_dir(tmp_path, built["candidates"])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["expected_chain_end"]
    fork_row = [row for row in actual["rows"] if row[0] == built["fork_seq"]]
    assert len(fork_row) == 1
    assert fork_row[0][2] == built["winner_op"]


CONCURRENT_FORK_PARAMS = list(range(4))


@pytest.mark.parametrize("seed", CONCURRENT_FORK_PARAMS)
def test_concurrent_forks_independent_artifacts_do_not_cross_contaminate(tmp_path, seed):
    """Adjacent forks on different artifacts resolve without leaking state."""
    candidates, results, expected_end = spec_build_concurrent_independent_forks_chain(seed)
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == expected_end
    for artifact_id, info in results.items():
        assert actual["artifact_states"][artifact_id][0] == info["final_state"]
        assert actual["artifact_states"][artifact_id][1] == info["last_seq"]
        fork_row = [row for row in actual["rows"] if row[0] == info["fork_seq"] and row[1] == artifact_id]
        assert len(fork_row) == 1
        assert fork_row[0][2] == info["winner_op"]


PAYLOAD_RESTORE_PARAMS = [(seed, 2 + seed % 4, seed % 3) for seed in range(10)]


@pytest.mark.parametrize("index", range(len(PAYLOAD_RESTORE_PARAMS)))
def test_payload_binding_rejects_stale_content_after_restore(tmp_path, index):
    """Stale content after restore rejected."""
    _seed, n_cycles, artifact_pick = PAYLOAD_RESTORE_PARAMS[index]
    artifact_id = f"pb-{artifact_pick}"
    steps = []
    for cycle in range(n_cycles):
        steps.append((artifact_id, "STORE", f"payload{cycle}", 100 + cycle * 7))
        steps.append((artifact_id, "EVICT", f"payload{cycle}", 100 + cycle * 7))
    steps.append((artifact_id, "STORE", "final-content", 999))
    good_chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, good_chain)
    tip_before_bad = good_chain[-1]["chain_hash"]
    bad = make_entry(
        len(good_chain) + 1, artifact_id, "HIT", f"payload{max(n_cycles - 1, 0)}", 100,
        good_chain[-1]["ts"] + 5, tip_before_bad, fragment_name="stale_hit.csv", row_index=0,
    )
    write_fragment_csv(os.path.join(tmp_path, "stale_hit.csv"), [entry_row(bad)])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(good_chain)


PAYLOAD_ACCEPT_PARAMS = [(seed, 2 + seed % 5) for seed in range(10)]


@pytest.mark.parametrize("index", range(len(PAYLOAD_ACCEPT_PARAMS)))
def test_payload_binding_accepts_matching_latest_store(tmp_path, index):
    """Matching latest store accepted."""
    _seed, n_cycles = PAYLOAD_ACCEPT_PARAMS[index]
    artifact_id = "pb-accept"
    steps = []
    for cycle in range(n_cycles):
        steps.append((artifact_id, "STORE", f"gen{cycle}", 50 + cycle))
        steps.append((artifact_id, "HIT", f"gen{cycle}", 50 + cycle))
        steps.append((artifact_id, "EVICT", f"gen{cycle}", 50 + cycle))
    steps.append((artifact_id, "STORE", "latest", 777))
    steps.append((artifact_id, "HIT", "latest", 777))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)


PAYLOAD_SIZE_ONLY_MISMATCH_PARAMS = list(range(8))


@pytest.mark.parametrize("index", PAYLOAD_SIZE_ONLY_MISMATCH_PARAMS)
def test_payload_binding_checks_size_not_only_content(tmp_path, index):
    """Size mismatch alone rejected."""
    artifact_id = f"pb-size-{index}"
    steps = [
        (artifact_id, "STORE", "shared-hash", 100 + index),
        (artifact_id, "EVICT", "shared-hash", 100 + index),
        (artifact_id, "STORE", "shared-hash", 200 + index),
    ]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    tip = chain[-1]["chain_hash"]
    bad = make_entry(
        len(chain) + 1, artifact_id, "HIT", "shared-hash", 100 + index,
        chain[-1]["ts"] + 3, tip, fragment_name="size_mismatch.csv", row_index=0,
    )
    write_fragment_csv(os.path.join(tmp_path, "size_mismatch.csv"), [entry_row(bad)])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)


PAYLOAD_MULTI_ARTIFACT_PARAMS = [(seed, 2 + seed % 3) for seed in range(10)]


@pytest.mark.parametrize("index", range(len(PAYLOAD_MULTI_ARTIFACT_PARAMS)))
def test_payload_binding_interleaved_across_artifacts(tmp_path, index):
    """Interleaved artifacts bind independently."""
    seed, n_artifacts = PAYLOAD_MULTI_ARTIFACT_PARAMS[index]
    candidates, _genuine, _states = spec_build_interaction_chain(seed, 30, {}, n_artifacts=n_artifacts)
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 30


TS_EQUAL_PARAMS = list(range(8))


@pytest.mark.parametrize("index", TS_EQUAL_PARAMS)
def test_timestamp_equal_to_previous_is_allowed(tmp_path, index):
    """Equal timestamp allowed to continue."""
    artifact_id = f"ts-eq-{index}"
    steps = [
        (artifact_id, "STORE", "c1", 10, 1000),
        (artifact_id, "HIT", "c1", 10, 1000),
        (artifact_id, "EVICT", "c1", 10, 1001),
    ]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 3


TS_REGRESSION_PARAMS = list(range(10))


@pytest.mark.parametrize("index", TS_REGRESSION_PARAMS)
def test_timestamp_regression_truncates_chain(tmp_path, index):
    """Regressed timestamp truncates chain."""
    artifact_id = f"ts-reg-{index}"
    steps = [
        (artifact_id, "STORE", "c1", 10, 5000),
        (artifact_id, "HIT", "c1", 10, 5000 + index),
    ]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    tip = chain[-1]["chain_hash"]
    bad = make_entry(
        3, artifact_id, "EVICT", "c1", 10, 5000 + index - 1, tip,
        fragment_name="regressed.csv", row_index=0,
    )
    write_fragment_csv(os.path.join(tmp_path, "regressed.csv"), [entry_row(bad)])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 2


TS_DEEP_DECOY_PARAMS = [(seed, 3 + seed % 5) for seed in range(10)]


@pytest.mark.parametrize("index", range(len(TS_DEEP_DECOY_PARAMS)))
def test_timestamp_regression_behind_deep_decoy_subtree(tmp_path, index):
    """Regression buried in deep decoy."""
    seed, depth = TS_DEEP_DECOY_PARAMS[index]
    built = spec_build_fork_chain(
        seed, warmup_length=4, dead_end_depth=depth, true_tail_length=depth + 1, n_artifacts=1, dead_end_first=True
    )
    candidates = list(built["candidates"])
    dead_tail = sorted(
        (c for c in candidates if c["fragment_name"] == built["dead_branch_name"]),
        key=lambda c: c["row_index"],
    )
    last_dead = dead_tail[-1]
    extension = make_entry(
        last_dead["seq"] + 1, built["fork_owner"], "MISS", "", 0, last_dead["ts"] - 1, last_dead["chain_hash"],
        fragment_name=built["dead_branch_name"], row_index=last_dead["row_index"] + 1,
    )
    candidates.append(extension)
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["expected_chain_end"]


RERUN_STALE_PARAMS = [(seed, 3 + seed % 4) for seed in range(10)]


@pytest.mark.parametrize("index", range(len(RERUN_STALE_PARAMS)))
def test_storage_rerun_clears_stale_ledger_and_state(tmp_path, index):
    """Rerun clears prior stale rows."""
    seed, n_artifacts = RERUN_STALE_PARAMS[index]
    big_dir = tmp_path / "big"
    small_dir = tmp_path / "small"
    big_dir.mkdir()
    small_dir.mkdir()
    candidates, _genuine, _states = spec_build_interaction_chain(seed, 20, {}, n_artifacts=n_artifacts)
    write_candidates_dir(big_dir, candidates)
    scratch = agent_bridge.new_scratch_dir()
    db_path = os.path.join(scratch, "shared.db")
    summary_path = os.path.join(scratch, "shared_summary.json")
    first = run_pipeline_isolated_paths(big_dir, db_path, summary_path, timeout=30)
    assert first["chain_end"] == 20
    small_artifact = f"ia-{seed % max(n_artifacts, 1)}"
    small_chain = build_valid_chain([(small_artifact, "STORE", "only-content", 42)])
    write_candidates_dir(small_dir, small_chain)
    second = run_pipeline_isolated_paths(small_dir, db_path, summary_path, timeout=30)
    expected_second = spec_recover(str(small_dir))
    assert second["chain_end"] == expected_second["chain_end"] == 1
    assert second["recovered_seqs"] == [1]
    assert set(second["artifact_states"].keys()) == {small_artifact}
    assert second["counts"] == expected_second["counts"]


RERUN_STALE_SUMMARY_PARAMS = list(range(6))


@pytest.mark.parametrize("index", RERUN_STALE_SUMMARY_PARAMS)
def test_storage_rerun_summary_json_matches_shrunk_db(tmp_path, index):
    """Summary json also drops stale artifacts."""
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    steps_first = [(f"art-a-{index}", "STORE", "x", 5), (f"art-b-{index}", "STORE", "y", 6)]
    steps_second = [(f"art-c-{index}", "STORE", "z", 7)]
    write_candidates_dir(first_dir, build_valid_chain(steps_first))
    scratch = agent_bridge.new_scratch_dir()
    db_path = os.path.join(scratch, "reuse.db")
    summary_path = os.path.join(scratch, "reuse_summary.json")
    run_pipeline_isolated_paths(first_dir, db_path, summary_path, timeout=30)
    write_candidates_dir(second_dir, build_valid_chain(steps_second))
    second = run_pipeline_isolated_paths(second_dir, db_path, summary_path, timeout=30)
    assert second["chain_end"] == 1
    assert set(second["summary_json"]["artifact_states"].keys()) == {f"art-c-{index}"}
    assert second["summary_json"]["chain_end"] == 1
    assert second["recovered_seqs"] == [1]


RERUN_ARTIFACT_SET_CHANGE_PARAMS = list(range(6))


@pytest.mark.parametrize("index", RERUN_ARTIFACT_SET_CHANGE_PARAMS)
def test_storage_rerun_artifact_set_change_clears_stale_metric(tmp_path, index):
    """Rerun with a changed population updates distinct_artifacts."""
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    shared_artifact = f"asc-shared-{index}"
    dropped_a = f"asc-dropped-a-{index}"
    dropped_b = f"asc-dropped-b-{index}"
    new_artifact = f"asc-new-{index}"
    steps_first = [
        (shared_artifact, "STORE", "s1", 10),
        (dropped_a, "STORE", "d1", 11),
        (dropped_b, "STORE", "d2", 12),
    ]
    steps_second = [
        (shared_artifact, "STORE", "s2", 20),
        (new_artifact, "STORE", "n1", 21),
    ]
    write_candidates_dir(first_dir, build_valid_chain(steps_first))
    scratch = agent_bridge.new_scratch_dir()
    db_path = os.path.join(scratch, "reuse.db")
    summary_path = os.path.join(scratch, "reuse_summary.json")
    first = run_pipeline_isolated_paths(first_dir, db_path, summary_path, timeout=30)
    assert first["distinct_artifacts"] == 3
    write_candidates_dir(second_dir, build_valid_chain(steps_second))
    second = run_pipeline_isolated_paths(second_dir, db_path, summary_path, timeout=30)
    expected_second = spec_recover(str(second_dir))
    assert second["distinct_artifacts"] == expected_second["distinct_artifacts"] == 2
    assert second["summary_json"]["distinct_artifacts"] == 2
    assert set(second["artifact_states"].keys()) == {shared_artifact, new_artifact}
    assert second["chain_end"] == 2


ZERO_COUNT_PARAMS = [
    ("MISS",),
    ("HIT",),
    ("EVICT",),
    ("HIT", "EVICT"),
    ("MISS", "EVICT"),
    ("MISS", "HIT"),
    ("HIT", "MISS", "EVICT"),
]


@pytest.mark.parametrize("missing_ops", ZERO_COUNT_PARAMS)
def test_summary_json_reports_zero_for_absent_ops(tmp_path, missing_ops):
    """Absent ops still report zero."""
    keep_alive = "zc-keep-alive"
    misser = "zc-misser"
    present_plan = [(keep_alive, "STORE", "seed-content", 11)]
    if "HIT" not in missing_ops:
        present_plan.append((keep_alive, "HIT", "seed-content", 11))
    if "EVICT" not in missing_ops:
        present_plan.append((keep_alive, "EVICT", "seed-content", 11))
    if "MISS" not in missing_ops:
        present_plan.append((misser, "MISS"))
    chain = build_valid_chain(present_plan)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    for op in missing_ops:
        assert actual["counts"][op] == 0
        assert actual["summary_json"]["counts"][op] == 0
    assert set(actual["summary_json"]["counts"].keys()) == set(SPEC_OPS)


ZERO_COUNT_HIDDEN_PARAMS = list(range(6))


@pytest.mark.parametrize("index", ZERO_COUNT_HIDDEN_PARAMS)
def test_summary_counts_dict_has_all_four_keys_always(tmp_path, index):
    """All four op keys always present."""
    artifact_id = f"zc-full-{index}"
    steps = [(artifact_id, "STORE", "only", 5)]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert set(actual["counts"].keys()) == set(SPEC_OPS)
    for op in SPEC_OPS:
        assert actual["counts"][op] is not None


LARGE_RANDOM_PARAMS = [
    (1000 + i, 30 + (i * 7) % 60, 1 + i % 5, 2 + i % 4)
    for i in range(10)
]


@pytest.mark.parametrize("index", range(len(LARGE_RANDOM_PARAMS)))
def test_randomized_reconstruction_with_forks_and_decoys(tmp_path, index):
    """Randomized history with forks."""
    seed, length, n_artifacts, n_forks = LARGE_RANDOM_PARAMS[index]
    candidates, expected_end = spec_build_cascading_fork_chain(
        seed, n_forks=n_forks, warmup_length=length, dead_depth=2 + (seed % 3), true_tail=6 + (seed % 4),
        n_artifacts=n_artifacts, dead_first=(seed % 2 == 0),
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == expected_end


FIELD_SENSITIVITY_CASES = [
    ("artifact_id", "wrong-artifact"),
    ("content_hash", "wrong-hash"),
    ("size_bytes", 999999),
    ("ts", 1),
]


@pytest.mark.parametrize("field,decoy_value", FIELD_SENSITIVITY_CASES)
def test_hash_preimage_field_sensitivity(tmp_path, field, decoy_value):
    """Mutated field without rehash rejected."""
    artifact_id = "hp-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "c1", 50), (artifact_id, "HIT", "c1", 50)])
    genuine = chain[0]
    mutated = dict(genuine, fragment_name="field_decoy.csv", row_index=0)
    mutated[field] = decoy_value
    write_candidates_dir(tmp_path, [mutated, chain[1]])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 0


VALID_TRANSITION_PAIRS = [
    ("UNKNOWN", "MISS"),
    ("UNKNOWN", "STORE"),
    ("STORED", "HIT"),
    ("STORED", "EVICT"),
    ("EVICTED", "MISS"),
    ("EVICTED", "STORE"),
]


@pytest.mark.parametrize("index,state_op", list(enumerate(VALID_TRANSITION_PAIRS)))
def test_state_machine_valid_transition_reaches_expected_state(tmp_path, index, state_op):
    """Valid transition reaches expected state."""
    state, op = state_op
    artifact_id = f"sm-valid-{index}"
    steps = []
    if state == "STORED":
        steps.append((artifact_id, "STORE", "seed", 10))
    elif state == "EVICTED":
        steps.append((artifact_id, "STORE", "seed", 10))
        steps.append((artifact_id, "EVICT", "seed", 10))
    if op in PAYLOAD_BOUND_OPS:
        steps.append((artifact_id, op, "seed", 10))
    else:
        steps.append((artifact_id, op))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)
    expected_state = SPEC_TRANSITIONS[(state, op)]
    assert actual["artifact_states"][artifact_id][0] == expected_state


INVALID_TRANSITION_PAIRS = [
    ("UNKNOWN", "HIT"),
    ("UNKNOWN", "EVICT"),
    ("STORED", "STORE"),
    ("EVICTED", "HIT"),
    ("EVICTED", "EVICT"),
]


@pytest.mark.parametrize("state,op", INVALID_TRANSITION_PAIRS)
def test_state_machine_invalid_transition_truncates_chain(tmp_path, state, op):
    """Invalid transition truncates chain."""
    artifact_id = "sm-invalid"
    steps = []
    if state == "STORED":
        steps.append((artifact_id, "STORE", "seed", 10))
    elif state == "EVICTED":
        steps.append((artifact_id, "STORE", "seed", 10))
        steps.append((artifact_id, "EVICT", "seed", 10))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    tip = chain[-1]["chain_hash"] if chain else GENESIS_HASH
    ts = (chain[-1]["ts"] + 1) if chain else 1000
    bad_content, bad_size = ("bad", 5) if op in PAYLOAD_BOUND_OPS else ("", 0)
    bad = make_entry(len(chain) + 1, artifact_id, op, bad_content, bad_size, ts, tip, fragment_name="bad.csv", row_index=0)
    write_fragment_csv(os.path.join(tmp_path, "bad.csv"), [entry_row(bad)])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)


MULTI_STEP_SEQUENCES = [
    ["STORE", "HIT", "HIT", "EVICT", "STORE", "HIT"],
    ["STORE", "EVICT", "STORE", "EVICT", "STORE"],
    ["STORE", "HIT", "EVICT", "MISS", "STORE"],
    ["STORE", "EVICT", "MISS", "MISS", "STORE", "EVICT"],
    ["STORE", "HIT", "EVICT", "STORE", "HIT", "EVICT", "STORE"],
]


@pytest.mark.parametrize("index", range(len(MULTI_STEP_SEQUENCES)))
def test_state_machine_multi_step_walk_reaches_final_state(tmp_path, index):
    """Multi step walk reaches final state."""
    ops = MULTI_STEP_SEQUENCES[index]
    artifact_id = f"sm-walk-{index}"
    steps = []
    state = "UNKNOWN"
    for i, op in enumerate(ops):
        if op == "STORE":
            steps.append((artifact_id, "STORE", f"payload{i}", 10 + i))
        elif op in PAYLOAD_BOUND_OPS:
            content_hash, size_bytes = steps[-1][2], steps[-1][3]
            for prior in reversed(steps):
                if prior[1] == "STORE":
                    content_hash, size_bytes = prior[2], prior[3]
                    break
            steps.append((artifact_id, op, content_hash, size_bytes))
        else:
            steps.append((artifact_id, op))
        state = spec_apply(state, op)
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)
    assert actual["artifact_states"][artifact_id][0] == state


EVICTED_STORE_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", EVICTED_STORE_SEEDS)
def test_evicted_artifact_can_be_restored_via_store(tmp_path, seed):
    """Evicted artifact restored via store."""
    artifact_id = f"restore-{seed}"
    steps = [
        (artifact_id, "STORE", "gen1", 10),
        (artifact_id, "EVICT", "gen1", 10),
        (artifact_id, "STORE", "gen2", 20 + seed),
        (artifact_id, "HIT", "gen2", 20 + seed),
    ]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 4
    assert actual["artifact_states"][artifact_id][0] == "STORED"


REFRESH_BINDING_CASES = [
    ("stored_match", "STORED", True, True),
    ("stored_mismatch", "STORED", False, False),
    ("evicted_match", "EVICTED", True, True),
    ("evicted_mismatch", "EVICTED", False, False),
    ("unknown_invalid", "UNKNOWN", True, False),
]


@pytest.mark.parametrize("case_id,prior_state,payload_matches,expect_accept", REFRESH_BINDING_CASES)
def test_refresh_transition_and_payload_binding(tmp_path, case_id, prior_state, payload_matches, expect_accept):
    """Refresh only continues with a matching payload from stored or evicted."""
    artifact_id = f"refresh-{case_id}"
    steps = []
    if prior_state in ("STORED", "EVICTED"):
        steps.append((artifact_id, "STORE", "genX", 30))
    if prior_state == "EVICTED":
        steps.append((artifact_id, "EVICT", "genX", 30))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    tip = chain[-1]["chain_hash"] if chain else GENESIS_HASH
    ts = (chain[-1]["ts"] + 1) if chain else 1000
    content_hash = "genX" if payload_matches else "wrong-genX"
    refresh_row = make_entry(
        len(chain) + 1, artifact_id, "REFRESH", content_hash, 30, ts, tip,
        fragment_name="refresh_probe.csv", row_index=0,
    )
    write_fragment_csv(os.path.join(tmp_path, "refresh_probe.csv"), [entry_row(refresh_row)])
    actual, _expected = assert_matches_spec(tmp_path)
    if expect_accept:
        assert actual["chain_end"] == len(chain) + 1
        assert actual["artifact_states"][artifact_id][0] == prior_state
    else:
        assert actual["chain_end"] == len(chain)


REFRESH_SEQUENCE_CASES = [
    (["STORE", "REFRESH", "REFRESH", "REFRESH", "HIT"], "STORED"),
    (["STORE", "EVICT", "REFRESH", "REFRESH", "MISS"], "EVICTED"),
    (["STORE", "REFRESH", "EVICT", "REFRESH", "STORE"], "STORED"),
    (["STORE", "EVICT", "REFRESH", "STORE", "REFRESH", "HIT"], "STORED"),
    (["STORE", "REFRESH", "HIT", "EVICT", "REFRESH", "REFRESH"], "EVICTED"),
]


@pytest.mark.parametrize("index", range(len(REFRESH_SEQUENCE_CASES)))
def test_refresh_interleaved_multi_step_walk(tmp_path, index):
    """Refresh interleaves with other lifecycle ops without breaking the walk."""
    ops, final_state = REFRESH_SEQUENCE_CASES[index]
    artifact_id = f"refresh-walk-{index}"
    steps = []
    payload = ("", 0)
    for i, op in enumerate(ops):
        if op == "STORE":
            payload = (f"walkpayload{index}", 40 + index)
            steps.append((artifact_id, "STORE", payload[0], payload[1]))
        elif op in PAYLOAD_BOUND_OPS:
            steps.append((artifact_id, op, payload[0], payload[1]))
        else:
            steps.append((artifact_id, op))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)
    assert actual["artifact_states"][artifact_id][0] == final_state


REFRESH_LAST_SEQ_SEEDS = list(range(4))


@pytest.mark.parametrize("seed", REFRESH_LAST_SEQ_SEEDS)
def test_refresh_last_seq_tracking(tmp_path, seed):
    """Last seq reflects a trailing refresh, not an earlier touch."""
    artifact_id = f"refresh-lastseq-{seed}"
    other_id = f"refresh-lastseq-other-{seed}"
    steps = [
        (artifact_id, "STORE", "keep", 50 + seed),
        (artifact_id, "EVICT", "keep", 50 + seed),
        (other_id, "STORE", "noise", 5),
        (artifact_id, "REFRESH", "keep", 50 + seed),
        (other_id, "HIT", "noise", 5),
        (artifact_id, "REFRESH", "keep", 50 + seed),
    ]
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == len(chain)
    refresh_seq = chain[-1]["seq"]
    assert actual["artifact_states"][artifact_id][1] == refresh_seq


def _nonempty_subsets(items):
    subsets = []
    n = len(items)
    for mask in range(1, 1 << n):
        subsets.append(tuple(items[i] for i in range(n) if mask & (1 << i)))
    return subsets


def _trimmed_decoy_subsets(items):
    all_subsets = _nonempty_subsets(items)
    singles = [s for s in all_subsets if len(s) == 1]
    pairs = [s for s in all_subsets if len(s) == 2]
    triples = [s for s in all_subsets if len(s) == 3]
    return singles + pairs + triples[:1]


NONEMPTY_DECOY_SUBSETS = _trimmed_decoy_subsets(list(DECOY_KINDS))


@pytest.mark.parametrize("index,subset", list(enumerate(NONEMPTY_DECOY_SUBSETS)))
def test_decoy_subset_single_position_matrix(tmp_path, index, subset):
    """Decoy subset never wins position."""
    length = 20
    position = 10
    decoy_map = {position: list(subset)}
    candidates, genuine, _states = spec_build_interaction_chain(
        7000 + index, length, decoy_map, n_artifacts=1, protect_reach=length - position
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length
    assert actual["recovered_seqs"] == [g["seq"] for g in genuine]


DECOY_LARGE_SUBSET_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", DECOY_LARGE_SUBSET_SEEDS)
def test_decoy_full_subset_repeat_matrix(tmp_path, seed):
    """All decoy kinds together rejected."""
    length = 22
    position = 11
    decoy_map = {position: list(DECOY_KINDS)}
    candidates, _genuine, _states = spec_build_interaction_chain(
        8000 + seed, length, decoy_map, n_artifacts=2, protect_reach=length - position
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length


DECOY_CASCADING_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", DECOY_CASCADING_SEEDS)
def test_decoy_cascading_multi_position_matrix(tmp_path, seed):
    """Decoys at several positions rejected."""
    length = 30
    positions = [8, 16, 24]
    kinds_cycle = list(DECOY_KINDS)
    decoy_map = {
        positions[0]: kinds_cycle[seed % 5 : seed % 5 + 2] or [kinds_cycle[0]],
        positions[1]: kinds_cycle[(seed + 1) % 5 : (seed + 1) % 5 + 2] or [kinds_cycle[1]],
        positions[2]: kinds_cycle[(seed + 2) % 5 : (seed + 2) % 5 + 2] or [kinds_cycle[2]],
    }
    candidates, _genuine, _states = spec_build_interaction_chain(
        9000 + seed, length, decoy_map, n_artifacts=2, protect_reach=6
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length


DECOY_MULTI_ARTIFACT_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", DECOY_MULTI_ARTIFACT_SEEDS)
def test_decoy_multi_artifact_matrix(tmp_path, seed):
    """Decoys across artifacts rejected."""
    length = 26
    decoy_map = {6: ["hash_corrupt", "wrong_artifact_id"], 14: ["state_invalid", "later_valid_tiebreak"], 20: ["wrong_prev_hash"]}
    candidates, _genuine, _states = spec_build_interaction_chain(
        10000 + seed, length, decoy_map, n_artifacts=4, protect_reach=6
    )
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length


DISTINCT_ARTIFACT_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", DISTINCT_ARTIFACT_SEEDS)
def test_distinct_artifacts_excludes_rejected_candidates(tmp_path, seed):
    """Distinct artifact count ignores rejected ghost candidates."""
    length = 24
    n_artifacts = 3
    decoy_map = {
        5: ["wrong_artifact_id"],
        11: ["wrong_artifact_id", "hash_corrupt"],
        17: ["wrong_artifact_id"],
    }
    candidates, _genuine, _states = spec_build_interaction_chain(
        90000 + seed, length, decoy_map, n_artifacts=n_artifacts, protect_reach=4
    )
    write_candidates_dir(tmp_path, candidates)
    actual, expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length
    assert actual["distinct_artifacts"] == expected["distinct_artifacts"] == n_artifacts
    raw_candidate_artifacts = {c["artifact_id"] for c in candidates}
    assert len(raw_candidate_artifacts) > n_artifacts


LEXICOGRAPHIC_TRAP_CASES = [(kind, index) for kind in LEXICOGRAPHIC_TRAP_KINDS for index in range(3)]


@pytest.mark.parametrize("kind,index", LEXICOGRAPHIC_TRAP_CASES)
def test_lexicographic_trap_single_position(tmp_path, kind, index):
    """Plain string tie-break required."""
    first_name, last_name = lexicographic_trap_names(kind, index)
    artifact_id = f"lex-{kind}-{index}"
    tip = GENESIS_HASH
    warmup = make_entry(1, artifact_id, "STORE", "seed", 10, 1000, tip, fragment_name="warm.csv", row_index=0)
    tip = warmup["chain_hash"]
    winner = make_entry(2, artifact_id, "HIT", "seed", 10, 1001, tip, fragment_name=first_name, row_index=0)
    loser = make_entry(2, artifact_id, "EVICT", "seed", 10, 1001, tip, fragment_name=last_name, row_index=0)
    candidates = [warmup, winner, loser]
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 2
    assert actual["rows"][-1][2] == winner["op"]


LEXICOGRAPHIC_CASCADE_SEEDS = list(range(10))


@pytest.mark.parametrize("index", range(len(LEXICOGRAPHIC_CASCADE_SEEDS)))
def test_lexicographic_trap_cascading_positions(tmp_path, index):
    """Cascaded trap names resolved correctly."""
    seed = LEXICOGRAPHIC_CASCADE_SEEDS[index]
    kind = LEXICOGRAPHIC_TRAP_KINDS[seed % len(LEXICOGRAPHIC_TRAP_KINDS)]
    artifact_id = f"lexcasc-{seed}"
    tip = GENESIS_HASH
    candidates = []
    seq = 1
    candidates.append(make_entry(seq, artifact_id, "STORE", "seed", 10, 1000, tip, fragment_name="warm.csv", row_index=0))
    tip = candidates[-1]["chain_hash"]
    for step in range(3):
        seq += 1
        first_name, last_name = lexicographic_trap_names(kind, step)
        winner = make_entry(seq, artifact_id, "HIT", "seed", 10, 1000 + seq, tip, fragment_name=first_name, row_index=step)
        loser = make_entry(seq, artifact_id, "EVICT", "seed", 10, 1000 + seq, tip, fragment_name=last_name, row_index=step)
        candidates.append(winner)
        candidates.append(loser)
        tip = winner["chain_hash"]
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == seq


MALFORMED_ROW_KINDS = ["short_row", "long_row", "non_numeric_seq", "non_numeric_size", "non_numeric_ts"]


@pytest.mark.parametrize("kind", MALFORMED_ROW_KINDS)
def test_malformed_row_skipped_chain_still_completes(tmp_path, kind):
    """Malformed row skipped not fatal."""
    artifact_id = "mf-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10), (artifact_id, "EVICT", "a", 10)])
    rows = [entry_row(e) for e in chain]
    if kind == "short_row":
        bad_row = ["1", "x"]
    elif kind == "long_row":
        bad_row = ["1", "x", "y", "z", "1", "1", "0" * 64, "0" * 64, "extra"]
    elif kind == "non_numeric_seq":
        bad_row = ["notanumber", "x", "STORE", "", "0", "1", "0" * 64, "0" * 64]
    elif kind == "non_numeric_size":
        bad_row = ["1", "x", "STORE", "", "notanumber", "1", "0" * 64, "0" * 64]
    else:
        bad_row = ["1", "x", "STORE", "", "0", "notanumber", "0" * 64, "0" * 64]
    all_rows = [rows[0], bad_row, rows[1], rows[2]]
    path = os.path.join(tmp_path, "malformed.csv")
    write_fragment_csv(path, all_rows)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 3


MALFORMED_POSITIONS = ["start", "middle", "end"]


@pytest.mark.parametrize("position", MALFORMED_POSITIONS)
def test_malformed_row_position_never_breaks_valid_rows(tmp_path, position):
    """Malformed row position irrelevant."""
    artifact_id = "mf-pos"
    chain = build_valid_chain(
        [(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10), (artifact_id, "EVICT", "a", 10), (artifact_id, "MISS",)]
    )
    rows = [entry_row(e) for e in chain]
    bad_row = ["77", "bad", "STORE", "x"]
    if position == "start":
        all_rows = [bad_row, *rows]
    elif position == "middle":
        all_rows = [rows[0], rows[1], bad_row, rows[2], rows[3]]
    else:
        all_rows = [*rows, bad_row]
    write_fragment_csv(os.path.join(tmp_path, "malformed.csv"), all_rows)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 4


def test_malformed_row_at_large_scale_still_completes(tmp_path):
    """Large scale malformed rows tolerated."""
    candidates, _genuine, _states = spec_build_interaction_chain(31415, 60, {}, n_artifacts=3)
    write_candidates_dir(tmp_path, candidates)
    path = os.path.join(tmp_path, "main.csv")
    with open(path, "a", newline="") as fh:
        fh.write("999,bad,STORE,x\r\n")
        fh.write("998,bad2,STORE,x,y,z,w\r\n")
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 60


COMPOUND_MALFORMED_FILES = [
    ("short_row", "non_numeric_seq"),
    ("long_row", "non_numeric_size"),
    ("short_row", "long_row", "non_numeric_ts"),
    ("non_numeric_seq", "non_numeric_size", "non_numeric_ts"),
]


@pytest.mark.parametrize("kinds", COMPOUND_MALFORMED_FILES)
def test_compound_malformed_rows_all_valid_rows_survive(tmp_path, kinds):
    """Several malformation kinds coexist safely."""
    artifact_id = "cm-artifact"
    chain = build_valid_chain(
        [(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10), (artifact_id, "EVICT", "a", 10)]
    )
    rows = [entry_row(e) for e in chain]
    bad_lookup = {
        "short_row": ["1", "x"],
        "long_row": ["1", "x", "y", "z", "1", "1", "0" * 64, "0" * 64, "extra"],
        "non_numeric_seq": ["notanumber", "x", "STORE", "", "0", "1", "0" * 64, "0" * 64],
        "non_numeric_size": ["1", "x", "STORE", "", "notanumber", "1", "0" * 64, "0" * 64],
        "non_numeric_ts": ["1", "x", "STORE", "", "0", "notanumber", "0" * 64, "0" * 64],
    }
    all_rows = [rows[0]]
    for kind in kinds:
        all_rows.append(bad_lookup[kind])
    all_rows.extend(rows[1:])
    write_fragment_csv(os.path.join(tmp_path, "compound.csv"), all_rows)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 3


COMPOUND_MULTI_FILE_SEEDS = list(range(11000, 11008))


@pytest.mark.parametrize("seed", COMPOUND_MULTI_FILE_SEEDS)
def test_compound_malformed_rows_across_multiple_files(tmp_path, seed):
    """Malformed rows spread across files."""
    candidates, _genuine, _states = spec_build_interaction_chain(seed, 24, {}, n_artifacts=2)
    n_files = 3
    by_file = {i: [] for i in range(n_files)}
    for i, cand in enumerate(candidates):
        by_file[i % n_files].append(cand)
    for i, rows in by_file.items():
        path = os.path.join(tmp_path, f"multi_{i}.csv")
        write_fragment_csv(path, [entry_row(r) for r in rows])
        with open(path, "a", newline="") as fh:
            fh.write(f"{900 + i},bad,STORE,x\r\n")
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 24


BAD_HEADERS = [
    "seq,artifact,op,content_hash,size_bytes,ts,prev_hash,chain_hash",
    "seq,artifact_id,op,content_hash,size_bytes,ts,prev_hash",
    "SEQ,ARTIFACT_ID,OP,CONTENT_HASH,SIZE_BYTES,TS,PREV_HASH,CHAIN_HASH",
]


@pytest.mark.parametrize("bad_header", BAD_HEADERS)
def test_bad_header_voids_only_that_file(tmp_path, bad_header):
    """Bad header voids only its file."""
    artifact_id = "bh-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10)])
    good_path = os.path.join(tmp_path, "good.csv")
    write_fragment_csv(good_path, [entry_row(chain[0])])
    bad_path = os.path.join(tmp_path, "bad_header.csv")
    with open(bad_path, "w", newline="") as fh:
        fh.write(bad_header + "\r\n")
        fh.write("2,other,STORE,x,1,1000,0,0\r\n")
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 1


IGNORED_FILE_NAMES = ["notes.txt", "readme.md", "backup.csv.bak", "config.json"]


@pytest.mark.parametrize("ignored_name", IGNORED_FILE_NAMES)
def test_ignored_non_csv_files_do_not_interfere(tmp_path, ignored_name):
    """Non csv files are ignored."""
    artifact_id = "ig-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10)])
    write_candidates_dir(tmp_path, chain)
    with open(os.path.join(tmp_path, ignored_name), "w") as fh:
        fh.write("not,csv,data,at,all\n")
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 2


def test_bom_and_header_only_files_are_harmless(tmp_path):
    """BOM and empty files harmless; a BOM'd header must not be silently normalized to a match."""
    artifact_id = "bom-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10)])
    write_fragment_csv(os.path.join(tmp_path, "good.csv"), [entry_row(chain[0])])
    bom_path = os.path.join(tmp_path, "bom.csv")
    with open(bom_path, "wb") as fh:
        fh.write(b"\xef\xbb\xbf" + CSV_HEADER.encode() + b"\r\n")
    with open(bom_path, "a", newline="") as fh:
        csv.writer(fh).writerow(entry_row(chain[1]))
    with open(os.path.join(tmp_path, "header_only.csv"), "w") as fh:
        fh.write(CSV_HEADER + "\r\n")
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 1


GLOBAL_TRUNCATION_CASES = [
    (seed, 2 + seed % 4, 4 + seed % 6, seed % 3, "state_invalid" if seed % 2 == 0 else "wrong_prev_hash")
    for seed in range(10)
]


@pytest.mark.parametrize("index", range(len(GLOBAL_TRUNCATION_CASES)))
def test_global_truncation_stops_every_artifact(tmp_path, index):
    """One fault truncates whole chain."""
    seed, n_artifacts, tail_length, owner_index, fault_kind = GLOBAL_TRUNCATION_CASES[index]
    built = spec_build_global_truncation_chain(seed, n_artifacts, tail_length, owner_index, fault_kind)
    write_candidates_dir(tmp_path, built["candidates"])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["chain_end"]
    actual_states = {a: v[0] for a, v in actual["artifact_states"].items()}
    assert actual_states == built["final_states"]


GLOBAL_TRUNCATION_PAYLOAD_CASES = [(seed, 3 + seed % 3, 5 + seed % 5, 0) for seed in range(6)]


@pytest.mark.parametrize("index", range(len(GLOBAL_TRUNCATION_PAYLOAD_CASES)))
def test_global_truncation_payload_fault_stops_chain(tmp_path, index):
    """Payload fault truncates whole chain."""
    seed, n_artifacts, tail_length, owner_index = GLOBAL_TRUNCATION_PAYLOAD_CASES[index]
    built = spec_build_global_truncation_chain(seed, n_artifacts, tail_length, owner_index, "payload_mismatch")
    write_candidates_dir(tmp_path, built["candidates"])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == built["chain_end"]


ALL_FOLD_SEQUENCES = list(range(10))


@pytest.mark.parametrize("index", ALL_FOLD_SEQUENCES)
def test_compute_artifact_states_orders_by_seq_not_fragment_name(tmp_path, index):
    """Fold order follows seq not name."""
    candidates, _genuine, _states = spec_build_interaction_chain(20000 + index, 18, {}, n_artifacts=3)
    for cand in candidates:
        cand["fragment_name"] = "".join(reversed(f"{cand['seq']:03d}_{cand['fragment_name']}"))
    write_candidates_dir(tmp_path, candidates)
    actual, expected = assert_matches_spec(tmp_path)
    actual_states = dict(actual["artifact_states"])
    for artifact_id in expected["final_states"]:
        assert actual_states[artifact_id][0] == expected["final_states"][artifact_id]


FOLD_LAST_SEQ_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", FOLD_LAST_SEQ_SEEDS)
def test_artifact_state_last_seq_matches_true_last_touch(tmp_path, seed):
    """Last seq matches true last touch."""
    candidates, genuine, _states = spec_build_interaction_chain(30000 + seed, 22, {}, n_artifacts=3)
    write_candidates_dir(tmp_path, candidates)
    actual, _expected = assert_matches_spec(tmp_path)
    last_touch = {}
    for g in genuine:
        last_touch[g["artifact_id"]] = g["seq"]
    for artifact_id, seq in last_touch.items():
        assert actual["artifact_states"][artifact_id][1] == seq


SCHEMA_EXPECTATIONS = [
    ("ledger", {"seq", "artifact_id", "op", "content_hash", "size_bytes", "ts", "prev_hash", "chain_hash"}),
    ("artifact_state", {"artifact_id", "state", "last_seq"}),
    ("summary", {"metric", "value"}),
]


@pytest.mark.parametrize("table,expected_columns", SCHEMA_EXPECTATIONS)
def test_storage_schema_has_expected_columns(bundled_pipeline, table, expected_columns):
    """Schema columns match documented set."""
    conn = sqlite3.connect(bundled_pipeline["db_path"])
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    conn.close()
    assert columns == expected_columns


ROUND_TRIP_SIZES = [0, 1, 3, 12, 50]


@pytest.mark.parametrize("index", range(len(ROUND_TRIP_SIZES)))
def test_storage_round_trip_full_fidelity(tmp_path, index):
    """Round trip preserves every field."""
    size = ROUND_TRIP_SIZES[index]
    artifact_id = f"rt-{index}"
    steps = []
    for i in range(size):
        steps.append((artifact_id, "STORE", f"c{i}", 100 + i))
        steps.append((artifact_id, "EVICT", f"c{i}", 100 + i))
    chain = build_valid_chain(steps)
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert len(actual["rows"]) == len(chain)
    for row, entry in zip(actual["rows"], chain):
        assert row[1] == entry["artifact_id"]
        assert row[3] == entry["content_hash"]
        assert row[4] == entry["size_bytes"]
        assert row[5] == entry["ts"]
        assert row[6] == entry["prev_hash"]
        assert row[7] == entry["chain_hash"]


@pytest.mark.parametrize("n", [0, 1])
def test_storage_handles_edge_sizes(tmp_path, n):
    """Handles empty and single row."""
    if n == 0:
        os.makedirs(tmp_path, exist_ok=True)
    else:
        chain = build_valid_chain([("edge-artifact", "STORE", "c", 5)])
        write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == n


def test_storage_artifact_state_covers_exactly_recovered_artifacts(tmp_path):
    """State table covers only recovered artifacts."""
    candidates, _genuine, _states = spec_build_interaction_chain(40000, 25, {}, n_artifacts=4)
    write_candidates_dir(tmp_path, candidates)
    actual, expected = assert_matches_spec(tmp_path)
    assert set(actual["artifact_states"].keys()) == set(expected["final_states"].keys())


@pytest.mark.parametrize("size_bytes", [1 << 40, 1 << 62, (1 << 63) - 1])
def test_storage_handles_large_size_bytes(tmp_path, size_bytes):
    """Large size bytes preserved exactly."""
    chain = build_valid_chain([("large-artifact", "STORE", "c1", size_bytes)])
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["rows"][0][4] == size_bytes


@pytest.mark.parametrize("content_hash", ["", "cafe-hash-unicode", "x" * 4000], ids=["empty", "unicode", "long"])
def test_storage_content_hash_survives_unicode_and_empty(tmp_path, content_hash):
    """Content hash preserved exactly always."""
    chain = build_valid_chain([("uni-artifact", "STORE", content_hash, 5)])
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["rows"][0][3] == content_hash


def test_summary_json_exposes_only_documented_keys(bundled_pipeline):
    """Summary json has documented keys."""
    assert set(bundled_pipeline["summary_json"].keys()) == {
        "counts", "chain_end", "distinct_artifacts", "artifact_states",
    }
    assert set(bundled_pipeline["summary_json"]["counts"].keys()) == set(SPEC_OPS)


SYNTHETIC_GENESIS_LENGTHS = [1, 5, 12, 30]


@pytest.mark.parametrize("index", range(len(SYNTHETIC_GENESIS_LENGTHS)))
def test_synthetic_chains_anchor_to_fixed_genesis(tmp_path, index):
    """Chain anchors to fixed genesis."""
    length = SYNTHETIC_GENESIS_LENGTHS[index]
    steps = [(f"gx-{i}", "STORE", f"c{i}", i) for i in range(length)]
    chain = build_valid_chain(steps)
    assert chain[0]["prev_hash"] == GENESIS_HASH
    write_candidates_dir(tmp_path, chain)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == length


WRONG_GENESIS_ANCHORS = [
    "1" * 64,
    hashlib.sha256(b"wrong").hexdigest(),
    "0" * 63 + "1",
]


@pytest.mark.parametrize("wrong_hash", WRONG_GENESIS_ANCHORS)
def test_wrong_genesis_anchor_rejected(tmp_path, wrong_hash):
    """Wrong genesis anchor rejected fully."""
    entry = make_entry(1, "wg-artifact", "STORE", "c", 5, 1000, wrong_hash, fragment_name="w.csv", row_index=0)
    write_candidates_dir(tmp_path, [entry])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 0


@pytest.mark.parametrize("op", SPEC_OPS)
def test_bundled_dataset_summary_counts(bundled_pipeline, op):
    """Bundled dataset op counts correct."""
    expected = spec_recover(BUNDLED_DATA_DIR)
    assert bundled_pipeline["counts"][op] == expected["counts"][op]


BUNDLED_EXPECTED = spec_recover(BUNDLED_DATA_DIR)


@pytest.mark.parametrize("artifact_id", sorted(BUNDLED_EXPECTED["final_states"]))
def test_bundled_dataset_artifact_state_and_last_seq(bundled_pipeline, artifact_id):
    """Bundled dataset artifact state correct."""
    assert bundled_pipeline["artifact_states"][artifact_id][0] == BUNDLED_EXPECTED["final_states"][artifact_id]


def test_bundled_dataset_full_continuity_and_consistency(bundled_pipeline):
    """Bundled dataset chain fully continuous."""
    assert bundled_pipeline["chain_end"] == BUNDLED_EXPECTED["chain_end"]
    assert bundled_pipeline["recovered_seqs"] == BUNDLED_EXPECTED["recovered_seqs"]
    assert bundled_pipeline["recovered_seqs"] == list(range(1, bundled_pipeline["chain_end"] + 1))
    conn = sqlite3.connect(bundled_pipeline["db_path"])
    rows = conn.execute("SELECT seq, prev_hash, chain_hash FROM ledger ORDER BY seq").fetchall()
    conn.close()
    tip = GENESIS_HASH
    for seq, prev_hash, chain_hash in rows:
        assert prev_hash == tip
        tip = chain_hash


def test_bundled_dataset_rerun_is_fully_deterministic():
    """Bundled dataset rerun deterministic."""
    first = run_pipeline_isolated(BUNDLED_DATA_DIR)
    second = run_pipeline_isolated(BUNDLED_DATA_DIR)
    assert first["chain_end"] == second["chain_end"]
    assert first["recovered_seqs"] == second["recovered_seqs"]
    assert first["counts"] == second["counts"]


@pytest.mark.parametrize("name", HIDDEN_DATASET_NAMES)
def test_hidden_dataset_anchors_to_fixed_genesis(name):
    """Hidden dataset anchors to genesis."""
    directory = os.path.join(HOLDOUT_DIR, name)
    expected = spec_recover(directory)
    actual = run_pipeline_isolated(directory)
    assert actual["chain_end"] == expected["chain_end"]


@pytest.mark.parametrize("name", HIDDEN_DATASET_NAMES)
def test_hidden_dataset_matches_expected_json(name):
    """Hidden dataset matches golden json."""
    directory = os.path.join(HOLDOUT_DIR, name)
    with open(os.path.join(directory, "expected.json")) as fh:
        golden = json.load(fh)
    respec = spec_recover(directory)
    assert respec["chain_end"] == golden["chain_end"]
    assert respec["counts"] == golden["counts"]
    assert respec["recovered_seqs"] == golden["recovered_seqs"]
    assert respec["final_states"] == golden["final_states"]
    assert respec["distinct_artifacts"] == golden["distinct_artifacts"]
    actual = run_pipeline_isolated(directory)
    assert actual["chain_end"] == golden["chain_end"]
    assert actual["recovered_seqs"] == golden["recovered_seqs"]
    assert actual["distinct_artifacts"] == golden["distinct_artifacts"]


@pytest.mark.parametrize("name", HIDDEN_DATASET_NAMES)
def test_hidden_dataset_summary_json_matches_db_and_expected(name):
    """Hidden dataset summary matches db."""
    directory = os.path.join(HOLDOUT_DIR, name)
    with open(os.path.join(directory, "expected.json")) as fh:
        golden = json.load(fh)
    actual = run_pipeline_isolated(directory)
    assert actual["chain_end"] == golden["chain_end"]
    assert actual["counts"] == golden["counts"]
    assert actual["recovered_seqs"] == golden["recovered_seqs"]
    assert actual["distinct_artifacts"] == golden["distinct_artifacts"]
    actual_states = {a: v[0] for a, v in actual["artifact_states"].items()}
    assert actual_states == golden["final_states"]
    assert actual["summary_json"]["chain_end"] == golden["chain_end"]
    assert actual["summary_json"]["distinct_artifacts"] == golden["distinct_artifacts"]
    assert set(actual["summary_json"]["artifact_states"].keys()) == set(golden["final_states"].keys())


HIDDEN_DATASET_ARTIFACTS = []
for _name in HIDDEN_DATASET_NAMES:
    with open(os.path.join(HOLDOUT_DIR, _name, "expected.json")) as _fh:
        _golden = json.load(_fh)
    _first_artifact = min(_golden["final_states"])
    HIDDEN_DATASET_ARTIFACTS.append((_name, _first_artifact))


@pytest.mark.parametrize("name,artifact_id", HIDDEN_DATASET_ARTIFACTS)
def test_hidden_dataset_artifact_state_matches_recovery(name, artifact_id):
    """Hidden dataset per artifact state matches."""
    directory = os.path.join(HOLDOUT_DIR, name)
    with open(os.path.join(directory, "expected.json")) as fh:
        golden = json.load(fh)
    actual = run_pipeline_isolated(directory)
    actual_states = {a: v[0] for a, v in actual["artifact_states"].items()}
    assert actual_states[artifact_id] == golden["final_states"][artifact_id]


def _copy_dataset(src_name, dst_dir):
    src_dir = os.path.join(HOLDOUT_DIR, src_name)
    for name in os.listdir(src_dir):
        if name == "expected.json":
            continue
        with open(os.path.join(src_dir, name), newline="") as fh:
            content = fh.read()
        with open(os.path.join(dst_dir, name), "w", newline="") as fh:
            fh.write(content)


def _load_rows_from_dir(directory, fname):
    path = os.path.join(directory, fname)
    with open(path, newline="") as fh:
        lines = fh.readlines()
    return path, lines


PERTURB_CONTENT_HASH_CASES = []
for _name in HIDDEN_DATASET_NAMES[:4]:
    _directory = os.path.join(HOLDOUT_DIR, _name)
    _files = sorted(f for f in os.listdir(_directory) if f.endswith(".csv"))
    _chosen = None
    for _f in _files:
        with open(os.path.join(_directory, _f), newline="") as _fh:
            _flines = _fh.readlines()
        for _line in _flines[1:]:
            _parts = _line.strip().split(",")
            if len(_parts) == 8 and _parts[0] == "1":
                _chosen = _f
                break
        if _chosen:
            break
    if _chosen:
        PERTURB_CONTENT_HASH_CASES.append((_name, _chosen))


@pytest.mark.parametrize("dataset,fragment_file", PERTURB_CONTENT_HASH_CASES)
def test_perturbation_content_hash_corrupt_truncates(tmp_path, dataset, fragment_file):
    """Perturbed content hash re-derived truncation."""
    _copy_dataset(dataset, tmp_path)
    path = os.path.join(tmp_path, fragment_file)
    with open(path, newline="") as fh:
        lines = fh.readlines()
    target_line = None
    for i in range(1, len(lines)):
        parts = lines[i].strip().split(",")
        if len(parts) == 8 and parts[0].isdigit() and int(parts[0]) == 1:
            target_line = i
            break
    assert target_line is not None
    parts = lines[target_line].rstrip("\r\n").split(",")
    parts[7] = hashlib.sha256((parts[7] + "TAMPER").encode()).hexdigest()
    lines[target_line] = ",".join(parts) + "\r\n"
    with open(path, "w", newline="") as fh:
        fh.writelines(lines)
    expected = spec_recover(str(tmp_path))
    actual = run_pipeline_isolated(tmp_path)
    assert actual["chain_end"] == expected["chain_end"] == 0


PERTURB_PREV_HASH_CASES = list(HIDDEN_DATASET_NAMES[:4])


@pytest.mark.parametrize("dataset", PERTURB_PREV_HASH_CASES)
def test_perturbation_prev_hash_corrupt_truncates(tmp_path, dataset):
    """Perturbed prev hash re-derived truncation."""
    _copy_dataset(dataset, tmp_path)
    candidates = spec_load_fragments(str(tmp_path))
    seq1_candidates = [c for c in candidates if c["seq"] == 1]
    target = min(seq1_candidates, key=lambda c: (c["fragment_name"], c["row_index"]))
    path = os.path.join(tmp_path, target["fragment_name"])
    with open(path, newline="") as fh:
        lines = fh.readlines()
    for i in range(1, len(lines)):
        parts = lines[i].rstrip("\r\n").split(",")
        if len(parts) == 8 and parts[0] == "1" and parts[6] == target["prev_hash"]:
            parts[6] = hashlib.sha256((parts[6] + "TAMPER").encode()).hexdigest()
            lines[i] = ",".join(parts) + "\r\n"
            break
    with open(path, "w", newline="") as fh:
        fh.writelines(lines)
    expected = spec_recover(str(tmp_path))
    actual = run_pipeline_isolated(tmp_path)
    assert actual["chain_end"] == expected["chain_end"] == 0


PERTURB_GAP_CASES = list(HIDDEN_DATASET_NAMES)


@pytest.mark.parametrize("dataset", PERTURB_GAP_CASES)
def test_perturbation_gap_removal_truncates_at_expected_seq(tmp_path, dataset):
    """Removing a row re-derives truncation."""
    _copy_dataset(dataset, tmp_path)
    original = spec_recover(os.path.join(HOLDOUT_DIR, dataset))
    if original["chain_end"] < 3:
        pytest.skip("chain too short for this dataset")
    target_seq = original["chain_end"] // 2
    for fname in sorted(os.listdir(tmp_path)):
        path = os.path.join(tmp_path, fname)
        with open(path, newline="") as fh:
            lines = fh.readlines()
        kept = [lines[0]]
        removed = False
        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) == 8 and parts[0].isdigit() and int(parts[0]) == target_seq:
                removed = True
                continue
            kept.append(line)
        if removed:
            with open(path, "w", newline="") as fh:
                fh.writelines(kept)
    expected = spec_recover(str(tmp_path))
    actual = run_pipeline_isolated(tmp_path)
    assert actual["chain_end"] == expected["chain_end"]
    assert expected["chain_end"] < original["chain_end"]


PERTURB_DUPLICATE_CASES = list(HIDDEN_DATASET_NAMES)


@pytest.mark.parametrize("dataset", PERTURB_DUPLICATE_CASES)
def test_perturbation_harmless_duplicate_injection(tmp_path, dataset):
    """Injected exact duplicate stays harmless."""
    _copy_dataset(dataset, tmp_path)
    original = spec_recover(os.path.join(HOLDOUT_DIR, dataset))
    files = sorted(f for f in os.listdir(tmp_path) if f.endswith(".csv"))
    target_path = os.path.join(tmp_path, files[0])
    with open(target_path, newline="") as fh:
        lines = fh.readlines()
    if len(lines) > 1:
        with open(target_path, "a", newline="") as fh:
            fh.write(lines[1])
    expected = spec_recover(str(tmp_path))
    actual = run_pipeline_isolated(tmp_path)
    assert actual["chain_end"] == expected["chain_end"] == original["chain_end"]


ORDER_INDEPENDENT_SEEDS = list(range(8))


@pytest.mark.parametrize("index", ORDER_INDEPENDENT_SEEDS)
def test_build_chain_result_independent_of_csv_row_order(tmp_path, index):
    """Row order in file irrelevant."""
    candidates, _genuine, _states = spec_build_interaction_chain(50000 + index, 20, {6: ["hash_corrupt"], 14: ["state_invalid"]}, n_artifacts=2, protect_reach=5)
    import random as _random
    shuffled = list(candidates)
    _random.Random(index).shuffle(shuffled)
    write_candidates_dir(tmp_path, shuffled)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 20


METAMORPHIC_SEEDS = list(range(10))


@pytest.mark.parametrize("index", METAMORPHIC_SEEDS)
def test_metamorphic_shuffled_rows_never_change_recovery(tmp_path, index):
    """Shuffling rows never changes result."""
    import random as _random
    candidates, _genuine, _states = spec_build_interaction_chain(60000 + index, 22, {5: ["wrong_prev_hash"], 15: ["later_valid_tiebreak"]}, n_artifacts=3, protect_reach=4)
    baseline_dir = tmp_path / "baseline"
    shuffled_dir = tmp_path / "shuffled"
    baseline_dir.mkdir()
    shuffled_dir.mkdir()
    write_candidates_dir(baseline_dir, candidates)
    shuffled = list(candidates)
    _random.Random(1000 + index).shuffle(shuffled)
    write_candidates_dir(shuffled_dir, shuffled)
    baseline = run_pipeline_isolated(baseline_dir)
    shuffled_result = run_pipeline_isolated(shuffled_dir)
    assert baseline["chain_end"] == shuffled_result["chain_end"]
    assert baseline["recovered_seqs"] == shuffled_result["recovered_seqs"]
    assert baseline["counts"] == shuffled_result["counts"]


FILE_SPLIT_COUNTS = [1, 2, 3, 5, 8]


@pytest.mark.parametrize("n_files", FILE_SPLIT_COUNTS)
def test_fragment_file_split_invariance(tmp_path, n_files):
    """Splitting into files never changes result."""
    candidates, _genuine, _states = spec_build_interaction_chain(70000 + n_files, 24, {8: ["hash_corrupt", "state_invalid"]}, n_artifacts=2, protect_reach=4)
    import random as _random
    rng = _random.Random(n_files)
    by_file = {i: [] for i in range(n_files)}
    for cand in candidates:
        by_file[rng.randrange(n_files)].append(dict(cand, fragment_name=f"split_{rng.randrange(n_files)}.csv"))
    flat = [c for rows in by_file.values() for c in rows]
    write_candidates_dir(tmp_path, flat)
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 24


DUPLICATE_NAME_PAIRS = [("a.csv", "b.csv"), ("fragment_x.csv", "fragment_y.csv"), ("main1.csv", "main2.csv")]


@pytest.mark.parametrize("first_name,second_name", DUPLICATE_NAME_PAIRS)
def test_duplicate_identical_content_resolves_deterministically(tmp_path, first_name, second_name):
    """Exact duplicate rows resolve deterministically."""
    artifact_id = "dup-artifact"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10)])
    write_fragment_csv(os.path.join(tmp_path, first_name), [entry_row(e) for e in chain])
    write_fragment_csv(os.path.join(tmp_path, second_name), [entry_row(chain[0])])
    actual, _expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == 2


ROW_INDEX_ORDER_SEEDS = [501, 502, 503, 504, 505]


@pytest.mark.parametrize("seed", ROW_INDEX_ORDER_SEEDS)
def test_row_index_ordering_within_single_file_is_stable(tmp_path, seed):
    """Row index tie-break stable within file."""
    artifact_id = f"riord-{seed}"
    chain = build_valid_chain([(artifact_id, "STORE", "a", 10), (artifact_id, "HIT", "a", 10), (artifact_id, "EVICT", "a", 10)])
    write_candidates_dir(tmp_path, chain)
    actual1, _ = assert_matches_spec(tmp_path)
    actual2, _ = assert_matches_spec(tmp_path)
    assert actual1["recovered_seqs"] == actual2["recovered_seqs"]


STRUCTURAL_RENAME_SEEDS = list(range(6))


@pytest.mark.parametrize("index", STRUCTURAL_RENAME_SEEDS)
def test_structural_rename_flips_tie_break_winner(tmp_path, index):
    """Renaming files flips tie break winner."""
    built = spec_build_fork_chain(
        80000 + index, warmup_length=3, dead_end_depth=3, true_tail_length=3, n_artifacts=1, dead_end_first=True
    )
    write_candidates_dir(tmp_path, built["candidates"])
    actual_before, _expected_before = assert_matches_spec(tmp_path)
    row_before = next(row for row in actual_before["rows"] if row[0] == built["fork_seq"])
    assert row_before[2] == built["dead_branch_op"]

    flipped_dir = tmp_path / "flipped"
    flipped_dir.mkdir()
    flipped_candidates = []
    for cand in built["candidates"]:
        new_cand = dict(cand)
        if cand["fragment_name"] == built["dead_branch_name"]:
            new_cand["fragment_name"] = built["dead_branch_name"].replace("aaa_", "zzz_").replace("zzy_", "aab_")
        elif cand["fragment_name"] == built["true_branch_name"]:
            new_cand["fragment_name"] = built["true_branch_name"].replace("zzz_", "aaa_").replace("aab_", "zzy_")
        flipped_candidates.append(new_cand)
    write_candidates_dir(flipped_dir, flipped_candidates)
    actual_after = run_pipeline_isolated(flipped_dir)
    expected_after = spec_recover(str(flipped_dir))
    assert actual_after["chain_end"] == expected_after["chain_end"] == built["expected_chain_end"]
    row_after = next(row for row in actual_after["rows"] if row[0] == built["fork_seq"])
    assert row_after[2] == built["true_branch_op"]
    assert row_after[2] != row_before[2]


STATE_LEAK_SEEDS = list(range(6))


@pytest.mark.parametrize("seed", STATE_LEAK_SEEDS)
def test_repeated_isolated_runs_do_not_leak_state(tmp_path, seed):
    """Repeated isolated runs stay independent."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    chain_a = build_valid_chain([(f"leak-a-{seed}", "STORE", "content-a", 11)])
    chain_b = build_valid_chain([(f"leak-b-{seed}", "STORE", "content-b", 22), (f"leak-b-{seed}", "HIT", "content-b", 22)])
    write_candidates_dir(dir_a, chain_a)
    write_candidates_dir(dir_b, chain_b)
    result_a = run_pipeline_isolated(dir_a)
    result_b = run_pipeline_isolated(dir_b)
    assert result_a["chain_end"] == 1
    assert result_b["chain_end"] == 2
    assert set(result_a["artifact_states"].keys()) == {f"leak-a-{seed}"}
    assert set(result_b["artifact_states"].keys()) == {f"leak-b-{seed}"}


@pytest.mark.parametrize("length", [6001, 12000])
def test_long_chain_has_no_recursion_ceiling(tmp_path, length):
    chain = build_valid_chain([(f"long-{i}", "STORE") for i in range(length)])
    write_candidates_dir(tmp_path, chain)
    actual, expected = assert_matches_spec(tmp_path)
    assert actual["chain_end"] == expected["chain_end"] == length
    assert actual["rows"] == [tuple(entry_row(row)) for row in chain]


@pytest.mark.skipif(os.geteuid() != 0, reason="Requires verifier UID isolation")
def test_candidate_uid_cannot_read_verifier_or_reference():
    import subprocess

    private_paths = [
        "/tests/test_outputs.py",
        "/tests/conftest.py",
        os.path.join(HOLDOUT_DIR, HIDDEN_DATASET_NAMES[0], "expected.json"),
    ]
    for directory in ("/solution", "/oracle", "/app/solution", "/app/oracle"):
        if os.path.isdir(directory):
            private_paths.append(directory)
    probe = '''
import os, sys
for path in sys.argv[1:]:
    try:
        if os.path.isdir(path):
            os.listdir(path)
        else:
            with open(path, "rb") as fh:
                fh.read(1)
    except PermissionError:
        continue
    raise SystemExit("Candidate can access protected path: " + path)
'''
    result = subprocess.run(
        [*agent_bridge.DROP_PRIVILEGES, "python3", "-I", "-c", probe, *private_paths],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
