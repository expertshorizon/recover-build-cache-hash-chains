import csv
import hashlib
import os
import random

GENESIS_PREV_HASH = "0" * 64
GENESIS_PREIMAGE = f"0|GENESIS|GENESIS|{''}|{0}|{0}|{GENESIS_PREV_HASH}"
GENESIS_HASH = hashlib.sha256(GENESIS_PREIMAGE.encode("utf-8")).hexdigest()

HOLDOUT_DIR = "/tests/holdout"

CSV_COLUMNS = (
    "seq",
    "artifact_id",
    "op",
    "content_hash",
    "size_bytes",
    "ts",
    "prev_hash",
    "chain_hash",
)

SPEC_TRANSITIONS = {
    ("UNKNOWN", "MISS"): "UNKNOWN",
    ("UNKNOWN", "STORE"): "STORED",
    ("STORED", "HIT"): "STORED",
    ("STORED", "EVICT"): "EVICTED",
    ("STORED", "REFRESH"): "STORED",
    ("EVICTED", "MISS"): "EVICTED",
    ("EVICTED", "STORE"): "STORED",
    ("EVICTED", "REFRESH"): "EVICTED",
}
SPEC_STATES = ("UNKNOWN", "STORED", "EVICTED")
SPEC_OPS = ("STORE", "HIT", "MISS", "EVICT", "REFRESH")
PAYLOAD_BOUND_OPS = ("HIT", "EVICT", "REFRESH")
INVALID_OP_FOR_STATE = {"UNKNOWN": "HIT", "STORED": "STORE", "EVICTED": "HIT"}
VALID_OPS_FROM_STATE = {
    "UNKNOWN": ("STORE", "MISS"),
    "STORED": ("HIT", "EVICT"),
    "EVICTED": ("MISS", "STORE"),
}
FULL_VALID_OPS_FROM_STATE = {
    "UNKNOWN": ("STORE", "MISS"),
    "STORED": ("HIT", "EVICT", "REFRESH"),
    "EVICTED": ("MISS", "STORE", "REFRESH"),
}


def spec_apply(state, op):
    return SPEC_TRANSITIONS.get((state, op), state)


def spec_validate_transition(state, op):
    if (state, op) not in SPEC_TRANSITIONS:
        return False, state
    return True, SPEC_TRANSITIONS[(state, op)]


def spec_validate_payload(op, content_hash, size_bytes, last_store):
    if op not in PAYLOAD_BOUND_OPS:
        return True
    if last_store is None:
        return False
    return last_store == (content_hash, size_bytes)


def spec_other_valid_op(state, op):
    options = [candidate for candidate in FULL_VALID_OPS_FROM_STATE[state] if candidate != op]
    return options[0] if options else None


def spec_serialize(seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash):
    return f"{seq}|{artifact_id}|{op}|{content_hash}|{size_bytes}|{ts}|{prev_hash}"


def spec_hash(seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash):
    preimage = spec_serialize(seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash)
    return hashlib.sha256(preimage.encode("utf-8")).hexdigest()


def make_entry(
    seq,
    artifact_id,
    op,
    content_hash="",
    size_bytes=0,
    ts=0,
    prev_hash="",
    fragment_name="f.csv",
    row_index=0,
):
    chain_hash = spec_hash(seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash)
    return {
        "seq": seq,
        "artifact_id": artifact_id,
        "op": op,
        "content_hash": content_hash,
        "size_bytes": size_bytes,
        "ts": ts,
        "prev_hash": prev_hash,
        "chain_hash": chain_hash,
        "fragment_name": fragment_name,
        "row_index": row_index,
    }


def _auto_payload(op, seq, last_store, artifact_id, store_tag):
    if op == "STORE":
        return f"{store_tag}{seq}", 10 + seq
    if op in PAYLOAD_BOUND_OPS:
        return last_store[artifact_id]
    return "", 0


def build_valid_chain(steps, start_hash=GENESIS_HASH, fragment_name="f.csv", ts_start=1000, ts_step=1, store_tag="c"):
    entries = []
    tip = start_hash
    last_store = {}
    ts = ts_start
    for idx, step in enumerate(steps, start=1):
        if len(step) == 2:
            artifact_id, op = step
            content_hash, size_bytes = _auto_payload(op, idx, last_store, artifact_id, store_tag)
            entry_ts = ts
        elif len(step) == 4:
            artifact_id, op, content_hash, size_bytes = step
            entry_ts = ts
        else:
            artifact_id, op, content_hash, size_bytes, entry_ts = step
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        entry = make_entry(
            idx, artifact_id, op, content_hash, size_bytes, entry_ts, tip,
            fragment_name=fragment_name, row_index=idx,
        )
        entries.append(entry)
        tip = entry["chain_hash"]
        ts = entry_ts + ts_step
    return entries


def entry_row(entry):
    return [entry[column] for column in CSV_COLUMNS]


def write_fragment_csv(path, rows):
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(list(CSV_COLUMNS))
        for row in rows:
            writer.writerow(row)


def write_candidates_dir(directory, candidates):
    directory = str(directory)
    os.makedirs(directory, exist_ok=True)
    by_file = {}
    for candidate in candidates:
        by_file.setdefault(candidate["fragment_name"], []).append(candidate)
    for name, rows in by_file.items():
        ordered = sorted(rows, key=lambda c: c["row_index"])
        write_fragment_csv(os.path.join(directory, name), [entry_row(c) for c in ordered])
    return directory


def spec_load_fragments(directory):
    entries = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".csv"):
            continue
        with open(os.path.join(directory, name), newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header is None or tuple(cell.strip() for cell in header) != CSV_COLUMNS:
                continue
            for row_index, row in enumerate(reader):
                values = [cell.strip() for cell in row]
                if tuple(values) == CSV_COLUMNS:
                    continue
                if len(values) != len(CSV_COLUMNS):
                    continue
                try:
                    entries.append(
                        {
                            "seq": int(values[0]),
                            "artifact_id": values[1],
                            "op": values[2],
                            "content_hash": values[3],
                            "size_bytes": int(values[4]),
                            "ts": int(values[5]),
                            "prev_hash": values[6],
                            "chain_hash": values[7],
                            "fragment_name": name,
                            "row_index": row_index,
                        }
                    )
                except ValueError:
                    continue
    entries.sort(key=lambda entry: (entry["seq"], entry["fragment_name"], entry["row_index"]))
    return entries


def _spec_admit(candidate, tip_hash, artifact_states, last_store, last_ts):
    if candidate["prev_hash"] != tip_hash:
        return None
    recomputed = spec_hash(
        candidate["seq"],
        candidate["artifact_id"],
        candidate["op"],
        candidate["content_hash"],
        candidate["size_bytes"],
        candidate["ts"],
        candidate["prev_hash"],
    )
    if recomputed != candidate["chain_hash"]:
        return None
    if last_ts is not None and candidate["ts"] < last_ts:
        return None
    current_state = artifact_states.get(candidate["artifact_id"], "UNKNOWN")
    is_valid, new_state = spec_validate_transition(current_state, candidate["op"])
    if not is_valid:
        return None
    last_store_entry = last_store.get(candidate["artifact_id"])
    if not spec_validate_payload(candidate["op"], candidate["content_hash"], candidate["size_bytes"], last_store_entry):
        return None
    return new_state


def spec_build_chain(candidates):
    successors = {}
    for row in candidates:
        successors.setdefault((row["seq"], row["prev_hash"]), []).append(row)
    for rows in successors.values():
        rows.sort(key=lambda row: (row["fragment_name"], row["row_index"]))
    states, stores = {}, {}
    path, best = [], []
    events = [("enter", row) for row in reversed(successors.get((1, GENESIS_HASH), []))]
    while events:
        kind, data = events.pop()
        if kind == "leave":
            if len(path) > len(best):
                best = list(path)
            artifact, previous_state, previous_store = data
            for mapping, previous in ((states, previous_state), (stores, previous_store)):
                if previous is None:
                    mapping.pop(artifact, None)
                else:
                    mapping[artifact] = previous
            path.pop()
            continue
        row = data
        tip = path[-1]["chain_hash"] if path else GENESIS_HASH
        ts = path[-1]["ts"] if path else None
        state = _spec_admit(row, tip, states, stores, ts)
        if state is None:
            continue
        artifact = row["artifact_id"]
        events.append(("leave", (artifact, states.get(artifact), stores.get(artifact))))
        states[artifact] = state
        if row["op"] == "STORE":
            stores[artifact] = (row["content_hash"], row["size_bytes"])
        path.append(row)
        children = successors.get((row["seq"] + 1, row["chain_hash"]), [])
        events.extend(("enter", child) for child in reversed(children))
    return best, len(best)


def spec_recover(directory):
    candidates = spec_load_fragments(directory)
    recovered, chain_end = spec_build_chain(candidates)
    counts = dict.fromkeys(SPEC_OPS, 0)
    for entry in recovered:
        counts[entry["op"]] += 1
    states = {}
    for entry in recovered:
        current_state = states.get(entry["artifact_id"], "UNKNOWN")
        _, new_state = spec_validate_transition(current_state, entry["op"])
        states[entry["artifact_id"]] = new_state
    distinct_artifacts = len({entry["artifact_id"] for entry in recovered})
    return {
        "chain_end": chain_end,
        "counts": counts,
        "recovered_seqs": [entry["seq"] for entry in recovered],
        "final_states": states,
        "distinct_artifacts": distinct_artifacts,
    }


DECOY_KINDS = (
    "hash_corrupt",
    "state_invalid",
    "wrong_prev_hash",
    "later_valid_tiebreak",
    "wrong_artifact_id",
)


def _spec_protect_tail(decoy, length, base_ts, name_stem):
    if length <= 0:
        return []
    extras = []
    tip = decoy["chain_hash"]
    ts = (base_ts if base_ts is not None else decoy["ts"]) + 1
    artifact_id = f"{decoy['artifact_id']}-protect"
    for step in range(length):
        content_hash = f"pt{step}"
        entry = make_entry(
            decoy["seq"] + 1 + step, artifact_id, "STORE", content_hash, 12, ts + step, tip,
            fragment_name=f"{name_stem}_tail{step}.csv", row_index=0,
        )
        extras.append(entry)
        tip = entry["chain_hash"]
    return extras


def _spec_build_decoy(kind, kind_index, genuine, prior_state, tip, ts, genuine_fragment_name, protect_reach=0, protect_ts=None):
    seq = genuine["seq"]
    artifact_id = genuine["artifact_id"]
    stem = genuine_fragment_name.removesuffix(".csv")
    if kind == "hash_corrupt":
        decoy = dict(genuine, fragment_name=f"aaa_{stem}_hc{seq}_{kind_index}.csv", row_index=900 + kind_index)
        decoy["chain_hash"] = hashlib.sha256((genuine["chain_hash"] + "X").encode()).hexdigest()
        extras = _spec_protect_tail(decoy, protect_reach, protect_ts, f"aaa_{stem}_hc{seq}_{kind_index}")
        return [decoy, *extras]
    if kind == "state_invalid":
        bad_op = INVALID_OP_FOR_STATE[prior_state]
        bad_content = f"bad{seq}" if bad_op == "STORE" else ""
        bad_size = 77 if bad_op == "STORE" else 0
        decoy = make_entry(
            seq, artifact_id, bad_op, bad_content, bad_size, ts, tip,
            fragment_name=f"aaa_{stem}_si{seq}_{kind_index}.csv", row_index=900 + kind_index,
        )
        extras = _spec_protect_tail(decoy, protect_reach, protect_ts, f"aaa_{stem}_si{seq}_{kind_index}")
        return [decoy, *extras]
    if kind == "wrong_prev_hash":
        wrong_prev = hashlib.sha256((tip + "WRONG").encode()).hexdigest()
        decoy = make_entry(
            seq, artifact_id, genuine["op"], genuine["content_hash"], genuine["size_bytes"], ts, wrong_prev,
            fragment_name=f"{stem}_wp{seq}_{kind_index}.csv", row_index=900 + kind_index,
        )
        return [decoy]
    if kind == "later_valid_tiebreak":
        alt_op = spec_other_valid_op(prior_state, genuine["op"])
        alt_content, alt_size = ("", 0)
        if alt_op == "STORE":
            alt_content, alt_size = f"alt{seq}", 55
        elif alt_op in PAYLOAD_BOUND_OPS:
            store_key = genuine.get("_last_store_for_decoy")
            if store_key is not None:
                alt_content, alt_size = store_key
        later_name = f"{stem}_zzzlater{seq}_{kind_index}.csv"
        decoy = make_entry(
            seq, artifact_id, alt_op, alt_content, alt_size, ts, tip,
            fragment_name=later_name, row_index=900 + kind_index,
        )
        return [decoy]
    if kind == "wrong_artifact_id":
        ghost_id = f"{artifact_id}-ghost{kind_index}"
        ghost_name = f"{stem}_zzzghost{seq}_{kind_index}.csv"
        decoy = make_entry(
            seq, ghost_id, "STORE", f"ghost{seq}", 11, ts, tip,
            fragment_name=ghost_name, row_index=900 + kind_index,
        )
        return [decoy]
    raise ValueError(kind)


def spec_build_interaction_chain(seed, length, decoy_seq_kinds, n_artifacts=1, genuine_fragment_name="main.csv", protect_reach=0):
    rng = random.Random(seed)
    artifacts = [f"ia-{i}" for i in range(n_artifacts)] if n_artifacts > 1 else ["ia-solo"]
    states = {artifact_id: "UNKNOWN" for artifact_id in artifacts}
    last_store = {}
    tip = GENESIS_HASH
    candidates = []
    genuine_entries = []
    for seq in range(1, length + 1):
        artifact_id = rng.choice(artifacts)
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
        content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "ic")
        ts = 5_000_000 + seq
        genuine = make_entry(
            seq, artifact_id, op, content_hash, size_bytes, ts, tip,
            fragment_name=genuine_fragment_name, row_index=seq,
        )
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        genuine["_last_store_for_decoy"] = last_store.get(artifact_id)
        candidates.append(genuine)
        genuine_entries.append(genuine)
        remaining = length - seq
        tail_protect = min(protect_reach, remaining) if remaining > 0 else 0
        for kind_index, kind in enumerate(decoy_seq_kinds.get(seq, ())):
            candidates.extend(
                _spec_build_decoy(
                    kind, kind_index, genuine, states[artifact_id], tip, ts, genuine_fragment_name,
                    protect_reach=tail_protect, protect_ts=ts,
                )
            )
        states[artifact_id] = spec_apply(states[artifact_id], op)
        tip = genuine["chain_hash"]
    return candidates, genuine_entries, states


def spec_generate_history(seed, length=25, n_artifacts=3, n_files=3, decoy_seqs=()):
    rng = random.Random(seed)
    artifacts = [f"gen-{i}" for i in range(n_artifacts)]
    states = {artifact_id: "UNKNOWN" for artifact_id in artifacts}
    last_store = {}
    tip = GENESIS_HASH
    entries = []
    decoys = []
    for seq in range(1, length + 1):
        artifact_id = rng.choice(artifacts)
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
        content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "c")
        ts = 3_000_000 + seq
        entry = make_entry(seq, artifact_id, op, content_hash, size_bytes, ts, tip)
        entries.append(entry)
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        if seq in decoy_seqs:
            if seq % 2 == 0:
                decoy = dict(entry, fragment_name="decoy.csv", row_index=999)
                decoy["chain_hash"] = hashlib.sha256((entry["chain_hash"] + "D").encode()).hexdigest()
            else:
                bad_op = INVALID_OP_FOR_STATE[states[artifact_id]]
                bad_content = f"bad{seq}" if bad_op == "STORE" else ""
                bad_size = 42 if bad_op == "STORE" else 0
                decoy = make_entry(seq, artifact_id, bad_op, bad_content, bad_size, ts, tip, fragment_name="decoy.csv", row_index=999)
            decoys.append(decoy)
        states[artifact_id] = spec_apply(states[artifact_id], op)
        tip = entry["chain_hash"]
    files = {i: [] for i in range(n_files)}
    for entry in entries:
        files[rng.randrange(n_files)].append(entry)
    for decoy in decoys:
        files[rng.randrange(n_files)].append(decoy)
    counts = dict.fromkeys(SPEC_OPS, 0)
    for entry in entries:
        counts[entry["op"]] += 1
    return {
        "entries": entries,
        "files": files,
        "states": states,
        "counts": counts,
        "length": length,
    }


LEXICOGRAPHIC_TRAP_KINDS = ("digit_length", "case", "punctuation")


def lexicographic_trap_names(kind, index):
    if kind == "digit_length":
        candidates = [f"frag_{index}.csv", f"frag_1{index}.csv"]
    elif kind == "case":
        candidates = [f"Frag_{index}.csv", f"frag_{index}.csv"]
    elif kind == "punctuation":
        candidates = [f"frag-{index}.csv", f"frag0{index}.csv", f"frag_{index}.csv"]
    else:
        raise ValueError(kind)
    ordered = sorted(candidates)
    return ordered[0], ordered[-1]


def spec_build_global_truncation_chain(seed, n_artifacts, tail_length, owner_index, fault_kind):
    rng = random.Random(seed)
    artifacts = [f"gt-{i}" for i in range(n_artifacts)]
    states = dict.fromkeys(artifacts, "UNKNOWN")
    own_tip = dict.fromkeys(artifacts, GENESIS_HASH)
    last_store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 1
    warmup = rng.randint(3 * n_artifacts, 5 * n_artifacts)
    for _ in range(warmup):
        artifact_id = rng.choice(artifacts)
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
        content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "w")
        ts = 6_000_000 + seq
        entry = make_entry(
            seq, artifact_id, op, content_hash, size_bytes, ts, tip,
            fragment_name="gt_main.csv", row_index=seq,
        )
        candidates.append(entry)
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        states[artifact_id] = spec_apply(states[artifact_id], op)
        own_tip[artifact_id] = entry["chain_hash"]
        tip = entry["chain_hash"]
        seq += 1
    recovered_entries = list(candidates)
    participated = {entry["artifact_id"] for entry in recovered_entries}
    true_final_states = {artifact_id: state for artifact_id, state in states.items() if artifact_id in participated}
    truncate_seq = seq
    owner = artifacts[owner_index % n_artifacts]
    if fault_kind == "state_invalid":
        bad_op = INVALID_OP_FOR_STATE[states[owner]]
        bad_content = f"bad{truncate_seq}" if bad_op == "STORE" else ""
        bad_size = 9 if bad_op == "STORE" else 0
        candidates.append(
            make_entry(
                truncate_seq, owner, bad_op, bad_content, bad_size, 6_000_000 + truncate_seq, tip,
                fragment_name="gt_owner.csv", row_index=0,
            )
        )
    elif fault_kind == "payload_mismatch" and states[owner] == "STORED":
        candidates.append(
            make_entry(
                truncate_seq, owner, "HIT", "wrong-content", 999, 6_000_000 + truncate_seq, tip,
                fragment_name="gt_owner.csv", row_index=0,
            )
        )
    else:
        bad_prev = hashlib.sha256((tip + "GT_WRONG").encode()).hexdigest()
        candidates.append(
            make_entry(
                truncate_seq, owner, "STORE", f"gtbad{truncate_seq}", 7, 6_000_000 + truncate_seq, bad_prev,
                fragment_name="gt_owner.csv", row_index=0,
            )
        )
    for i, artifact_id in enumerate(artifacts):
        if artifact_id == owner:
            continue
        art_seq = truncate_seq + 1
        art_tip = own_tip[artifact_id]
        art_state = states[artifact_id]
        art_last_store = dict(last_store) if artifact_id in last_store else {}
        for step in range(tail_length):
            op = rng.choice(FULL_VALID_OPS_FROM_STATE[art_state])
            content_hash, size_bytes = _auto_payload(op, art_seq, art_last_store, artifact_id, f"tail{i}_")
            entry = make_entry(
                art_seq, artifact_id, op, content_hash, size_bytes, 6_000_000 + art_seq, art_tip,
                fragment_name=f"gt_tail_{i}.csv", row_index=step,
            )
            candidates.append(entry)
            if op == "STORE":
                art_last_store[artifact_id] = (content_hash, size_bytes)
            art_state = spec_apply(art_state, op)
            art_tip = entry["chain_hash"]
            art_seq += 1
    counts = dict.fromkeys(SPEC_OPS, 0)
    for entry in recovered_entries:
        counts[entry["op"]] += 1
    return {
        "candidates": candidates,
        "chain_end": truncate_seq - 1,
        "final_states": true_final_states,
        "counts": counts,
        "owner": owner,
        "artifacts": artifacts,
    }


def spec_build_fork_chain(seed, warmup_length, dead_end_depth, true_tail_length, n_artifacts=1, dead_end_first=True):
    rng = random.Random(seed)
    artifacts = [f"fk-{i}" for i in range(n_artifacts)] if n_artifacts > 1 else ["fk-solo"]
    states = dict.fromkeys(artifacts, "UNKNOWN")
    last_store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 1
    for _ in range(warmup_length):
        artifact_id = rng.choice(artifacts)
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
        content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "w")
        entry = make_entry(
            seq, artifact_id, op, content_hash, size_bytes, 7_000_000 + seq, tip,
            fragment_name="main.csv", row_index=seq,
        )
        candidates.append(entry)
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        states[artifact_id] = spec_apply(states[artifact_id], op)
        tip = entry["chain_hash"]
        seq += 1
    fork_owner = rng.choice(artifacts)
    fork_state = states[fork_owner]
    op_a, op_b = VALID_OPS_FROM_STATE[fork_state]
    dead_name = "aaa_dead_branch.csv" if dead_end_first else "zzz_dead_branch.csv"
    true_name = "zzz_true_branch.csv" if dead_end_first else "aaa_true_branch.csv"
    fork_seq = seq
    fork_ts = 7_000_000 + fork_seq
    dead_store = dict(last_store)
    true_store = dict(last_store)
    dead_content, dead_size = _auto_payload(op_a, fork_seq, dead_store, fork_owner, "d")
    dead_entry = make_entry(
        fork_seq, fork_owner, op_a, dead_content, dead_size, fork_ts, tip,
        fragment_name=dead_name, row_index=0,
    )
    if op_a == "STORE":
        dead_store[fork_owner] = (dead_content, dead_size)
    true_content, true_size = _auto_payload(op_b, fork_seq, true_store, fork_owner, "t")
    true_entry = make_entry(
        fork_seq, fork_owner, op_b, true_content, true_size, fork_ts, tip,
        fragment_name=true_name, row_index=0,
    )
    if op_b == "STORE":
        true_store[fork_owner] = (true_content, true_size)
    candidates.append(dead_entry)
    candidates.append(true_entry)
    dead_state = spec_apply(fork_state, op_a)
    true_state = spec_apply(fork_state, op_b)
    dead_tip = dead_entry["chain_hash"]
    dead_ts = fork_ts
    dead_seq = fork_seq
    for step in range(dead_end_depth):
        dead_seq += 1
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[dead_state])
        content_hash, size_bytes = _auto_payload(op, dead_seq, dead_store, fork_owner, "d")
        dead_ts += 1
        entry = make_entry(
            dead_seq, fork_owner, op, content_hash, size_bytes, dead_ts, dead_tip,
            fragment_name=dead_name, row_index=step + 1,
        )
        candidates.append(entry)
        if op == "STORE":
            dead_store[fork_owner] = (content_hash, size_bytes)
        dead_state = spec_apply(dead_state, op)
        dead_tip = entry["chain_hash"]
    true_tip = true_entry["chain_hash"]
    true_ts = fork_ts
    true_seq = fork_seq
    for step in range(true_tail_length):
        true_seq += 1
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[true_state])
        content_hash, size_bytes = _auto_payload(op, true_seq, true_store, fork_owner, "t")
        true_ts += 1
        entry = make_entry(
            true_seq, fork_owner, op, content_hash, size_bytes, true_ts, true_tip,
            fragment_name=true_name, row_index=step + 1,
        )
        candidates.append(entry)
        if op == "STORE":
            true_store[fork_owner] = (content_hash, size_bytes)
        true_state = spec_apply(true_state, op)
        true_tip = entry["chain_hash"]
    winner_reach = fork_seq + max(dead_end_depth, true_tail_length)
    return {
        "candidates": candidates,
        "fork_seq": fork_seq,
        "expected_chain_end": winner_reach,
        "true_branch_name": true_name,
        "dead_branch_name": dead_name,
        "true_branch_op": op_b,
        "dead_branch_op": op_a,
        "fork_owner": fork_owner,
    }


def _extend_one_fork(rng, candidates, tip, seq, states, last_store, artifacts, dead_depth, true_tail, fork_index, dead_first):
    fork_owner = artifacts[fork_index % len(artifacts)]
    if states[fork_owner] != "STORED":
        seq += 1
        content_hash, size_bytes = _auto_payload("STORE", seq, last_store, fork_owner, f"prep{fork_index}_")
        entry = make_entry(seq, fork_owner, "STORE", content_hash, size_bytes, 8_000_000 + seq, tip,
                            fragment_name="main.csv", row_index=seq)
        candidates.append(entry)
        last_store[fork_owner] = (content_hash, size_bytes)
        states[fork_owner] = spec_apply(states[fork_owner], "STORE")
        tip = entry["chain_hash"]
    op_a, op_b = VALID_OPS_FROM_STATE["STORED"]
    dead_name = f"aaa_seg{fork_index}_dead.csv" if dead_first else f"zzz_seg{fork_index}_dead.csv"
    true_name = f"zzz_seg{fork_index}_true.csv" if dead_first else f"aaa_seg{fork_index}_true.csv"
    fork_seq = seq + 1
    fork_ts = 8_000_000 + fork_seq
    payload = last_store[fork_owner]
    dead_entry = make_entry(fork_seq, fork_owner, op_a, payload[0], payload[1], fork_ts, tip, fragment_name=dead_name, row_index=0)
    true_entry = make_entry(fork_seq, fork_owner, op_b, payload[0], payload[1], fork_ts, tip, fragment_name=true_name, row_index=0)
    candidates.append(dead_entry)
    candidates.append(true_entry)
    dead_state = spec_apply("STORED", op_a)
    dead_store = dict(last_store)
    if op_a == "STORE":
        dead_store[fork_owner] = payload
    dead_tip = dead_entry["chain_hash"]
    dead_seq = fork_seq
    for step_i in range(dead_depth):
        dead_seq += 1
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[dead_state])
        ch, sz = _auto_payload(op, dead_seq, dead_store, fork_owner, f"dd{fork_index}_")
        entry = make_entry(dead_seq, fork_owner, op, ch, sz, 8_000_000 + dead_seq, dead_tip, fragment_name=dead_name, row_index=step_i + 1)
        candidates.append(entry)
        if op == "STORE":
            dead_store[fork_owner] = (ch, sz)
        dead_state = spec_apply(dead_state, op)
        dead_tip = entry["chain_hash"]
    true_state = spec_apply("STORED", op_b)
    true_store = dict(last_store)
    if op_b == "STORE":
        true_store[fork_owner] = payload
    true_tip = true_entry["chain_hash"]
    true_seq = fork_seq
    for step_i in range(true_tail):
        true_seq += 1
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[true_state])
        ch, sz = _auto_payload(op, true_seq, true_store, fork_owner, f"tt{fork_index}_")
        entry = make_entry(true_seq, fork_owner, op, ch, sz, 8_000_000 + true_seq, true_tip, fragment_name=true_name, row_index=step_i + 1)
        candidates.append(entry)
        if op == "STORE":
            true_store[fork_owner] = (ch, sz)
        true_state = spec_apply(true_state, op)
        true_tip = entry["chain_hash"]
    states[fork_owner] = true_state
    last_store.update(true_store)
    return true_tip, true_seq, states, last_store


def spec_build_cascading_fork_chain(seed, n_forks, warmup_length, dead_depth, true_tail, n_artifacts=1, dead_first=True):
    rng = random.Random(seed)
    artifacts = [f"cf-{i}" for i in range(n_artifacts)] if n_artifacts > 1 else ["cf-solo"]
    states = dict.fromkeys(artifacts, "UNKNOWN")
    last_store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 0
    for _ in range(warmup_length):
        artifact_id = rng.choice(artifacts)
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
        seq += 1
        content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "warm")
        entry = make_entry(seq, artifact_id, op, content_hash, size_bytes, 8_000_000 + seq, tip, fragment_name="main.csv", row_index=seq)
        candidates.append(entry)
        if op == "STORE":
            last_store[artifact_id] = (content_hash, size_bytes)
        states[artifact_id] = spec_apply(states[artifact_id], op)
        tip = entry["chain_hash"]
    for fork_index in range(n_forks):
        tip, seq, states, last_store = _extend_one_fork(
            rng, candidates, tip, seq, states, last_store, artifacts, dead_depth, true_tail, fork_index, dead_first
        )
    _recovered, chain_end = spec_build_chain(candidates)
    return candidates, chain_end


def _linear_walk(rng, tip, seq, state, store, artifact_id, steps, ts_base, name, name_prefix, candidates):
    for step in range(steps):
        seq += 1
        op = rng.choice(FULL_VALID_OPS_FROM_STATE[state])
        content_hash, size_bytes = _auto_payload(op, seq, store, artifact_id, name_prefix)
        entry = make_entry(
            seq, artifact_id, op, content_hash, size_bytes, ts_base + seq, tip,
            fragment_name=name, row_index=step,
        )
        candidates.append(entry)
        if op == "STORE":
            store[artifact_id] = (content_hash, size_bytes)
        state = spec_apply(state, op)
        tip = entry["chain_hash"]
    return tip, seq, state, store


def spec_build_nested_fork_chain(seed, warmup_length=4, long_prefix_len=3, inner_dead_depth=1, inner_true_depth=9):
    rng = random.Random(seed)
    artifact_id = "nf-solo"
    state = "UNKNOWN"
    store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 0
    tip, seq, state, store = _linear_walk(
        rng, tip, seq, state, store, artifact_id, warmup_length, 9_000_000, "seg_00.csv", "w", candidates
    )
    fork_state = state
    op_a, op_b = VALID_OPS_FROM_STATE[fork_state]
    fork_seq = seq + 1
    fork_ts = 9_000_000 + fork_seq
    dead_end_depth = long_prefix_len + inner_dead_depth + 3 + (seed % 3)
    if not (dead_end_depth > long_prefix_len + inner_dead_depth):
        raise ValueError("nested fork parameters do not create a naive trap")
    if not (long_prefix_len + inner_true_depth > dead_end_depth):
        raise ValueError("nested fork parameters do not let the true branch actually win")

    dead_store = dict(store)
    dead_content, dead_size = _auto_payload(op_a, fork_seq, dead_store, artifact_id, "d")
    dead_entry = make_entry(
        fork_seq, artifact_id, op_a, dead_content, dead_size, fork_ts, tip,
        fragment_name="seg_01.csv", row_index=0,
    )
    candidates.append(dead_entry)
    if op_a == "STORE":
        dead_store[artifact_id] = (dead_content, dead_size)
    dead_state = spec_apply(fork_state, op_a)
    _dead_tip, _dead_seq, dead_state, dead_store = _linear_walk(
        rng, dead_entry["chain_hash"], fork_seq, dead_state, dead_store, artifact_id,
        dead_end_depth - 1, 9_000_000, "seg_01.csv", "d", candidates,
    )

    long_store = dict(store)
    long_content, long_size = _auto_payload(op_b, fork_seq, long_store, artifact_id, "l")
    long_entry = make_entry(
        fork_seq, artifact_id, op_b, long_content, long_size, fork_ts, tip,
        fragment_name="seg_02.csv", row_index=0,
    )
    candidates.append(long_entry)
    if op_b == "STORE":
        long_store[artifact_id] = (long_content, long_size)
    long_state = spec_apply(fork_state, op_b)
    long_tip, long_seq, long_state, long_store = _linear_walk(
        rng, long_entry["chain_hash"], fork_seq, long_state, long_store, artifact_id,
        long_prefix_len - 1, 9_000_000, "seg_02.csv", "l", candidates,
    )

    inner_fork_seq = long_seq + 1
    inner_fork_ts = 9_000_000 + inner_fork_seq
    inner_op_a, inner_op_b = VALID_OPS_FROM_STATE[long_state]
    inner_dead_store = dict(long_store)
    inner_dead_content, inner_dead_size = _auto_payload(inner_op_a, inner_fork_seq, inner_dead_store, artifact_id, "id")
    inner_dead_entry = make_entry(
        inner_fork_seq, artifact_id, inner_op_a, inner_dead_content, inner_dead_size, inner_fork_ts, long_tip,
        fragment_name="seg_03.csv", row_index=0,
    )
    candidates.append(inner_dead_entry)
    if inner_op_a == "STORE":
        inner_dead_store[artifact_id] = (inner_dead_content, inner_dead_size)
    inner_dead_state = spec_apply(long_state, inner_op_a)
    _linear_walk(
        rng, inner_dead_entry["chain_hash"], inner_fork_seq, inner_dead_state, inner_dead_store, artifact_id,
        inner_dead_depth - 1, 9_000_000, "seg_03.csv", "id", candidates,
    )

    inner_true_store = dict(long_store)
    inner_true_content, inner_true_size = _auto_payload(inner_op_b, inner_fork_seq, inner_true_store, artifact_id, "it")
    inner_true_entry = make_entry(
        inner_fork_seq, artifact_id, inner_op_b, inner_true_content, inner_true_size, inner_fork_ts, long_tip,
        fragment_name="seg_04.csv", row_index=0,
    )
    candidates.append(inner_true_entry)
    if inner_op_b == "STORE":
        inner_true_store[artifact_id] = (inner_true_content, inner_true_size)
    inner_true_state = spec_apply(long_state, inner_op_b)
    _linear_walk(
        rng, inner_true_entry["chain_hash"], inner_fork_seq, inner_true_state, inner_true_store, artifact_id,
        inner_true_depth - 1, 9_000_000, "seg_04.csv", "it", candidates,
    )

    true_reach = long_prefix_len + inner_true_depth + warmup_length
    naive_reach = long_prefix_len + inner_dead_depth + warmup_length
    dead_reach = dead_end_depth + warmup_length
    _recovered, chain_end = spec_build_chain(candidates)
    if chain_end != true_reach:
        raise ValueError(f"fixture invariant broken: chain_end={chain_end} expected {true_reach}")
    if not (dead_reach > naive_reach):
        raise ValueError("trap condition not actually satisfied")
    return candidates, chain_end


def spec_build_misleading_name_fork_chain(seed, warmup_length=3, dead_depth=3, true_tail=8):
    rng = random.Random(seed)
    artifact_id = "mn-solo"
    state = "UNKNOWN"
    store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 0
    tip, seq, state, store = _linear_walk(
        rng, tip, seq, state, store, artifact_id, warmup_length, 9_500_000, "seg_00.csv", "w", candidates
    )
    fork_state = state
    op_a, op_b = VALID_OPS_FROM_STATE[fork_state]
    fork_seq = seq + 1
    fork_ts = 9_500_000 + fork_seq
    decoy_sounding_name = "zzz_decoy_stream.csv"
    genuine_sounding_name = "aaa_genuine_backup.csv"

    dead_store = dict(store)
    dead_content, dead_size = _auto_payload(op_a, fork_seq, dead_store, artifact_id, "d")
    dead_entry = make_entry(
        fork_seq, artifact_id, op_a, dead_content, dead_size, fork_ts, tip,
        fragment_name=genuine_sounding_name, row_index=0,
    )
    candidates.append(dead_entry)
    if op_a == "STORE":
        dead_store[artifact_id] = (dead_content, dead_size)
    dead_state = spec_apply(fork_state, op_a)
    _linear_walk(
        rng, dead_entry["chain_hash"], fork_seq, dead_state, dead_store, artifact_id,
        dead_depth - 1, 9_500_000, genuine_sounding_name, "d", candidates,
    )

    true_store = dict(store)
    true_content, true_size = _auto_payload(op_b, fork_seq, true_store, artifact_id, "t")
    true_entry = make_entry(
        fork_seq, artifact_id, op_b, true_content, true_size, fork_ts, tip,
        fragment_name=decoy_sounding_name, row_index=0,
    )
    candidates.append(true_entry)
    if op_b == "STORE":
        true_store[artifact_id] = (true_content, true_size)
    true_state = spec_apply(fork_state, op_b)
    _linear_walk(
        rng, true_entry["chain_hash"], fork_seq, true_state, true_store, artifact_id,
        true_tail - 1, 9_500_000, decoy_sounding_name, "t", candidates,
    )

    expected_end = fork_seq + max(dead_depth, true_tail) - 1
    winner_name = decoy_sounding_name if true_tail > dead_depth else genuine_sounding_name
    winner_op = op_b if true_tail > dead_depth else op_a
    return {
        "candidates": candidates,
        "fork_seq": fork_seq,
        "expected_chain_end": expected_end,
        "winner_fragment_name": winner_name,
        "winner_op": winner_op,
    }


def spec_build_concurrent_independent_forks_chain(seed, warmup_length=4, dead_depth=3, true_tail=7):
    rng = random.Random(seed)
    artifacts = ["cif-alpha", "cif-beta"]
    states = dict.fromkeys(artifacts, "UNKNOWN")
    last_store = {}
    tip = GENESIS_HASH
    candidates = []
    seq = 0
    for _ in range(warmup_length):
        for artifact_id in artifacts:
            seq += 1
            op = rng.choice(FULL_VALID_OPS_FROM_STATE[states[artifact_id]])
            content_hash, size_bytes = _auto_payload(op, seq, last_store, artifact_id, "w")
            entry = make_entry(
                seq, artifact_id, op, content_hash, size_bytes, 9_800_000 + seq, tip,
                fragment_name="seg_00.csv", row_index=seq,
            )
            candidates.append(entry)
            if op == "STORE":
                last_store[artifact_id] = (content_hash, size_bytes)
            states[artifact_id] = spec_apply(states[artifact_id], op)
            tip = entry["chain_hash"]
    results = {}
    for i, artifact_id in enumerate(artifacts):
        fork_state = states[artifact_id]
        op_a, op_b = VALID_OPS_FROM_STATE[fork_state]
        fork_seq = seq + 1
        fork_ts = 9_800_000 + fork_seq
        dead_store = dict(last_store)
        dead_content, dead_size = _auto_payload(op_a, fork_seq, dead_store, artifact_id, f"d{i}")
        dead_entry = make_entry(
            fork_seq, artifact_id, op_a, dead_content, dead_size, fork_ts, tip,
            fragment_name=f"seg_{10 + 2 * i}.csv", row_index=0,
        )
        candidates.append(dead_entry)
        if op_a == "STORE":
            dead_store[artifact_id] = (dead_content, dead_size)
        dead_state = spec_apply(fork_state, op_a)
        _linear_walk(
            rng, dead_entry["chain_hash"], fork_seq, dead_state, dead_store, artifact_id,
            dead_depth - 1, 9_800_000, f"seg_{10 + 2 * i}.csv", f"d{i}", candidates,
        )
        true_store = dict(last_store)
        true_content, true_size = _auto_payload(op_b, fork_seq, true_store, artifact_id, f"t{i}")
        true_entry = make_entry(
            fork_seq, artifact_id, op_b, true_content, true_size, fork_ts, tip,
            fragment_name=f"seg_{11 + 2 * i}.csv", row_index=0,
        )
        candidates.append(true_entry)
        if op_b == "STORE":
            true_store[artifact_id] = (true_content, true_size)
        true_state = spec_apply(fork_state, op_b)
        true_tip, true_seq, true_state, true_store = _linear_walk(
            rng, true_entry["chain_hash"], fork_seq, true_state, true_store, artifact_id,
            true_tail - 1, 9_800_000, f"seg_{11 + 2 * i}.csv", f"t{i}", candidates,
        )
        results[artifact_id] = {
            "fork_seq": fork_seq,
            "winner_op": op_b,
            "final_state": true_state,
            "last_seq": true_seq,
        }
        last_store = dict(last_store)
        last_store.update(true_store)
        states = dict(states)
        states[artifact_id] = true_state
        tip = true_tip
        seq = true_seq
    _recovered, chain_end = spec_build_chain(candidates)
    return candidates, results, chain_end
