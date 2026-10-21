def compute_summary(entries) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.op] = counts.get(entry.op, 0) + 1
    return counts


def compute_artifact_states(entries, statemachine_module) -> dict[str, tuple[str, int]]:
    states: dict[str, tuple[str, int]] = {}
    for entry in sorted(entries, key=lambda e: e.fragment_name):
        prior_state, prior_seq = states.get(entry.artifact_id, ("UNKNOWN", 0))
        _, new_state = statemachine_module.validate_transition(prior_state, entry.op)
        if entry.op in ("STORE", "HIT", "EVICT", "MISS"):
            states[entry.artifact_id] = (new_state, entry.seq)
        else:
            states[entry.artifact_id] = (new_state, prior_seq)
    return states


def compute_distinct_artifacts(candidates, recovered) -> int:
    return len({candidate.artifact_id for candidate in candidates})
