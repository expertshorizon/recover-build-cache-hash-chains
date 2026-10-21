from ledger.models import VALID_OPS


def compute_summary(entries) -> dict[str, int]:
    counts = dict.fromkeys(VALID_OPS, 0)
    for entry in entries:
        counts[entry.op] += 1
    return counts


def compute_artifact_states(entries, statemachine_module) -> dict[str, tuple[str, int]]:
    states: dict[str, tuple[str, int]] = {}
    for entry in sorted(entries, key=lambda e: e.seq):
        current_state = states.get(entry.artifact_id, ("UNKNOWN", 0))[0]
        _, new_state = statemachine_module.validate_transition(current_state, entry.op)
        states[entry.artifact_id] = (new_state, entry.seq)
    return states


def compute_distinct_artifacts(candidates, recovered) -> int:
    return len({entry.artifact_id for entry in recovered})
