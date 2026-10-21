from ledger import hashing, statemachine
from ledger.config import GENESIS_HASH
from ledger.models import LedgerEntry


def _group_by_seq(candidates: list[LedgerEntry]) -> dict[int, list[LedgerEntry]]:
    groups: dict[int, list[LedgerEntry]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.seq, []).append(candidate)
    return groups


def _admit(candidate, tip_hash, artifact_states, last_store, last_ts):
    if candidate.prev_hash != tip_hash:
        return None
    if hashing.compute_chain_hash(candidate) != candidate.chain_hash:
        return None
    if last_ts is not None and candidate.ts < last_ts:
        return None
    current_state = artifact_states.get(candidate.artifact_id, "UNKNOWN")
    is_valid, new_state = statemachine.validate_transition(current_state, candidate.op)
    if not is_valid:
        return None
    last_store_entry = last_store.get(candidate.artifact_id)
    if not statemachine.validate_payload(candidate.op, candidate.content_hash, candidate.size_bytes, last_store_entry):
        return None
    return new_state


def build_chain(candidates: list[LedgerEntry]) -> tuple[list[LedgerEntry], int]:
    # Traverse in filename order. Retaining the first path of each maximum
    # length implements the tie rule without keeping every possible suffix.
    groups = {}
    for candidate in candidates:
        groups.setdefault((candidate.seq, candidate.prev_hash), []).append(candidate)
    for rows in groups.values():
        rows.sort(key=lambda row: (row.fragment_name, row.row_index))
    states, stores = {}, {}
    path, best = [], []
    missing = object()
    stack = [(iter(groups.get((1, GENESIS_HASH), ())), None)]
    while stack:
        rows, undo = stack[-1]
        candidate = next(rows, None)
        if candidate is None:
            if len(path) > len(best):
                best = list(path)
            stack.pop()
            if undo is not None:
                artifact, old_state, old_store = undo
                for mapping, old in ((states, old_state), (stores, old_store)):
                    if old is missing:
                        mapping.pop(artifact, None)
                    else:
                        mapping[artifact] = old
                path.pop()
            continue
        tip = path[-1].chain_hash if path else GENESIS_HASH
        last_ts = path[-1].ts if path else None
        new_state = _admit(candidate, tip, states, stores, last_ts)
        if new_state is None:
            continue
        artifact = candidate.artifact_id
        undo = (artifact, states.get(artifact, missing), stores.get(artifact, missing))
        states[artifact] = new_state
        if candidate.op == "STORE":
            stores[artifact] = (candidate.content_hash, candidate.size_bytes)
        path.append(candidate)
        children = groups.get((candidate.seq + 1, candidate.chain_hash), ())
        if not children and len(path) > len(best):
            best = list(path)
        # An internal node can also be the best endpoint if all children fail.
        stack.append((iter(children), undo))
    return best, len(best)
