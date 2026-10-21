from ledger import statemachine
from ledger.config import GENESIS_HASH
from ledger.models import LedgerEntry


def _group_by_seq(candidates: list[LedgerEntry]) -> dict[int, list[LedgerEntry]]:
    groups: dict[int, list[LedgerEntry]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.seq, []).append(candidate)
    return groups


def build_chain(candidates: list[LedgerEntry]) -> tuple[list[LedgerEntry], int]:
    groups = _group_by_seq(candidates)
    tip_hash = GENESIS_HASH
    artifact_states: dict[str, str] = {}
    last_store: dict[str, tuple[str, int]] = {}
    last_ts = None
    recovered: list[LedgerEntry] = []
    chain_end = 0
    seq = 1
    while seq in groups:
        winners = []
        for candidate in groups[seq]:
            if candidate.prev_hash != tip_hash:
                continue
            if last_ts is not None and candidate.ts <= last_ts:
                continue
            current_state = artifact_states.get(candidate.artifact_id, "UNKNOWN")
            is_valid, new_state = statemachine.validate_transition(current_state, candidate.op)
            if not is_valid:
                continue
            last_store_entry = last_store.get(candidate.artifact_id)
            if not statemachine.validate_payload(candidate.op, candidate.content_hash, candidate.size_bytes, last_store_entry):
                continue
            winners.append((candidate, new_state))
        if not winners:
            break
        winners.sort(key=lambda pair: (pair[0].fragment_name, pair[0].row_index))
        chosen, new_state = winners[0]
        recovered.append(chosen)
        artifact_states[chosen.artifact_id] = new_state
        last_store.setdefault(chosen.artifact_id, (chosen.content_hash, chosen.size_bytes))
        tip_hash = chosen.chain_hash
        last_ts = chosen.ts
        chain_end = seq
        seq += 1
    return recovered, chain_end
