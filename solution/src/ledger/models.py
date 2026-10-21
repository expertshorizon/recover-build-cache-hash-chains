from dataclasses import dataclass

VALID_OPS = ("STORE", "HIT", "MISS", "EVICT", "REFRESH")
VALID_STATES = ("UNKNOWN", "STORED", "EVICTED")


@dataclass
class LedgerEntry:
    seq: int
    artifact_id: str
    op: str
    content_hash: str
    size_bytes: int
    ts: int
    prev_hash: str
    chain_hash: str
    fragment_name: str = ""
    row_index: int = 0
