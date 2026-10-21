TRANSITIONS = {
    ("UNKNOWN", "MISS"): "UNKNOWN",
    ("UNKNOWN", "STORE"): "STORED",
    ("STORED", "HIT"): "STORED",
    ("STORED", "EVICT"): "EVICTED",
    ("STORED", "REFRESH"): "STORED",
    ("EVICTED", "MISS"): "EVICTED",
    ("EVICTED", "STORE"): "STORED",
    ("EVICTED", "REFRESH"): "EVICTED",
}

PAYLOAD_BOUND_OPS = ("HIT", "EVICT", "REFRESH")


def validate_transition(current_state: str, op: str) -> tuple[bool, str]:
    key = (current_state, op)
    if key not in TRANSITIONS:
        return False, current_state
    return True, TRANSITIONS[key]


def validate_payload(op: str, content_hash: str, size_bytes: int, last_store: tuple[str, int] | None) -> bool:
    if op not in PAYLOAD_BOUND_OPS:
        return True
    if last_store is None:
        return False
    return last_store == (content_hash, size_bytes)
