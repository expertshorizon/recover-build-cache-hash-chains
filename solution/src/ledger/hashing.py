import hashlib

CANONICAL_FIELDS = ("seq", "artifact_id", "op", "content_hash", "size_bytes", "ts", "prev_hash")


def canonical_serialize(entry) -> str:
    parts = [str(getattr(entry, field)) for field in CANONICAL_FIELDS]
    return "|".join(parts)


def compute_chain_hash(entry) -> str:
    preimage = canonical_serialize(entry)
    return hashlib.sha256(preimage.encode("utf-8")).hexdigest()
