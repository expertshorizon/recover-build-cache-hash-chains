import hashlib

CANONICAL_FIELDS = ("seq", "artifact_id", "op", "content_hash", "size_bytes", "ts", "prev_hash")


def canonical_serialize(entry) -> str:
    """SHA-256 preimage: pipe-joined canonical fields, UTF-8, no newline.

    Numeric fields use decimal integers; text values are whitespace-trimmed
    by the fragment reader. Field order matches the CSV header without chain_hash.
    """
    parts = [str(getattr(entry, field)) for field in CANONICAL_FIELDS]
    return "|".join(parts)


def compute_chain_hash(entry) -> str:
    preimage = canonical_serialize(entry)
    return hashlib.sha256(preimage.encode("utf-8")).hexdigest()
