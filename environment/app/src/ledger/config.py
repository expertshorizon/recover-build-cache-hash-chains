import hashlib

GENESIS_PREV_HASH = "0" * 64
GENESIS_SEQ = 0
GENESIS_ARTIFACT_ID = "GENESIS"
GENESIS_OP = "GENESIS"


def _genesis_preimage() -> str:
    fields = (GENESIS_SEQ, GENESIS_ARTIFACT_ID, GENESIS_OP, "", 0, 0, GENESIS_PREV_HASH)
    return "|".join(str(field) for field in fields)


GENESIS_HASH = hashlib.sha256(_genesis_preimage().encode("utf-8")).hexdigest()

FRAGMENTS_DIR = "/app/data/fragments"
OUTPUT_DIR = "/app/output"
DB_PATH = f"{OUTPUT_DIR}/ledger.db"
SUMMARY_PATH = f"{OUTPUT_DIR}/summary.json"
