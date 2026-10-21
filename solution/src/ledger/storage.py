import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger (
    seq INTEGER PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    op TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    prev_hash TEXT NOT NULL,
    chain_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifact_state (
    artifact_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    last_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS summary (
    metric TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def materialize(db_path, entries, artifact_states, summary_counts) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute("DELETE FROM ledger")
        conn.execute("DELETE FROM artifact_state")
        conn.execute("DELETE FROM summary")
        conn.executemany(
            "INSERT INTO ledger "
            "(seq, artifact_id, op, content_hash, size_bytes, ts, prev_hash, chain_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    e.seq,
                    e.artifact_id,
                    e.op,
                    e.content_hash,
                    e.size_bytes,
                    e.ts,
                    e.prev_hash,
                    e.chain_hash,
                )
                for e in entries
            ],
        )
        conn.executemany(
            "INSERT INTO artifact_state (artifact_id, state, last_seq) VALUES (?, ?, ?)",
            [(artifact_id, state, last_seq) for artifact_id, (state, last_seq) in artifact_states.items()],
        )
        conn.executemany(
            "INSERT INTO summary (metric, value) VALUES (?, ?)",
            list(summary_counts.items()),
        )
        conn.commit()
    finally:
        conn.close()
