import csv
import glob
import os

from ledger.models import LedgerEntry

REQUIRED_COLUMNS = [
    "seq",
    "artifact_id",
    "op",
    "content_hash",
    "size_bytes",
    "ts",
    "prev_hash",
    "chain_hash",
]


def load_fragment(path: str) -> list[LedgerEntry]:
    entries: list[LedgerEntry] = []
    name = os.path.basename(path)
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return entries
        if [cell.strip() for cell in header] != REQUIRED_COLUMNS:
            return entries
        for row_index, row in enumerate(reader):
            values = [cell.strip() for cell in row]
            if len(values) != len(REQUIRED_COLUMNS):
                break
            try:
                entry = LedgerEntry(
                    seq=int(values[0]),
                    artifact_id=values[1],
                    op=values[2],
                    content_hash=values[3],
                    size_bytes=int(values[4]),
                    ts=int(values[5]),
                    prev_hash=values[6],
                    chain_hash=values[7],
                    fragment_name=name,
                    row_index=row_index,
                )
            except ValueError:
                break
            entries.append(entry)
    return entries


def load_fragments(directory: str) -> list[LedgerEntry]:
    all_entries: list[LedgerEntry] = []
    for path in sorted(glob.glob(os.path.join(directory, "*.csv"))):
        all_entries.extend(load_fragment(path))
    all_entries.sort(key=lambda e: (e.seq, e.fragment_name, e.row_index))
    return all_entries
