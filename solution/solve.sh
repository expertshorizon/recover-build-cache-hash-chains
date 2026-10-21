#!/bin/bash
set -e

if [ -f /solution/src/ledger/config.py ]; then
    SRC=/solution
elif [ -f /oracle/src/ledger/config.py ]; then
    SRC=/oracle
else
    echo "solve.sh: could not locate the oracle solution mount" >&2
    exit 1
fi

cp "$SRC/src/ledger/config.py" /app/src/ledger/config.py
cp "$SRC/src/ledger/models.py" /app/src/ledger/models.py
cp "$SRC/src/ledger/__init__.py" /app/src/ledger/__init__.py
cp "$SRC/src/ledger/hashing.py" /app/src/ledger/hashing.py
cp "$SRC/src/ledger/statemachine.py" /app/src/ledger/statemachine.py
cp "$SRC/src/ledger/fragments.py" /app/src/ledger/fragments.py
cp "$SRC/src/ledger/chainbuilder.py" /app/src/ledger/chainbuilder.py
cp "$SRC/src/ledger/storage.py" /app/src/ledger/storage.py
cp "$SRC/src/ledger/summary.py" /app/src/ledger/summary.py

python3 /app/src/main.py
