#!/bin/bash
set -euo pipefail

mkdir -p /logs/verifier
chmod 700 /logs/verifier

# The candidate runs as UID 12000; test code and expected answers stay private.
chmod 700 /tests
for private_dir in /solution /oracle /app/solution /app/oracle; do
    if [ -d "$private_dir" ]; then
        chmod 700 "$private_dir"
    fi
done
mkdir -p /work /app/output
chmod 755 /work
cp /tests/agent_runner.py /work/agent_runner.py
chmod 644 /work/agent_runner.py
chmod -R a+rX /app/src /app/data
chown -R 12000:12000 /app/output
chmod 700 /app/output

cd /tests

rc=0
python -m pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA || rc=$?
if [ "$rc" -eq 0 ]; then
echo 1 > /logs/verifier/reward.txt
else
echo 0 > /logs/verifier/reward.txt
fi

exit 0
