import json
import os
import shutil
import subprocess
import tempfile

SANDBOX_UID = 12000
RUNNER_PATH = "/work/agent_runner.py"
WORK_ROOT = "/work"
DROP_PRIVILEGES = [
    "setpriv",
    f"--reuid={SANDBOX_UID}",
    f"--regid={SANDBOX_UID}",
    "--clear-groups",
]


def new_scratch_dir():
    os.makedirs(WORK_ROOT, exist_ok=True)
    scratch = tempfile.mkdtemp(dir=WORK_ROOT)
    os.chown(scratch, SANDBOX_UID, SANDBOX_UID)
    os.chmod(scratch, 0o700)
    return scratch


def make_sandbox_readable(path):
    for root, _dirs, files in os.walk(path):
        os.chmod(root, 0o755)
        for name in files:
            os.chmod(os.path.join(root, name), 0o644)
    os.chmod(path, 0o755)
    ancestor = os.path.dirname(os.path.abspath(path))
    while ancestor not in ("/", "/tmp", WORK_ROOT, os.path.dirname(ancestor)):
        try:
            current_mode = os.stat(ancestor).st_mode & 0o777
            os.chmod(ancestor, current_mode | 0o005)
        except OSError:
            break
        ancestor = os.path.dirname(ancestor)


def write_fragment_dir(base_dir, files):
    os.makedirs(base_dir, exist_ok=True)
    for name, content in files.items():
        target = os.path.join(base_dir, name)
        with open(target, "w", newline="") as fh:
            fh.write(content)
    make_sandbox_readable(base_dir)
    return base_dir


def call_agent(op, payload=None, timeout=30):
    scratch = new_scratch_dir()
    payload = dict(payload or {})
    if "directory" in payload:
        # Expose input fragments only, never sibling goldens or verifier code.
        staged = os.path.join(scratch, "fragments")
        os.mkdir(staged)
        for name in os.listdir(payload["directory"]):
            source = os.path.join(payload["directory"], name)
            if name.endswith(".csv") and os.path.isfile(source):
                shutil.copyfile(source, os.path.join(staged, name))
        make_sandbox_readable(staged)
        payload["directory"] = staged
    request_path = os.path.join(scratch, "request.json")
    response_path = os.path.join(scratch, "response.json")
    with open(request_path, "w") as fh:
        json.dump({"op": op, "payload": payload or {}}, fh)
    os.chown(request_path, SANDBOX_UID, SANDBOX_UID)
    os.chmod(request_path, 0o600)
    result = subprocess.run(
        [*DROP_PRIVILEGES, "python3", RUNNER_PATH, request_path, response_path],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    assert result.returncode == 0, f"agent_runner op {op!r} failed:\n{result.stderr}"
    with open(response_path) as fh:
        return json.load(fh)
