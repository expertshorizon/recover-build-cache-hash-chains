import json
import sys

sys.path.insert(0, "/app/src")

from ledger import config


def op_run_main(payload):
    import main as agent_main

    agent_main.run()
    return {"ok": True}


def op_run_main_isolated(payload):
    import main as agent_main

    original = (config.FRAGMENTS_DIR, config.DB_PATH, config.SUMMARY_PATH)
    config.FRAGMENTS_DIR = payload["directory"]
    config.DB_PATH = payload["db_path"]
    config.SUMMARY_PATH = payload["summary_path"]
    try:
        agent_main.run()
    finally:
        config.FRAGMENTS_DIR, config.DB_PATH, config.SUMMARY_PATH = original
    return {"ok": True}


DISPATCH = {
    "run_main": op_run_main,
    "run_main_isolated": op_run_main_isolated,
}


def main():
    request_path, response_path = sys.argv[1], sys.argv[2]
    with open(request_path) as fh:
        request = json.load(fh)
    handler = DISPATCH[request["op"]]
    result = handler(request.get("payload", {}))
    with open(response_path, "w") as fh:
        json.dump(result, fh)


if __name__ == "__main__":
    main()
