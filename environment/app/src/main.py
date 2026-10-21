import json
import os

from ledger import chainbuilder, config, fragments, statemachine, storage, summary


def run() -> None:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    candidates = fragments.load_fragments(config.FRAGMENTS_DIR)
    recovered, chain_end = chainbuilder.build_chain(candidates)
    counts = summary.compute_summary(recovered)
    states = summary.compute_artifact_states(recovered, statemachine)
    artifact_states = {artifact_id: (state, seq) for artifact_id, (state, seq) in states.items()}
    distinct_artifacts = summary.compute_distinct_artifacts(candidates, recovered)
    summary_counts = dict(counts)
    summary_counts["chain_end"] = chain_end
    summary_counts["distinct_artifacts"] = distinct_artifacts
    storage.materialize(config.DB_PATH, recovered, artifact_states, summary_counts)
    payload = {
        "counts": counts,
        "chain_end": chain_end,
        "distinct_artifacts": distinct_artifacts,
        "artifact_states": {artifact_id: state for artifact_id, (state, _) in states.items()},
    }
    with open(config.SUMMARY_PATH, "w") as fh:
        json.dump(payload, fh, indent=2)


if __name__ == "__main__":
    run()
