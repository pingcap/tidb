#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Reproduce the isolated h2 candidate's successes AND known Go mismatches.

An exit code of zero means the recorded experiment was reproduced, not parity.
No production dependency, source, generated output, or native repository is edited.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifacts(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob("*"))
            if p.is_file() and p.name != ".cargo-ok"}


def writable_copy(source, destination):
    shutil.copytree(source, destination)
    destination.chmod(0o755)
    for path in destination.rglob("*"):
        path.chmod(0o755 if path.is_dir() else 0o644)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--h2-source", type=Path, required=True)
    parser.add_argument("--pd-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    h2, pd, output = (p.resolve() for p in
                      (args.h2_source, args.pd_source, args.output))
    if output.exists():
        parser.error("output must be a new scratch directory")
    expected_h2 = json.loads((HERE / "h2-inputs.json").read_text())["artifacts"]
    if artifacts(h2) != expected_h2:
        parser.error("h2 source differs from the complete recorded crate artifact inventory")
    inventory = json.loads((HERE / "inventory.json").read_text())
    for entry in inventory["pd_artifacts"]:
        if sha(pd / entry["path"]) != entry["sha256"]:
            parser.error("PD source changed: " + entry["path"])
    package_files = sorted(str(p.relative_to(pd)) for p in
                           (pd / "pkg/utils/grpcutil").rglob("*") if p.is_file())
    if package_files != inventory["package_files"]:
        parser.error("PD package inventory changed")

    # Reuse the existing source-oracle runner and its failpoint restoration gate.
    spec = importlib.util.spec_from_file_location("source_review", HERE / "run.py")
    review = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(review)
    output.mkdir(parents=True)
    candidate = output / "candidate"
    candidate.mkdir()
    writable_copy(h2, candidate / "h2")
    review.run(["patch", "--batch", "-p1", "-i", str(HERE / "h2-readiness.patch")],
               candidate / "h2", output / "patch.log")
    (candidate / "src").mkdir()
    for source, destination in [
        ("h2_owner_probe.rs.txt", "src/lib.rs"),
        ("h2-probe.Cargo.toml.txt", "Cargo.toml"),
        ("h2-probe.Cargo.lock", "Cargo.lock"),
    ]:
        shutil.copyfile(HERE / source, candidate / destination)
    with (output / "candidate-tests.log").open("w") as log:
        result = subprocess.run(["cargo", "test", "--locked", "--lib", "--",
                                 "--test-threads=1"], cwd=candidate, stdout=log,
                                stderr=subprocess.STDOUT, check=False)
    observed = (output / "candidate-tests.log").read_text()
    expected_failures = ["first_frame_must_be_settings_like_go",
                         "settings_ack_first_frame_matches_go_readiness"]
    failed = sorted(line.split("::")[-1].split(" ...")[0]
                    for line in observed.splitlines()
                    if line.startswith("test tests::") and line.endswith(" ... FAILED"))
    if (result.returncode != 101 or failed != expected_failures
            or "8 passed; 2 failed; 0 ignored" not in observed):
        raise RuntimeError("candidate observations changed; inspect candidate-tests.log")
    frames = [json.loads(line.removeprefix("FIRST_FRAMES "))
              for line in observed.splitlines() if line.startswith("FIRST_FRAMES ")]
    ack = [json.loads(line.removeprefix("SETTINGS_ACK "))
           for line in observed.splitlines() if line.startswith("SETTINGS_ACK ")]
    if frames != [[True, True]] or ack != [False]:
        raise RuntimeError("first-frame observations changed; inspect candidate-tests.log")
    review.run(["cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings"],
               candidate, output / "candidate-clippy.log")
    review.run(["cargo", "fmt", "--all", "--check"], candidate,
               output / "candidate-fmt.log")
    if sha(candidate / "Cargo.lock") != sha(HERE / "h2-probe.Cargo.lock"):
        raise RuntimeError("candidate lockfile changed")

    copied = output / "go-source"
    writable_copy(pd, copied)
    for source, destination in [("oracle_test.go.txt", "native_contract_test.go"),
                                ("lifecycle_test.go.txt", "native_lifecycle_test.go")]:
        shutil.copyfile(HERE / source, copied / "pkg/utils/grpcutil" / destination)
    failpoint = review.REPO / "tools/bin/failpoint-ctl"
    review.go_contract(copied, output, "pd-own", failpoint)
    review.run(["go", "mod", "edit",
                "-require=github.com/pingcap/errors@v0.11.5-0.20260508054701-306e305bcf41",
                "-require=go.uber.org/zap@v1.27.1"], copied, output / "tidb-select.log")
    review.run(["go", "list", "-mod=mod", "-deps", "-test", "./pkg/utils/grpcutil"],
               copied, output / "tidb-resolve.log")
    review.go_contract(copied, output, "tidb-selected", failpoint)
    summary = {
        "status": "candidate-not-accepted; production package remains open",
        "candidate_tests": {"passed": 8, "failed": failed, "ignored": 0},
        "go_source_contracts": "passed with race/goleak under both dependency selections",
        "clippy_and_format": "passed",
        "first_frame_observations": {
            "ping_then_settings": {"go_ready": False, "candidate_ready": frames[0][0]},
            "unknown_then_settings": {"go_ready": False, "candidate_ready": frames[0][1]},
            "settings_ack": {"go_ready": True, "candidate_ready": ack[0]},
        },
        "input_sha256": {name: sha(HERE / name) for name in [
            "h2-readiness.patch", "h2-inputs.json", "h2_owner_probe.rs.txt",
            "h2-probe.Cargo.toml.txt", "h2-probe.Cargo.lock", "lifecycle_test.go.txt",
        ]},
    }
    (output / "candidate-observations.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
