#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Run isolated PD source contracts and report native backend observations.

Success means the experiment completed, not that Rust matches Go. Neither
production repository's source nor dependency files are modified. An example
with a reserved name is installed temporarily in client-rust and always removed.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]
PD_VERSION = "v0.0.0-20260805103528-afa43111d149"
NATIVE_BASELINE = "6163ecfc587b248dcbf0e30c1c9d905b4bc5a665"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, cwd, log, env=None):
    with log.open("w") as output:
        subprocess.run(command, cwd=cwd, env=env, stdout=output,
                       stderr=subprocess.STDOUT, check=True)


def go_contract(source, output, label, failpoint):
    packages = ["pkg/utils/grpcutil", "pkg/retry"]
    before = {str(p.relative_to(source)): digest(p)
              for package in packages for p in (source / package).glob("*.go")}
    environment = dict(os.environ, PD_GRPCUTIL_ORACLE=str(output))
    try:
        run([str(failpoint), "enable", *packages], source,
            output / (label + "-enable.log"))
        run(["go", "test", "-mod=readonly", "-race", "./pkg/utils/grpcutil", "-count=1"],
            source, output / (label + "-test.log"), environment)
    finally:
        run([str(failpoint), "disable", *packages], source,
            output / (label + "-disable.log"))
        after = {str(p.relative_to(source)): digest(p)
                 for package in packages for p in (source / package).glob("*.go")}
        if before != after:
            raise RuntimeError("Scratch failpoint cleanup changed source bytes")
    observed = json.loads((output / "go-readiness.json").read_text())
    if observed != json.loads((HERE / "go-readiness.json").read_text()):
        raise RuntimeError("Go source readiness contract changed")
    return observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pd-source", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source, native, output = (p.resolve() for p in
                              (args.pd_source, args.native, args.output))
    if output.exists():
        parser.error("output must be a new scratch directory")
    if source.name != "client@" + PD_VERSION:
        parser.error("pd-source must be the recorded module-cache revision")
    inventory = json.loads((HERE / "inventory.json").read_text())
    for artifact in inventory["pd_artifacts"]:
        if digest(source / artifact["path"]) != artifact["sha256"]:
            parser.error("pinned PD artifact changed: " + artifact["path"])
    package_files = sorted(str(p.relative_to(source)) for p in
                           (source / "pkg/utils/grpcutil").rglob("*") if p.is_file())
    if package_files != inventory["package_files"]:
        parser.error("PD package inventory is incomplete")
    native_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=native,
                                          text=True).strip()
    if native_head != NATIVE_BASELINE:
        parser.error("native revision changed; refresh the contract review before recording results")
    for artifact in inventory["native_artifacts"]:
        if digest(native / artifact["path"]) != artifact["sha256"]:
            parser.error("native artifact changed: " + artifact["path"])
    output.mkdir(parents=True)
    copied = output / "go-source"
    shutil.copytree(source, copied)
    copied.chmod(0o755)
    for path in copied.rglob("*"):
        path.chmod(0o755 if path.is_dir() else 0o644)
    shutil.copyfile(HERE / "oracle_test.go.txt",
                    copied / "pkg/utils/grpcutil/native_contract_test.go")
    failpoint = REPO / "tools/bin/failpoint-ctl"
    own = go_contract(copied, output, "pd-own", failpoint)
    # Select only the differing dependencies used by TiDB master. The source
    # grpc/failpoint pins already agree; no production go.mod/go.sum is edited.
    run(["go", "mod", "edit",
         "-require=github.com/pingcap/errors@v0.11.5-0.20260508054701-306e305bcf41",
         "-require=go.uber.org/zap@v1.27.1"], copied, output / "tidb-select.log")
    run(["go", "list", "-mod=mod", "-deps", "-test", "./pkg/utils/grpcutil"],
        copied, output / "tidb-resolve.log")
    selected = go_contract(copied, output, "tidb-selected", failpoint)
    if own != selected:
        raise RuntimeError("PD-owned and TiDB-selected dependency contracts disagree")

    example = native / "examples/pd_grpcutil_contract_probe.rs"
    lock_digest = digest(native / "Cargo.lock")
    # Exclusive creation avoids overwriting any user's existing example.
    target = example.open("x")
    try:
        with target:
            target.write((HERE / "tonic_probe.rs.txt").read_text())
        run(["cargo", "run", "--locked", "--example", "pd_grpcutil_contract_probe"],
            native, output / "rust-probe.log")
    finally:
        example.unlink()
    if digest(native / "Cargo.lock") != lock_digest:
        raise RuntimeError("Native probe unexpectedly changed its lockfile")
    observations = [json.loads(line) for line in (output / "rust-probe.log").read_text().splitlines()
                    if line.startswith("{")]
    if len(observations) != 1:
        raise RuntimeError("Expected one Rust observation record")
    observed = observations[0]
    summary = {
        "status": "contract-review-only; production package unaccepted",
        "go": own,
        "rust": observed,
        "mismatches": sorted(key for key in own if own[key] != observed[key]),
        "native_revision": native_head,
        "rust_probe_sha256": digest(HERE / "tonic_probe.rs.txt"),
        "go_oracle_sha256": digest(HERE / "oracle_test.go.txt"),
    }
    (output / "observations.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
