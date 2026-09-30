#!/usr/bin/env python3
"""Synchronize/check the complete TiPB or etcd API inputs selected by TiDB Go master.

Updating requires --write --go-ref <fetched-master-ref>. Checking uses the recorded
immutable dependency and rejects a different pin in a locally available master.
Cargo never runs Go or fetches schemas: builds use only checked-in inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEST = ROOT / "rust/crates/tidb-proto/proto/tipb"
MANIFEST = ROOT / "rust/crates/tidb-proto/tipb-source.json"
MODULE = "github.com/pingcap/tipb"
LABEL = "TiPB"


def run(*command: str) -> str:
    return subprocess.check_output(command, cwd=ROOT, text=True, stderr=subprocess.PIPE)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def version_from_go_mod(source: str) -> str:
    matches = re.findall(r"^\s*(?:require\s+)?" + re.escape(MODULE) + r"\s+(v\S+)", source, re.M)
    if len(matches) != 1:
        raise ValueError(f"Go source must contain exactly one {LABEL} requirement")
    return matches[0]


def selected_source(go_ref: str) -> dict:
    revision = run("git", "rev-parse", "--verify", f"{go_ref}^{{commit}}").strip()
    source = run("git", "show", f"{revision}:go.mod")
    return {"go_master": revision, "go_mod_sha256": digest(source.encode()),
            "module": MODULE, "version": version_from_go_mod(source)}


def module_source(version: str) -> pathlib.Path:
    # Explicit version: never consult the integration branch's older go.mod pin.
    result = json.loads(run("go", "mod", "download", "-json", f"{MODULE}@{version}"))
    if result.get("Error") or not result.get("Dir"):
        raise ValueError(f"cannot resolve {MODULE}@{version}: {result.get('Error')}")
    return pathlib.Path(result["Dir"])


def schema_inputs(module: pathlib.Path) -> dict[str, bytes]:
    if MODULE == "go.etcd.io/etcd/api/v3":
        files = {"etcd/api/" + str(p.relative_to(module)): p.read_bytes()
                 for p in module.rglob("*.proto")}
        files["LICENSE"] = (module / "LICENSE").read_bytes()
        return files
    files = {str(p.relative_to(module / "proto")): p.read_bytes()
             for p in (module / "proto").rglob("*.proto")}
    files.update({str(p.relative_to(module)): p.read_bytes()
                  for p in (module / "include").rglob("*.proto")})
    files["LICENSE"] = (module / "LICENSE").read_bytes()
    return files


def inventory(module: pathlib.Path) -> list[dict]:
    return [{"path": str(p.relative_to(module)), "sha256": digest(p.read_bytes())}
            for p in sorted(module.rglob("*")) if p.is_file()]


def differences(directory: pathlib.Path, expected: dict[str, bytes]) -> list[str]:
    actual = {str(p.relative_to(directory)): p for p in directory.rglob("*") if p.is_file()}
    errors = [f"missing upstream input: {p}" for p in sorted(expected.keys() - actual.keys())]
    errors += [f"extra local input: {p}" for p in sorted(actual.keys() - expected.keys())]
    errors += [f"modified upstream input: {p}" for p in sorted(expected.keys() & actual.keys())
               if actual[p].read_bytes() != expected[p]]
    return errors


def check_master_pin(source: dict) -> None:
    # Shallow CI checkouts may not have origin/master. The recorded immutable
    # module remains sufficient to reproduce their complete input check.
    try:
        master = run("git", "show", "origin/master:go.mod")
    except subprocess.CalledProcessError:
        return
    if version_from_go_mod(master) != source["version"]:
        raise ValueError(f"fetched Go master changed {LABEL}; refresh the selected source receipt")


def check_recorded_source(source: dict) -> None:
    if source["module"] != MODULE:
        raise ValueError(f"recorded module is not {LABEL}")
    try:
        recorded = selected_source(source["go_master"])
    except subprocess.CalledProcessError:
        # A shallow checkout can lack the historical baseline, just as it can
        # lack master. The immutable module and its input hashes still apply.
        return
    if any(source[key] != value for key, value in recorded.items()):
        raise ValueError(f"recorded {LABEL} pin differs from its Go baseline")


def main() -> int:
    global MODULE, LABEL, DEST, MANIFEST
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--go-ref", help="explicit fetched Go master revision for an update")
    parser.add_argument("--etcd-api", action="store_true", help="synchronize complete etcd API schemas instead of TiPB")
    args = parser.parse_args()
    if args.etcd_api:
        MODULE, LABEL = "go.etcd.io/etcd/api/v3", "etcd API"
        DEST = ROOT / "rust/crates/tidb-proto/proto/etcd"
        MANIFEST = ROOT / "rust/crates/tidb-proto/etcd-api-source.json"
    if args.write and not args.go_ref:
        parser.error("--write requires --go-ref; the integration branch is not the Go baseline")
    if args.go_ref and not args.write:
        parser.error("--go-ref selects an update; checking uses the recorded source manifest")
    try:
        source = selected_source(args.go_ref) if args.write else json.loads(MANIFEST.read_text())
        check_recorded_source(source)
        module = module_source(source["version"])
        inputs = schema_inputs(module)
        expected = {key: source[key] for key in ("go_master", "go_mod_sha256", "module", "version")}
        expected.update({"package": MODULE if args.etcd_api else "github.com/pingcap/tipb/go-tipb",
                         "integration": "complete upstream schemas generated by prost; original wire tests retained",
                         "module_artifacts": inventory(module)})
        if args.write:
            # Only this dedicated generated-input directory is replaced. Keeping
            # its exact membership removes stale schemas when upstream removes one.
            if DEST.exists():
                shutil.rmtree(DEST)
            for name, data in inputs.items():
                path = DEST / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            MANIFEST.write_text(json.dumps(expected, indent=2) + "\n")
        else:
            check_master_pin(source)
            errors = differences(DEST, inputs)
            if source != expected:
                errors.append(f"{LABEL} package/generation artifact inventory differs from pinned module")
            if errors:
                raise ValueError("\n".join(errors))
        print(f"Complete {LABEL} inputs match {MODULE}@{source['version']} ({len(inputs)} inputs; {len(expected['module_artifacts'])} module artifacts)")
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"{LABEL} source check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
