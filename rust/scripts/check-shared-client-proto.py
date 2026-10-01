#!/usr/bin/env python3
"""Check complete native protocol packages against the recorded Go-master pin.

Refresh the receipt with --write --go-ref origin/master after publishing and
synchronizing client-rust. This never edits native schemas or generated code.
"""
import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
NATIVE = ROOT / "rust/third_party/tikv-client-rs"
RECEIPT = ROOT / "rust/docs/shared-client-contracts-inventory.json"
MODULE = "github.com/pingcap/kvproto"
PACKAGES = {name: name for name in ("kvrpcpb", "errorpb", "metapb", "encryptionpb", "coprocessor", "mpp", "pdpb", "tikvpb")}
PACKAGES["brpb"] = "backup"
PACKAGES["import_sstpb"] = "import_sstpb"


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.PIPE)


def version(source):
    matches = re.findall(r"^\s*(?:require\s+)?" + re.escape(MODULE) + r"\s+(v\S+)", source, re.M)
    if len(matches) != 1:
        raise ValueError("Go baseline must contain exactly one kvproto requirement")
    return matches[0]


def artifact(path, base):
    return {"path": str(path.relative_to(base)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--go-ref")
    args = parser.parse_args()
    if args.write != bool(args.go_ref):
        parser.error("updates require both --write and --go-ref; checking uses the receipt")
    try:
        recorded = None if args.write else json.loads(RECEIPT.read_text())
        revision = (run("git", "rev-parse", "--verify", f"{args.go_ref}^{{commit}}").strip()
                    if args.write else recorded["go_master"])
        pin = version(run("git", "show", f"{revision}:go.mod")) if args.write else recorded["kvproto"]
        for ref in (revision, "origin/master"):
            try:
                source = run("git", "show", f"{ref}:go.mod")
            except subprocess.CalledProcessError:
                continue  # Historical refs can be absent in shallow CI clones.
            if version(source) != pin:
                raise ValueError(f"kvproto pin differs from {ref}; refresh the native owner and receipt")
        module = json.loads(run("go", "mod", "download", "-json", f"{MODULE}@{pin}"))
        upstream = pathlib.Path(module["Dir"])
        inputs = list((upstream / "proto").rglob("*.proto")) + list((upstream / "include").rglob("*.proto"))
        expected_paths = set()
        for path in inputs:
            relative = path.relative_to(upstream)
            native = NATIVE / relative if relative.parts[0] == "proto" else NATIVE / "proto" / relative
            expected_paths.add(native.relative_to(NATIVE / "proto"))
            if not native.is_file() or path.read_bytes() != native.read_bytes():
                raise ValueError(f"native input is missing or differs from Go master: {relative}")
        actual_paths = {p.relative_to(NATIVE / "proto") for p in (NATIVE / "proto").rglob("*.proto")}
        # Channelz belongs to gRPC's own API, not the kvproto module. Native
        # transport already owns this separate schema; it is not a projection.
        extra = actual_paths - expected_paths - {pathlib.Path("grpc_channelz.proto")}
        if extra:
            raise ValueError(f"stale or extra native kvproto inputs: {sorted(map(str, extra))}")
        receipt = {
            "go_master": revision, "kvproto": pin,
            "integration": "complete native packages; shared types and descriptor-derived TiKV transport",
            "packages": {name: {
                "go_artifacts": [artifact(p, upstream) for p in sorted((upstream / "pkg" / name).rglob("*")) if p.is_file()],
                "rust_output": artifact(NATIVE / f"kvproto/src/generated/{rust_name}.rs", ROOT),
                "integration": ("complete descriptor-derived transport with opaque batch bodies" if name == "tikvpb"
                                else "whole-package re-export and prost extern_path"),
            } for name, rust_name in PACKAGES.items()},
            "module_artifacts": [artifact(p, upstream) for p in sorted(upstream.rglob("*")) if p.is_file()],
            "build_inputs": [artifact(NATIVE / name, ROOT) for name in (
                "proto-build/src/main.rs", "proto-build/Cargo.toml", "kvproto/src/lib.rs",
                "kvproto/Cargo.toml", "kvproto/src/generated/mod.rs",
                "kvproto/src/generated/file_descriptor_set.bin")],
        }
        if args.write:
            RECEIPT.write_text(json.dumps(receipt, indent=2) + "\n")
        elif receipt != recorded:
            raise ValueError("shared protocol package/generated input receipt changed; review and refresh it")
        print(f"Complete native protocol inputs match {MODULE}@{pin}; {len(PACKAGES)} shared packages; {len(receipt['module_artifacts'])} module artifacts")
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Shared protocol check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
