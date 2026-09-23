#!/usr/bin/env python3
"""Sync/check tidb-proto's ScalarFuncSig projection against pinned Go TiPB."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUST_PROTO = ROOT / "rust/crates/tidb-proto/proto/select.proto"
GO_MODULE = "github.com/pingcap/tipb"


def enum_block(source: str, label: str) -> str:
    match = re.search(r"(?m)^enum ScalarFuncSig\s*\{.*?^\}", source, re.DOTALL)
    if match is None:
        raise RuntimeError(f"could not find enum ScalarFuncSig in {label}")
    return match.group(0).replace("\r\n", "\n")


def upstream_enum() -> tuple[str, str]:
    result = subprocess.run(
        ["go", "list", "-m", "-f", "{{.Dir}} {{.Version}}", GO_MODULE],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    module_dir, version = result.stdout.strip().rsplit(" ", 1)
    source_path = pathlib.Path(module_dir) / "proto/expression.proto"
    if not source_path.is_file():
        raise RuntimeError(f"pinned TiPB source not found: {source_path}")
    return enum_block(source_path.read_text(), str(source_path)), version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="replace the checked-in projection with the enum from pinned Go TiPB",
    )
    args = parser.parse_args()

    try:
        expected, version = upstream_enum()
        current_text = RUST_PROTO.read_text()
        current = enum_block(current_text, str(RUST_PROTO))
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"TiPB ScalarFuncSig sync failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        return 2

    if current == expected:
        print(f"ScalarFuncSig matches {GO_MODULE}@{version}")
        return 0

    if not args.write:
        print(
            f"ScalarFuncSig differs from {GO_MODULE}@{version}; "
            "run rust/scripts/sync-tipb-scalar-func-sig.py --write",
            file=sys.stderr,
        )
        return 1

    RUST_PROTO.write_text(current_text.replace(current, expected, 1))
    print(f"Updated ScalarFuncSig from {GO_MODULE}@{version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
