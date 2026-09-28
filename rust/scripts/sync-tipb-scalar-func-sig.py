#!/usr/bin/env python3
"""Sync/check tidb-proto's complete dispatch enums against pinned Go TiPB."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUST_PROTO = ROOT / "rust/crates/tidb-proto/proto/select.proto"
GO_MODULE = "github.com/pingcap/tipb"


def enum_block(source: str, label: str, name: str) -> str:
    match = re.search(rf"(?m)^enum {name}\s*\{{.*?^\}}", source, re.DOTALL)
    if match is None:
        raise RuntimeError(f"could not find enum {name} in {label}")
    return match.group(0).replace("\r\n", "\n")


def upstream_enums() -> tuple[dict[str, str], str]:
    result = subprocess.run(
        ["go", "list", "-m", "-f", "{{.Dir}} {{.Version}}", GO_MODULE],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    module_dir, version = result.stdout.strip().rsplit(" ", 1)
    enums = {}
    for name, source in {
        "ScalarFuncSig": "expression.proto",
        "ExprType": "expression.proto",
        "ExecType": "executor.proto",
    }.items():
        source_path = pathlib.Path(module_dir) / "proto" / source
        enums[name] = enum_block(source_path.read_text(), str(source_path), name)
    return enums, version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="replace the checked-in projection with complete dispatch enums from pinned Go TiPB",
    )
    args = parser.parse_args()

    try:
        expected, version = upstream_enums()
        current_text = RUST_PROTO.read_text()
        current = {name: enum_block(current_text, str(RUST_PROTO), name) for name in expected}
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"TiPB enum sync failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        return 2

    if current == expected:
        print(f"Dispatch enums match {GO_MODULE}@{version}")
        return 0

    if not args.write:
        print(
            f"Dispatch enums differ from {GO_MODULE}@{version}; "
            "run rust/scripts/sync-tipb-scalar-func-sig.py --write",
            file=sys.stderr,
        )
        return 1

    for name, block in expected.items():
        current_text = current_text.replace(current[name], block, 1)
    RUST_PROTO.write_text(current_text)
    print(f"Updated dispatch enums from {GO_MODULE}@{version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
