#!/usr/bin/env python3
"""Generate exact Rust collation weight images from TiDB's Go authorities.

The binary format is deliberately trivial and host-independent:

* ``general_ci_u16_le.bin``: 65,536 little-endian u16 weights.
* ``unicode_0400_u64_le.bin``: 65,536 little-endian u64 weights.
* ``unicode_0400_long_u64_le.bin``: sorted ``<u32,u64,u64>`` records.

Do not edit those files by hand. This program also checks the generated UCA
4.0 data against TiDB's retained original test fixture, preserving
``TestUnicode0400IsTheSame`` as part of the generation gate.
"""

from __future__ import annotations

import argparse
import re
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
CRATE = ROOT / "rust/crates/tidb-datatype"
OUTPUT = CRATE / "src/collation_data"
GENERAL_GO = ROOT / "pkg/util/collate/general_ci.go"
UCA_GO = ROOT / "pkg/util/collate/ucadata/unicode_ci_data_generated.go"
UCA_ORIGINAL_GO = ROOT / "pkg/util/collate/ucadata/unicode_ci_data_original_test.go"


def numeric_values(source: str) -> list[int]:
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.S)
    return [int(token, 0) for token in re.findall(r"0[xX][0-9a-fA-F]+|\b0\b", source)]


def between(source: str, start: str, end: str) -> str:
    try:
        return source.split(start, 1)[1].split(end, 1)[0]
    except IndexError as error:
        raise ValueError(f"cannot find source delimiters {start!r} .. {end!r}") from error


def parse_general_ci() -> list[int]:
    source = GENERAL_GO.read_text()
    planes: dict[int, list[int]] = {}
    for name, body in re.findall(r"plane([0-9A-F]{2}) = \[\]uint16\{(.*?)\}", source, re.S):
        values = numeric_values(body)
        if len(values) != 256:
            raise ValueError(f"general_ci plane {name} has {len(values)} values, expected 256")
        planes[int(name, 16)] = values

    expected_planes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x1E, 0x1F, 0x21, 0x24, 0xFF}
    if set(planes) != expected_planes:
        raise ValueError(f"unexpected general_ci planes: {sorted(planes)}")

    table_body = between(source, "planeTable = [][]uint16{", "}")
    table_entries = re.findall(r"plane[0-9A-F]{2}|nil", table_body)
    if len(table_entries) != 256:
        raise ValueError(f"general_ci plane table has {len(table_entries)} entries, expected 256")
    for index, entry in enumerate(table_entries):
        expected = f"plane{index:02X}" if index in expected_planes else "nil"
        if entry != expected:
            raise ValueError(f"general_ci plane table entry {index:#x} is {entry}, expected {expected}")

    return [planes[codepoint >> 8][codepoint & 0xFF] if codepoint >> 8 in planes else codepoint for codepoint in range(65536)]


def parse_long_map(source: str, start: str, end: str) -> list[tuple[int, int, int]]:
    body = between(source, start, end)
    rows = [
        (int(rune, 16), int(first, 16), int(second, 16))
        for rune, first, second in re.findall(
            r"0x([0-9A-Fa-f]+)\s*:\s*\{0x([0-9A-Fa-f]+),\s*0x([0-9A-Fa-f]+)\}", body
        )
    ]
    if len(rows) != 22:
        raise ValueError(f"UCA 4.0 long-rune map has {len(rows)} rows, expected 22")
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("UCA 4.0 long-rune map has duplicate runes")
    if len({row[1:] for row in rows}) != len(rows):
        raise ValueError("UCA 4.0 long-rune map has duplicate weights")
    return sorted(rows)


def parse_uca_generated() -> tuple[list[int], list[tuple[int, int, int]]]:
    source = UCA_GO.read_text()
    table = numeric_values(between(source, "MapTable4: [65536]uint64{", "},\n\tLongRuneMap:"))
    if len(table) != 65536:
        raise ValueError(f"generated UCA 4.0 table has {len(table)} values, expected 65536")
    long_map = parse_long_map(source, "LongRuneMap: map[rune][2]uint64{", "\n\t},\n}")
    markers = {index for index, value in enumerate(table) if value == 0xFFFD}
    long_runes = {row[0] for row in long_map}
    if markers != long_runes:
        raise ValueError(
            "UCA 4.0 long-rune markers and expansion records differ: "
            f"missing={sorted(markers - long_runes)}, extra={sorted(long_runes - markers)}"
        )
    return table, long_map


def verify_original(generated: list[int], generated_long: list[tuple[int, int, int]]) -> None:
    source = UCA_ORIGINAL_GO.read_text()
    original = numeric_values(between(source, "mapTable = []uint64{", "\n\t}\n\tlongRuneMap"))
    if generated != original:
        mismatch = next(index for index, pair in enumerate(zip(generated, original)) if pair[0] != pair[1])
        raise ValueError(
            f"generated UCA 4.0 table differs from original at {mismatch:#x}: "
            f"{generated[mismatch]:#x} != {original[mismatch]:#x}"
        )
    original_long = parse_long_map(source, "longRuneMap = map[rune][]uint64{", "\n\t}\n)")
    if generated_long != original_long:
        raise ValueError("generated UCA 4.0 long-rune map differs from original")


def encoded_files() -> dict[Path, bytes]:
    general = parse_general_ci()
    uca, long_map = parse_uca_generated()
    verify_original(uca, long_map)
    return {
        OUTPUT / "general_ci_u16_le.bin": b"".join(struct.pack("<H", value) for value in general),
        OUTPUT / "unicode_0400_u64_le.bin": b"".join(struct.pack("<Q", value) for value in uca),
        OUTPUT / "unicode_0400_long_u64_le.bin": b"".join(
            struct.pack("<IQQ", rune, first, second) for rune, first, second in long_map
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if checked-in images differ")
    args = parser.parse_args()
    outputs = encoded_files()
    if args.check:
        failures = [str(path.relative_to(ROOT)) for path, expected in outputs.items() if not path.exists() or path.read_bytes() != expected]
        if failures:
            print("collation images are stale: " + ", ".join(failures), file=sys.stderr)
            return 1
        print("collation images match Go sources: 65536 general_ci, 65536 UCA 4.0, 22 long-rune rows")
        return 0

    OUTPUT.mkdir(parents=True, exist_ok=True)
    for path, contents in outputs.items():
        path.write_bytes(contents)
        print(f"wrote {path.relative_to(ROOT)} ({len(contents)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
