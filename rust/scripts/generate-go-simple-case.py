#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate Rust simple-rune casing from Go's exact unicode.CaseRanges and caseOrbit."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / "rust/crates/tidb-mysql/src/simple_case.rs"
ORACLE = REPO_ROOT / "rust/crates/tidb-ast/tests/data/parser_format_simple_case.tsv"
VERSION_RE = re.compile(r'^const Version = "([^"]+)"$', re.MULTILINE)
ROW_RE = re.compile(
    r"^\s*\{(0x[0-9A-F]+), (0x[0-9A-F]+), d\{([^}]+)\}\},\s*$"
)
ORBIT_RE = re.compile(r"^\s*\{(0x[0-9A-F]+), (0x[0-9A-F]+)\},\s*$")


def go_root(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    result = subprocess.run(
        ["go", "env", "GOROOT"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return Path(result.stdout.strip()).resolve()


def parse_case_ranges(table_source: str) -> tuple[str, list[tuple[int, int, str, str]]]:
    version_match = VERSION_RE.search(table_source)
    if version_match is None:
        raise ValueError("Go unicode table has no Version constant")

    marker = "var _CaseRanges = []CaseRange{"
    try:
        block = table_source.split(marker, 1)[1]
    except IndexError as error:
        raise ValueError("Go unicode table has no _CaseRanges block") from error

    rows: list[tuple[int, int, str, str]] = []
    for line in block.splitlines():
        if line == "}":
            break
        match = ROW_RE.match(line)
        if match is None:
            if line.strip():
                raise ValueError(f"unrecognized Go CaseRange row: {line}")
            continue
        lo, hi, deltas = match.groups()
        values = [value.strip() for value in deltas.split(",")]
        if len(values) != 3:
            raise ValueError(f"CaseRange does not have three deltas: {line}")
        rows.append((int(lo, 16), int(hi, 16), values[0], values[1]))

    if not rows:
        raise ValueError("Go unicode table has an empty _CaseRanges block")
    return version_match.group(1), rows


def parse_case_orbit(table_source: str) -> list[tuple[int, int]]:
    marker = "var caseOrbit = []foldPair{"
    try:
        block = table_source.split(marker, 1)[1]
    except IndexError as error:
        raise ValueError("Go unicode table has no caseOrbit block") from error

    rows: list[tuple[int, int]] = []
    for line in block.splitlines():
        if line == "}":
            break
        match = ORBIT_RE.match(line)
        if match is None:
            if line.strip():
                raise ValueError(f"unrecognized Go foldPair row: {line}")
            continue
        rows.append((int(match.group(1), 16), int(match.group(2), 16)))

    if not rows:
        raise ValueError("Go unicode table has an empty caseOrbit block")
    return rows


def rust_delta(value: str) -> str:
    return "UPPER_LOWER" if value == "UpperLower" else value


def map_codepoint(codepoint: int, lo: int, delta: str, case: int) -> int:
    if delta == "UpperLower":
        return lo + (((codepoint - lo) & ~1) | (case & 1))
    return codepoint + int(delta)


def render_oracle(version: str, rows: list[tuple[int, int, str, str]]) -> str:
    output = [
        f"# Go unicode.CaseRanges simple mappings; Unicode {version}.",
        "# codepoint\tuppercase\tlowercase",
    ]
    for lo, hi, upper_delta, lower_delta in rows:
        for codepoint in range(lo, hi + 1):
            upper = map_codepoint(codepoint, lo, upper_delta, 0)
            lower = map_codepoint(codepoint, lo, lower_delta, 1)
            if upper != codepoint or lower != codepoint:
                output.append(f"{codepoint:X}\t{upper:X}\t{lower:X}")
    return "\n".join(output) + "\n"


def render(
    version: str,
    rows: list[tuple[int, int, str, str]],
    orbit: list[tuple[int, int]],
) -> str:
    entries = "\n".join(
        "    CaseRange { "
        f"lo: 0x{lo:04X}, hi: 0x{hi:04X}, "
        f"upper: {rust_delta(upper)}, lower: {rust_delta(lower)} "
        "},"
        for lo, hi, upper, lower in rows
    )
    orbit_entries = "\n".join(
        f"    FoldPair {{ from: 0x{source:04X}, to: 0x{target:04X} }},"
        for source, target in orbit
    )
    return f'''// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by scripts/generate-go-simple-case.py; DO NOT EDIT.
// Source: Go unicode.CaseRanges and caseOrbit, Unicode {version}.

/// Unicode version behind Go's exact simple-rune case mappings.
pub const GO_UNICODE_VERSION: &str = "{version}";

const UPPER_CASE: u8 = 0;
const LOWER_CASE: u8 = 1;
const UPPER_LOWER: i32 = 0x110000;

#[derive(Clone, Copy)]
struct CaseRange {{
    lo: u32,
    hi: u32,
    upper: i32,
    lower: i32,
}}

#[rustfmt::skip]
const CASE_RANGES: &[CaseRange] = &[
{entries}
];

#[derive(Clone, Copy)]
struct FoldPair {{
    from: u32,
    to: u32,
}}

/// Go `caseOrbit`: the simple-fold orbits `CaseRanges` cannot describe.
#[rustfmt::skip]
const CASE_ORBIT: &[FoldPair] = &[
{orbit_entries}
];

/// Applies Go `strings.ToUpper` simple-rune mappings without full-case expansion.
pub fn to_uppercase(value: &str) -> String {{
    map_string(value, UPPER_CASE)
}}

/// Applies Go `strings.ToLower` simple-rune mappings without full-case expansion.
pub fn to_lowercase(value: &str) -> String {{
    map_string(value, LOWER_CASE)
}}

fn map_string(value: &str, case: u8) -> String {{
    // Go strings.ToUpper/ToLower handles ASCII without Unicode table searches.
    if value.is_ascii() {{
        return if case == UPPER_CASE {{
            value.to_ascii_uppercase()
        }} else {{
            value.to_ascii_lowercase()
        }};
    }}
    let mut output = String::with_capacity(value.len());
    let mut raw = Vec::new();
    for character in value.chars() {{
        if character as u32 >= RAW_BYTE_ESCAPE_BASE + 0x80 {{
            raw.push((character as u32 - RAW_BYTE_ESCAPE_BASE) as u8);
            continue;
        }}
        push_raw_bytes(&mut output, &mut raw);
        output.push(map_character(character, case));
    }}
    push_raw_bytes(&mut output, &mut raw);
    output
}}

/// The character `tidb_hack::raw_bytes` carries Go string byte `b` (not part
/// of valid UTF-8) in is `U+10FF00 + b`, for `b` in `0x80..=0xFF`.
const RAW_BYTE_ESCAPE_BASE: u32 = 0x10FF00;

/// Go `strings.Map` over the bytes a run of raw-byte escapes stands for: a
/// byte that is not part of valid UTF-8 reads as `utf8.RuneError` and is
/// written as U+FFFD; a valid character (only one in the escape range is
/// ever escaped) has no case mapping and stays escaped.
fn push_raw_bytes(output: &mut String, raw: &mut Vec<u8>) {{
    for chunk in raw.utf8_chunks() {{
        for byte in chunk.valid().bytes() {{
            output.push(
                char::from_u32(RAW_BYTE_ESCAPE_BASE + u32::from(byte))
                    .expect("the raw-byte escapes are Unicode scalars"),
            );
        }}
        for _ in chunk.invalid() {{
            output.push(char::REPLACEMENT_CHARACTER);
        }}
    }}
    raw.clear();
}}

fn map_character(character: char, case: u8) -> char {{
    // Go unicode.ToUpper/ToLower keeps ASCII cheap inside mixed UTF-8 too.
    if character.is_ascii() {{
        return if case == UPPER_CASE {{
            character.to_ascii_uppercase()
        }} else {{
            character.to_ascii_lowercase()
        }};
    }}
    let codepoint = character as u32;
    let mut low = 0;
    let mut high = CASE_RANGES.len();
    while low < high {{
        let middle = low + (high - low) / 2;
        let range = CASE_RANGES[middle];
        if codepoint < range.lo {{
            high = middle;
        }} else if codepoint > range.hi {{
            low = middle + 1;
        }} else {{
            let delta = if case == UPPER_CASE {{
                range.upper
            }} else {{
                range.lower
            }};
            let mapped = if delta == UPPER_LOWER {{
                range.lo + ((codepoint - range.lo) & !1 | u32::from(case & 1))
            }} else {{
                codepoint.wrapping_add_signed(delta)
            }};
            return char::from_u32(mapped).expect("Go CaseRanges must map to a Unicode scalar");
        }}
    }}
    character
}}

/// Go `strings.ToLower(left) == strings.ToLower(right)`: the comparison two
/// `ast.CIStr.L`s (or a `CIStr.L` and a lowered written name) make. Unlike
/// [`equal_fold`], `İ` names `i` and `ſ` does not name `s`.
pub fn lowercase_equal(left: &str, right: &str) -> bool {{
    if left.is_ascii() && right.is_ascii() {{
        return left.eq_ignore_ascii_case(right);
    }}
    to_lowercase(left) == to_lowercase(right)
}}

/// Go `unicode.SimpleFold`: the next rune after `character` in its
/// simple-fold orbit, wrapping around to the smallest.
pub fn simple_fold(character: char) -> char {{
    let codepoint = character as u32;
    if let Ok(index) = CASE_ORBIT.binary_search_by_key(&codepoint, |pair| pair.from) {{
        return char::from_u32(CASE_ORBIT[index].to).expect("Go caseOrbit holds Unicode scalars");
    }}
    // No folding specified: the orbit is the rune with its ToLower and
    // ToUpper.
    let lower = map_character(character, LOWER_CASE);
    if lower != character {{
        return lower;
    }}
    map_character(character, UPPER_CASE)
}}

/// Go `strings.EqualFold`: equality under Unicode simple folding, which is
/// not `ToLower` equality (`ſ` folds with `s`, `İ` folds with nothing). A
/// byte that is not part of valid UTF-8 reads as `utf8.RuneError`.
pub fn equal_fold(left: &str, right: &str) -> bool {{
    if left.is_ascii() && right.is_ascii() {{
        return left.eq_ignore_ascii_case(right);
    }}
    let left = go_runes(left);
    let right = go_runes(right);
    left.len() == right.len()
        && left.iter().zip(&right).all(|(&left, &right)| {{
            if left == right {{
                return true;
            }}
            let (small, large) = if left < right {{
                (left, right)
            }} else {{
                (right, left)
            }};
            if large.is_ascii() {{
                return small.is_ascii_uppercase() && large == small.to_ascii_lowercase();
            }}
            // SimpleFold(x) returns the next equivalent rune > x or wraps
            // around to smaller values.
            let mut folded = simple_fold(small);
            while folded != small && folded < large {{
                folded = simple_fold(folded);
            }}
            folded == large
        }})
}}

/// The runes Go's `range` over `value` yields: a run of raw-byte escapes is
/// decoded as UTF-8, each byte not part of a valid sequence reading as
/// `utf8.RuneError`.
fn go_runes(value: &str) -> Vec<char> {{
    let mut runes = Vec::with_capacity(value.len());
    let mut raw = Vec::new();
    for character in value.chars() {{
        if character as u32 >= RAW_BYTE_ESCAPE_BASE + 0x80 {{
            raw.push((character as u32 - RAW_BYTE_ESCAPE_BASE) as u8);
            continue;
        }}
        push_raw_runes(&mut runes, &mut raw);
        runes.push(character);
    }}
    push_raw_runes(&mut runes, &mut raw);
    runes
}}

fn push_raw_runes(runes: &mut Vec<char>, raw: &mut Vec<u8>) {{
    for chunk in raw.utf8_chunks() {{
        runes.extend(chunk.valid().chars());
        runes.extend(chunk.invalid().iter().map(|_| char::REPLACEMENT_CHARACTER));
    }}
    raw.clear();
}}
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--go-root", type=Path)
    args = parser.parse_args()

    source = (go_root(args.go_root) / "src/unicode/tables.go").read_text(
        encoding="utf-8"
    )
    version, rows = parse_case_ranges(source)
    orbit = parse_case_orbit(source)
    generated = render(version, rows, orbit)
    oracle = render_oracle(version, rows)
    if args.check:
        for path, expected in ((OUTPUT, generated), (ORACLE, oracle)):
            if not path.is_file() or path.read_text(encoding="utf-8") != expected:
                print(
                    f"{path.relative_to(REPO_ROOT)} is stale; regenerate with "
                    "python3 rust/scripts/generate-go-simple-case.py",
                    file=sys.stderr,
                )
                return 1
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(generated, encoding="utf-8")
    ORACLE.parent.mkdir(parents=True, exist_ok=True)
    ORACLE.write_text(oracle, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
