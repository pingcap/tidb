#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Generate/check pkg/parser/mysql Unicode membership ranges for Rust.

The Go package intentionally delegates its identifier graph and locale digit
semantics to the Go toolchain's unicode tables. Pinning both the toolchain and
source digests makes a Unicode-version change explicit instead of silently
changing the Rust transcreation.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import tempfile
from pathlib import Path


EXPECTED_GO_VERSION = "go1.26.0"
EXPECTED_TABLES_SHA256 = "6a88d48746c4b2c471cc04ae40559694b734770212e6f5ea5f1962adb0c0bdb1"
EXPECTED_DIGIT_SHA256 = "dfb14dbbcb918a6c7ee9be15293327d022f9d64a4c1a90f9c8049b682e799761"
EXPECTED_ISPRINT_SHA256 = "24114557c2b3cad7b80c8e71683831bfbe1c72eb99e8fd28a3a48eaf77705fb4"
EXPECTED_RANGE_GRAPH_SHA256 = "7286d3b1ca2d1143c43775660ad2e08eba66f4a132cfa91c88dd583b54e490f6"
BEGIN = "// BEGIN GENERATED PARSER MYSQL UNICODE RANGES"
END = "// END GENERATED PARSER MYSQL UNICODE RANGES"
PRINT_BEGIN = "// BEGIN GENERATED GO STRCONV ISPRINT RANGES"
PRINT_END = "// END GENERATED GO STRCONV ISPRINT RANGES"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_output(*command: str) -> str:
    return subprocess.run(command, check=True, text=True, capture_output=True).stdout.strip()


def verify_sources(repo: Path) -> list[str]:
    version = checked_output("go", "env", "GOVERSION")
    if version != EXPECTED_GO_VERSION:
        raise SystemExit(f"Go version drift: expected {EXPECTED_GO_VERSION}, got {version}")
    goroot = Path(checked_output("go", "env", "GOROOT"))
    for path, expected in (
        (goroot / "src/unicode/tables.go", EXPECTED_TABLES_SHA256),
        (goroot / "src/unicode/digit.go", EXPECTED_DIGIT_SHA256),
        (goroot / "src/strconv/isprint.go", EXPECTED_ISPRINT_SHA256),
    ):
        actual = sha256(path)
        if actual != expected:
            raise SystemExit(f"Unicode source drift in {path}: expected {expected}, got {actual}")

    source = (repo / "pkg/parser/mysql/charset.go").read_text()
    match = re.search(
        r"var RangeGraph = \[\]\*unicode\.RangeTable\{.*?\n\}", source, re.DOTALL
    )
    if match is None:
        raise SystemExit("cannot locate pkg/parser/mysql RangeGraph")
    actual = hashlib.sha256(match.group(0).encode()).hexdigest()
    if actual != EXPECTED_RANGE_GRAPH_SHA256:
        raise SystemExit(
            "RangeGraph source drift: "
            f"expected {EXPECTED_RANGE_GRAPH_SHA256}, got {actual}"
        )
    return re.findall(r"unicode\.([A-Za-z0-9_]+),", match.group(0))


def generate(repo: Path) -> str:
    names = verify_sources(repo)
    graph = ", ".join(f"unicode.{name}" for name in names)
    go_source = f'''package main

import (
    "fmt"
    "unicode"
)

var graph = []*unicode.RangeTable{{{graph}}}

func emit(name string, predicate func(rune) bool) {{
    type span struct {{ lo, hi rune }}
    var spans []span
    inside := false
    var lo rune
    for r := rune(0); r <= unicode.MaxRune; r++ {{
        matched := predicate(r)
        if matched && !inside {{ lo, inside = r, true }}
        if inside && (!matched || r == unicode.MaxRune) {{
            hi := r - 1
            if matched && r == unicode.MaxRune {{ hi = r }}
            spans = append(spans, span{{lo, hi}})
            inside = false
        }}
    }}
    fmt.Printf("const %s: &[(u32, u32)] = &[\\n", name)
    for _, span := range spans {{ fmt.Printf("    (0x%X, 0x%X),\\n", span.lo, span.hi) }}
    fmt.Printf("]; // %d ranges\\n", len(spans))
}}

func main() {{
    emit("RANGE_GRAPH_CODEPOINTS", func(r rune) bool {{ return unicode.IsOneOf(graph, r) }})
    emit("UNICODE_DECIMAL_DIGITS", unicode.IsDigit)
}}
'''
    with tempfile.TemporaryDirectory(prefix="parser-mysql-unicode-") as directory:
        source = Path(directory) / "main.go"
        source.write_text(go_source)
        body = checked_output("go", "run", source.as_posix())
    return f"{BEGIN}\n{body}\n\n{END}"


def generate_go_is_print() -> str:
    goroot = Path(checked_output("go", "env", "GOROOT"))
    source = (goroot / "src/strconv/isprint.go").read_text()

    def rust_array(go_name: str, rust_name: str, kind: str) -> str:
        go_kind = "uint" + kind.removeprefix("u")
        match = re.search(
            rf"var {go_name} = \[\]{go_kind}\{{(.*?)\n\}}", source, re.DOTALL
        )
        if match is None:
            raise SystemExit(f"cannot locate Go strconv table {go_name}")
        body = re.sub(r"//.*", "", match.group(1))
        values = re.findall(r"0x[0-9a-fA-F]+", body)
        rows = [", ".join(values[index : index + 8]) for index in range(0, len(values), 8)]
        return f"const {rust_name}: &[{kind}] = &[\n    " + ",\n    ".join(rows) + ",\n];"

    tables = "\n\n".join(
        (
            rust_array("isPrint16", "GO_IS_PRINT16", "u16"),
            rust_array("isNotPrint16", "GO_IS_NOT_PRINT16", "u16"),
            rust_array("isPrint32", "GO_IS_PRINT32", "u32"),
            rust_array("isNotPrint32", "GO_IS_NOT_PRINT32", "u16"),
        )
    )
    generated = f'''{PRINT_BEGIN}
// Generated from Go `strconv.IsPrint` for parser/mysql fmt parity.
{tables}

fn go_is_print(character: char) -> bool {{
    let value = u32::from(character);
    if value <= 0xff {{
        return (0x20..=0x7e).contains(&value)
            || ((0xa1..=0xff).contains(&value) && value != 0xad);
    }}
    if value < 1 << 16 {{
        let value = value as u16;
        let index = GO_IS_PRINT16.partition_point(|candidate| *candidate < value);
        if index >= GO_IS_PRINT16.len()
            || value < GO_IS_PRINT16[index & !1]
            || GO_IS_PRINT16[index | 1] < value
        {{
            return false;
        }}
        return GO_IS_NOT_PRINT16.binary_search(&value).is_err();
    }}
    let index = GO_IS_PRINT32.partition_point(|candidate| *candidate < value);
    if index >= GO_IS_PRINT32.len()
        || value < GO_IS_PRINT32[index & !1]
        || GO_IS_PRINT32[index | 1] < value
    {{
        return false;
    }}
    if value >= 0x20000 {{
        return true;
    }}
    GO_IS_NOT_PRINT32
        .binary_search(&((value - 0x10000) as u16))
        .is_err()
}}
{PRINT_END}
'''
    with tempfile.TemporaryDirectory(prefix="parser-mysql-rustfmt-") as directory:
        target = Path(directory) / "generated.rs"
        target.write_text(generated)
        subprocess.run(("rustfmt", target.as_posix()), check=True)
        return target.read_text().strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail instead of updating")
    args = parser.parse_args()

    rust_root = Path(__file__).resolve().parents[1]
    repo = rust_root.parent
    target = rust_root / "crates/tidb-mysql/src/charset.rs"
    generated = generate(repo)
    current = target.read_text()
    pattern = re.compile(re.escape(BEGIN) + r".*?" + re.escape(END), re.DOTALL)
    match = pattern.search(current)
    if match is None:
        raise SystemExit(f"generated markers missing from {target}")
    if match.group(0) == generated:
        pass
    elif args.check:
        raise SystemExit(f"generated Unicode ranges are stale in {target}")
    else:
        target.write_text(pattern.sub(generated, current, count=1))

    print_target = rust_root / "crates/tidb-error/src/mysql/error.rs"
    print_generated = generate_go_is_print()
    print_current = print_target.read_text()
    print_pattern = re.compile(
        re.escape(PRINT_BEGIN) + r".*?" + re.escape(PRINT_END), re.DOTALL
    )
    print_match = print_pattern.search(print_current)
    if print_match is None:
        raise SystemExit(f"generated Go IsPrint markers missing from {print_target}")
    if print_match.group(0) == print_generated.strip():
        return
    if args.check:
        raise SystemExit(f"generated Go IsPrint ranges are stale in {print_target}")
    print_target.write_text(print_pattern.sub(print_generated.strip(), print_current, count=1))


if __name__ == "__main__":
    main()
