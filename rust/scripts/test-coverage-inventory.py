#!/usr/bin/env python3
"""Go-test -> Rust-test coverage inventory for the TiDB transcreation.

Run from the repository root:

    python3 rust/scripts/test-coverage-inventory.py            # rewrite the doc
    python3 rust/scripts/test-coverage-inventory.py --check    # fail if stale
    python3 rust/scripts/test-coverage-inventory.py --json     # raw records

Output: rust/docs/operations/test-coverage-inventory.md

The Go->Rust package mapping is NOT guessed. Every row carries a `source`
field naming where the mapping was read from:

  README     rust/README.md workspace table
  WORKSPACE  rust/docs/architecture/workspace.md table
  GITLOG     a `rust: transcreate pkg/...` commit message
  LAYOUT     a Rust module/file whose path is the Go package name, confirmed
             by reading the module header

Matching evidence is NAME-BASED unless a human recorded otherwise. That is
weak evidence and the generated doc says so on every row. Do not read a high
ratio here as "this package is covered"; read a low ratio as "this package is
definitely not covered".
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOC = REPO / "rust" / "docs" / "operations" / "test-coverage-inventory.md"

# --------------------------------------------------------------------------
# Mapping: Go package directory -> Rust source paths that transcreate it.
# `risk` ranks blast radius of a hole here: 3 = wire/txn/encoding/type
# semantics, 2 = planning/session/exec behavior, 1 = leaf utility.
# --------------------------------------------------------------------------


@dataclass
class Mapping:
    go_pkg: str
    rust_paths: list[str]
    source: str
    risk: int
    note: str = ""


MAPPINGS: list[Mapping] = [
    # ---- ring 3: wire/encoding/type semantics -----------------------------
    Mapping("pkg/types", ["rust/crates/tidb-datatype/src"], "WORKSPACE", 3,
            "one Go package -> many Rust modules; datatype also owns pkg/parser/charset and pkg/util/collate"),
    Mapping("pkg/types/parser_driver", ["rust/crates/tidb-datatype/src"], "WORKSPACE", 3),
    Mapping("pkg/parser/charset", ["rust/crates/tidb-datatype/src/charset.rs",
                                   "rust/crates/tidb-datatype/src/charset_data.rs",
                                   "rust/crates/tidb-datatype/src/encoding_table.rs",
                                   "rust/crates/tidb-datatype/src/encoding_base.rs",
                                   "rust/crates/tidb-datatype/src/encoding_labels.rs",
                                   "rust/crates/tidb-datatype/src/utf8_encoding.rs",
                                   "rust/crates/tidb-datatype/src/ascii_encoding.rs",
                                   "rust/crates/tidb-datatype/src/multibyte_encoding.rs"],
            "README", 3),
    Mapping("pkg/util/collate", ["rust/crates/tidb-datatype/src/collation.rs",
                                 "rust/crates/tidb-datatype/src/collation_data",
                                 "rust/crates/tidb-datatype/src/collation_tests.rs"],
            "README", 3),
    Mapping("pkg/util/codec", ["rust/crates/tidb-codec/src"], "WORKSPACE", 3),
    Mapping("pkg/util/rowcodec", ["rust/crates/tidb-codec/src/rowcodec.rs",
                                  "rust/crates/tidb-codec/src/row_decoder.rs",
                                  "rust/crates/tidb-codec/src/row_encoder.rs",
                                  "rust/crates/tidb-codec/src/row_index.rs",
                                  "rust/crates/tidb-codec/src/row_layout.rs"],
            "GITLOG", 3),
    Mapping("pkg/tablecodec", ["rust/crates/tidb-tablecodec/src",
                               "rust/crates/tidb-codec/src/table_key.rs"], "WORKSPACE", 3),
    Mapping("pkg/kv", ["rust/crates/tidb-txnkv/src"], "WORKSPACE", 3),
    Mapping("pkg/store/driver/txn", ["rust/crates/tidb-txnkv/src/driver",
                                     "rust/crates/tidb-txnkv/src/transaction"], "WORKSPACE", 3),
    Mapping("pkg/store/mockstore/unistore", ["rust/crates/tidb-txnkv/src/unistore.rs",
                                             "rust/crates/tidb-txnkv/src/mem_storage.rs"], "WORKSPACE", 2),
    Mapping("pkg/parser/mysql", ["rust/crates/tidb-mysql/src"], "WORKSPACE", 3),
    Mapping("pkg/util/chunk", ["rust/crates/tidb-chunk/src"], "LAYOUT", 3),
    Mapping("pkg/util/hack", ["rust/crates/tidb-hack/src"], "README", 2),

    # ---- ring 2: parser / expression / planner / exec ----------------------
    Mapping("pkg/parser", ["rust/crates/tidb-parser/src", "rust/crates/tidb-lexer/src"],
            "WORKSPACE", 3, "one Go package -> two Rust crates (lexer + grammar)"),
    # `tidb-ast` holds the node structs; every ported `pkg/parser/ast/*_test.go`
    # restore/visitor test landed in `tidb-parser/src/tests/` beside the grammar
    # that produces the node. Mapping only `tidb-ast` reported 73/73 of this
    # package's tests as `NONE` while 73/73 of them exist there by exact name.
    Mapping("pkg/parser/ast", ["rust/crates/tidb-ast/src",
                               "rust/crates/tidb-parser/src/tests"], "LAYOUT", 2),
    Mapping("pkg/meta/model", ["rust/crates/tidb-model/src"], "GITLOG", 2),
    # Most of the evidence for pkg/expression is not a Rust `#[test]` at all:
    # it is the per-topic differential corpus, whose headers cite the Go test
    # they transcreate and whose goldens are captured from the Go engine. Search
    # it too, or this package's row systematically under-reports itself.
    Mapping("pkg/expression", ["rust/crates/tidb-expr/src",
                               "rust/difftests/corpus/expr",
                               "rust/difftests/result-tests/tests"],
            "WORKSPACE", 3),
    Mapping("pkg/planner/core", ["rust/crates/tidb-planner/src",
                                 "rust/crates/tidb-session/src"], "WORKSPACE", 2),
    Mapping("pkg/planner/util", ["rust/crates/tidb-planner/src"], "WORKSPACE", 2),
    Mapping("pkg/planner/cardinality", ["rust/crates/tidb-planner/src",
                                        "rust/crates/tidb-stats/src"], "WORKSPACE", 2),
    Mapping("pkg/util/ranger", ["rust/crates/tidb-executor/src/index_range.rs",
                                "rust/crates/tidb-planner/src"], "GITLOG", 3),
    # `tidb-session/src/tests_*.rs` is where this tree ports a Go executor or
    # planner test that is only expressible as SQL against a live engine
    # (`tests_dml_lock_keys.rs` <- `pkg/executor/{insert,delete}_test.go`,
    # `tests_join_predicate_placement.rs` <- `pkg/planner/core/logical_plans_test.go`).
    # Without it those ports scored `REFERENCED` at best.
    Mapping("pkg/executor", ["rust/crates/tidb-exec/src", "rust/crates/tidb-executor/src",
                             "rust/crates/tidb-session/src"],
            "WORKSPACE", 2, "one Go package -> two Rust crates (seed exec + typed executor)"),
    Mapping("pkg/executor/aggfuncs", ["rust/crates/tidb-exec/src/aggregate",
                                      "rust/crates/tidb-exec/src/aggregate.rs"], "LAYOUT", 2),
    Mapping("pkg/session", ["rust/crates/tidb-session/src", "rust/crates/tidb-exec/src"],
            "WORKSPACE", 2),
    Mapping("pkg/sessionctx/variable", ["rust/crates/tidb-vardef/src",
                                        "rust/crates/tidb-exec/src"], "GITLOG", 2),
    Mapping("pkg/sessionctx/vardef", ["rust/crates/tidb-vardef/src"], "GITLOG", 2),
    Mapping("pkg/distsql", ["rust/crates/tidb-distsql/src"], "WORKSPACE", 2),
    Mapping("pkg/statistics", ["rust/crates/tidb-stats/src"], "LAYOUT", 2),
    Mapping("pkg/server", ["rust/crates/tidb-server/src", "rust/crates/tidb-protocol/src"],
            "WORKSPACE", 3, "one Go package -> two Rust crates (process + wire protocol)"),
    Mapping("pkg/meta", ["rust/crates/tidb-meta/src"], "GITLOG", 2),
    Mapping("pkg/meta/metadef", ["rust/crates/tidb-metadef/src"], "GITLOG", 1),
    Mapping("pkg/config", ["rust/crates/tidb-config/src"], "GITLOG", 1),
    Mapping("pkg/errno", ["rust/crates/tidb-error/src"], "LAYOUT", 1),

    # ---- ring 1: leaf utilities (one Go pkg -> one tidb-util module) -------
    *[Mapping(f"pkg/util/{g}", [f"rust/crates/tidb-util/src/{r}"], "GITLOG", 1)
      for g, r in [
          ("arena", "arena.rs"), ("backoff", "backoff.rs"), ("bitmap", "bitmap.rs"),
          ("column-mapping", "column_mapping.rs"), ("context", "context"),
          ("dbterror", "dbterror"), ("disjointset", "disjointset"),
          ("encrypt", "encrypt"), ("fastrand", "fastrand"), ("filter", "filter"),
          ("generic", "generic"), ("globalconn", "globalconn"), ("intest", "intest"),
          ("intset", "intset.rs"), ("israce", "israce"), ("logutil", "logutil"),
          ("mathutil", "mathutil"), ("memory", "memory"), ("mvmap", "mvmap"),
          ("naming", "naming"), ("nocopy", "nocopy"), ("paging", "paging.rs"),
          ("partialjson", "partialjson.rs"), ("ppcpuusage", "ppcpuusage.rs"),
          ("prefetch", "prefetch.rs"), ("queue", "queue.rs"), ("redact", "redact.rs"),
          ("selection", "selection.rs"), ("sem", "sem.rs"), ("size", "size"),
          ("slice", "slice.rs"), ("sqlescape", "sqlescape"), ("sqlkiller", "sqlkiller.rs"),
          ("table-filter", "table_filter"), ("table-rule-selector", "table_rule_selector.rs"),
          ("texttree", "texttree.rs"), ("tikvutil", "tikvutil"), ("timeutil", "timeutil"),
          ("trxevents", "../../tidb-txnkv/src/trxevents.rs"), ("vitess", "vitess.rs"),
          ("versioninfo", "versioninfo.rs"), ("watcher", "watcher.rs"),
          ("zeropool", "zeropool"), ("checksum", "checksum"),
          ("password-validation", "../../tidb-exec/src/password_validation.rs"),
      ]],
    Mapping("pkg/util/dbterror/exeerrors", ["rust/crates/tidb-util/src/dbterror"], "GITLOG", 1),
]

# Extra Rust roots always searched for a name reference (comments citing the
# Go test that a Rust vector came from).
REFERENCE_ROOTS = ["rust/crates", "rust/difftests"]

RUST_TEST = re.compile(
    r"#\[(?:tokio::)?test[^\]]*\][^\n]*\n(?:\s*#\[[^\]]*\][^\n]*\n)*\s*(?:async\s+)?fn\s+([a-zA-Z0-9_]+)")


def norm(name: str) -> str:
    n = re.sub(r"[^a-z0-9]", "", name.lower())
    return n[4:] if n.startswith("test") else n


def stem(token: str) -> str:
    return token[:-1] if len(token) > 3 and token.endswith("s") else token


def tokens(name: str) -> list[str]:
    """CamelCase / snake_case -> stemmed lowercase tokens of length >= 3.

    Rust ports are routinely renamed to a sentence describing the behavior
    (`TestUnionIterErrors` -> `test_union_iter_source_error_identity_order_
    and_close`), which no substring rule catches. Token containment does.
    """
    raw = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).replace(".", "_")
    out = [stem(t.lower()) for t in raw.split("_") if len(t) >= 3]
    return [t for t in out if t != "test"]


@dataclass
class GoTest:
    pkg: str
    name: str
    kind: str  # top | suite | subtest
    file: str
    line: int


@dataclass
class PkgReport:
    m: Mapping
    go_tests: list[GoTest] = field(default_factory=list)
    rust_tests: list[str] = field(default_factory=list)
    matches: dict = field(default_factory=dict)  # go name -> (evidence, rust name)
    missing_rust: bool = False
    missing_go: bool = False
    cited_files: set = field(default_factory=set)
    near: dict = field(default_factory=dict)  # NONE go name -> nearest rust name


GO_TOOL = "./rust/difftests/tools/go_test_declaration_inventory"
# Categories that are a real coverage obligation. `TestMain` is process setup,
# `Benchmark`/`Fuzz` are explicitly out of scope for this inventory.
GO_CATEGORIES = {"Test", "TestSuiteMethod"}


def load_go_declarations(cache: Path | None) -> dict[str, list[GoTest]]:
    """Go side comes from the repo's own go/ast enumerator, not from regex.

    `rust/difftests/tools/go_test_declaration_inventory` already parses every
    `*_test.go` with go/parser and resolves testify suite methods to their
    running parents. Reusing it means the Go side of this inventory cannot be
    fooled by comments or string literals, and cannot drift from the tool the
    rest of the transcreation process already trusts.
    """
    if cache and cache.exists():
        tsv = cache.read_text()
    else:
        proc = subprocess.run(["go", "run", GO_TOOL, "-root", "."], cwd=REPO,
                              capture_output=True, text=True)
        if proc.returncode != 0:
            print(proc.stderr, file=sys.stderr)
            raise SystemExit("go_test_declaration_inventory failed")
        tsv = proc.stdout
        if cache:
            cache.write_text(tsv)
    by_pkg: dict[str, list[GoTest]] = {}
    for line in tsv.split("\n"):
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 8:
            continue
        path, lineno, _col, receiver, name, category, actionable, parents = parts[:8]
        if actionable != "true" or category not in GO_CATEGORIES:
            continue
        pkg = os.path.dirname(path)
        label = name if receiver == "function" else f"{receiver}.{name}"
        by_pkg.setdefault(pkg, []).append(
            GoTest(pkg, label, category, os.path.basename(path), int(lineno)))
    return by_pkg


def owning_crate_tests(p: Path) -> Path | None:
    """Most Rust tests live in `crates/<c>/tests/`, not next to the module.

    Any mapped path pulls in its crate's whole `tests/` directory. That is
    deliberately over-generous for fine-grained (single-module) mappings: it
    can only move a Go test OUT of the `NONE` bucket, never into it, so the
    uncovered work list stays conservative.
    """
    for anc in [p] + list(p.parents):
        if anc.parent.name == "crates" and (anc / "tests").is_dir():
            return anc / "tests"
    return None


def collect_rust(m: Mapping) -> tuple[list[str], bool]:
    names: list[str] = []
    missing = False
    roots: list[Path] = []
    for rp in m.rust_paths:
        p = (REPO / rp).resolve()
        if not p.exists():
            missing = True
            continue
        roots.append(p)
        t = owning_crate_tests(p)
        if t is not None:
            roots.append(t)
    for p in roots:
        files = [p] if p.is_file() else sorted(p.rglob("*.rs"))
        for f in files:
            names += RUST_TEST.findall(f.read_text(errors="replace"))
    return sorted(set(names)), missing


def build_reference_index() -> tuple[str, dict[str, set[str]]]:
    """Scan the Rust tree once.

    Returns the raw text blob (for `is this Go test name mentioned anywhere?`)
    and a per-Go-package set of cited `*_test.go` basenames. The convention in
    this tree is that a ported Go test file lands as `<name>_source.rs` whose
    module doc names the Go `_test.go` file it was derived from, so a file
    citation is stronger provenance than any name match: somebody read that
    file. It is still file-level, not test-level.
    """
    chunks: list[str] = []
    cited: dict[str, set[str]] = {}
    pkg_re = re.compile(r"\b(pkg/[a-zA-Z0-9_/.-]+)/([a-zA-Z0-9_]+_test\.go)\b")
    # `.txt` is here because the differential corpora are provenance too: each
    # `corpus/<ns>/<topic>.txt` header names the Go test its rows were
    # transcreated from. Their `.golden.txt` partners are machine-written label
    # dumps with no prose, so reading them could only add noise.
    # `.py` and `.tsv` are here for the same reason `.txt` is: a generator
    # script and the `corpus/coverage/*.tsv` inventories both name the Go test
    # whose obligation they carry, and an extension filter is not a judgement
    # about provenance. `rust/docs/` is deliberately NOT a reference root --
    # this script writes the uncovered list there, so scanning it would let
    # every `NONE` certify itself as `REFERENCED` on the next run.
    for root in REFERENCE_ROOTS:
        for pat in ("*.rs", "*.md", "*.txt", "*.py", "*.tsv"):
            for f in (REPO / root).rglob(pat):
                if f.name.endswith(".golden.txt"):
                    continue
                text = f.read_text(errors="replace")
                chunks.append(text)
                for pkg, base in pkg_re.findall(text):
                    cited.setdefault(pkg, set()).add(base)
    return "\n".join(chunks), cited


def match(go: list[GoTest], rust: list[str], refblob: str) -> dict:
    by_norm: dict[str, str] = {}
    for r in rust:
        by_norm.setdefault(norm(r), r)
    res = {}
    for g in go:
        gn = norm(g.name.split(".")[-1])
        if gn in by_norm:
            res[g.name] = ("NAME-EXACT", by_norm[gn])
            continue
        cand = [r for k, r in by_norm.items()
                if len(gn) >= 6 and (gn in k or (len(k) >= 6 and k in gn))]
        if cand:
            res[g.name] = ("NAME-FUZZY", cand[0])
            continue
        gt = tokens(g.name.split(".")[-1])
        tok = [r for r in rust
               if len(gt) >= 2 and all(t in "_".join(tokens(r)) for t in gt)]
        if tok:
            res[g.name] = ("NAME-TOKENS", tok[0])
        elif g.name.split(".")[-1] in refblob:
            res[g.name] = ("REFERENCED", "(cited in Rust comment/doc only)")
        else:
            res[g.name] = ("NONE", "")
    return res


def near_misses(matches: dict, rust: list[str]) -> dict:
    """For each `NONE`, the nearest Rust test: every Go word but one.

    This is a REVIEW QUEUE, not an evidence tier, and it is not counted
    anywhere. It exists because `pkg/types/convert_test.go` `TestGetValidFloat`
    sat in `NONE` -- and was ranked as an unguarded behavior in the companion
    doc -- while `convert.rs::source_valid_float_prefix_rows` carried all 23 of
    its rows. The only thing between them was the Go verb `Get`. Loosening the
    match rule to close that was measured and rejected: it converts 355 rows
    tree-wide and most of them are coincidences (`TestMakeRefTo` "matching" a
    cache-refresh test). So the near miss is printed for a human to read and
    the Go test stays uncovered until one does.
    """
    rust_tokens = {r: "_".join(tokens(r)) for r in rust}
    out = {}
    for go_name, (evidence, _) in matches.items():
        if evidence != "NONE":
            continue
        want = tokens(go_name.split(".")[-1])
        if len(want) < 2:
            continue
        for r in rust:
            if sum(1 for t in want if t not in rust_tokens[r]) == 1:
                out[go_name] = r
                break
    return out


def run(cache: Path | None = None) -> list[PkgReport]:
    refblob, cited = build_reference_index()
    go_by_pkg = load_go_declarations(cache)
    reports = []
    for m in MAPPINGS:
        r = PkgReport(m)
        r.go_tests = go_by_pkg.get(m.go_pkg, [])
        r.missing_go = not (REPO / m.go_pkg).is_dir()
        r.rust_tests, r.missing_rust = collect_rust(m)
        r.matches = match(r.go_tests, r.rust_tests, refblob)
        r.cited_files = cited.get(m.go_pkg, set())
        r.near = near_misses(r.matches, r.rust_tests)
        reports.append(r)
    return reports


def ratio(r: PkgReport) -> tuple[int, int, int, int]:
    e = sum(1 for v in r.matches.values() if v[0] == "NAME-EXACT")
    f = sum(1 for v in r.matches.values()
            if v[0] in ("NAME-FUZZY", "NAME-TOKENS"))
    ref = sum(1 for v in r.matches.values() if v[0] == "REFERENCED")
    n = sum(1 for v in r.matches.values() if v[0] == "NONE")
    return e, f, ref, n


def render(reports: list[PkgReport]) -> str:
    rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO,
                         capture_output=True, text=True).stdout.strip()
    L = []
    A = L.append
    A("# Go test -> Rust test coverage inventory")
    A("")
    A(f"Generated by `rust/scripts/test-coverage-inventory.py` at `{rev}`. Do not hand-edit;")
    A("regenerate with `python3 rust/scripts/test-coverage-inventory.py` from the repo root.")
    A("")
    A("Ranked gaps and the honest confidence accounting live in the hand-written")
    A("companion [`test-coverage-gaps.md`](test-coverage-gaps.md). Read that first.")
    A("")
    A("## How to read this")
    A("")
    A("`AGENTS.md` non-negotiable 6 makes a package claim include **every** original")
    A("test artifact. Production code ported without its tests is a partial port. This")
    A("file measures that gap. It is deliberately pessimistic about its own evidence:")
    A("")
    A("| Evidence | Meaning | Strength |")
    A("| --- | --- | --- |")
    A("| `NAME-EXACT` | a Rust test's name normalizes to the Go test's name | WEAK. Same name is not same assertion. |")
    A("| `NAME-FUZZY` | one normalized name contains the other, or every word of the Go name appears in the Rust name | VERY WEAK. False positives on short names; catches the common case where a port was renamed to a sentence (`TestUnionIterErrors` -> `test_union_iter_source_error_identity_order_and_close`). |")
    A("| `REFERENCED` | the Go test name appears in Rust source/doc text but no test carries it | WEAK-NEGATIVE. Usually a citation, not a port. |")
    A("| `NONE` | no name, no citation | STRONG NEGATIVE. Treat as uncovered. |")
    A("")
    A("**A high match ratio here does not mean a package is covered.** Only the `NONE`")
    A("column is trustworthy, and it is trustworthy in one direction: those Go tests")
    A("have no Rust counterpart under any name. Rows above the line are candidates for")
    A("a human behavior check, not evidence of parity.")
    A("")
    A("Go tests counted: top-level `func TestXxx(t *testing.T)` and testify suite")
    A("methods `func (s *Suite) TestXxx(...)`. Table-driven subtests are *not* counted")
    A("as separate units -- they inflate both sides and are not separately addressable")
    A("in Rust. Benchmarks and fuzz targets are out of scope. Consequence, and the")
    A("largest systematic bias here: a Go test with a 200-row table counts once, so a")
    A("Rust test porting one of its rows matches at full credit. Coverage is therefore")
    A("**overstated** wherever Go used a table -- which is most of `pkg/types` and")
    A("`pkg/parser`.")
    A("")

    tot_go = sum(len(r.go_tests) for r in reports)
    te = tf = tr = tn = 0
    for r in reports:
        e, f, ref, n = ratio(r)
        te, tf, tr, tn = te + e, tf + f, tr + ref, tn + n
    A("## Totals")
    A("")
    A(f"- Go tests in mapped packages: **{tot_go}**")
    A(f"- `NAME-EXACT`: **{te}** ({te*100//max(tot_go,1)}%)")
    A(f"- `NAME-FUZZY`: **{tf}** ({tf*100//max(tot_go,1)}%)")
    A(f"- `REFERENCED` only: **{tr}** ({tr*100//max(tot_go,1)}%)")
    A(f"- `NONE` (uncovered under any name): **{tn}** ({tn*100//max(tot_go,1)}%)")
    A(f"- Rust tests found in mapped paths: **{sum(len(r.rust_tests) for r in reports)}**")
    A("")
    A("## Per-package table")
    A("")
    A("`risk` 3 = wire/transaction/encoding/type semantics, 2 = planning/session/")
    A("execution behavior, 1 = leaf utility. `src` records where the Go->Rust mapping")
    A("was read from (README / WORKSPACE / GITLOG / LAYOUT), never guessed.")
    A("")
    A("`files cited` is the stronger signal: Go `*_test.go` files this package has,")
    A("versus how many are named by a Rust test file (the `*_source.rs` convention).")
    A("A cited file means somebody read that Go file; it does not mean every test in")
    A("it was ported.")
    A("")
    A("| Go package | Rust target | src | risk | Go tests | exact | fuzzy | ref | NONE | uncovered | files cited |")
    A("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in sorted(reports, key=lambda x: (-x.m.risk, -ratio(x)[3])):
        e, f, ref, n = ratio(r)
        g = len(r.go_tests)
        all_files = {t.file for t in r.go_tests}
        files = f"{len(r.cited_files & all_files)}/{len(all_files)}" if all_files else "n/a"
        pct = f"{(ref+n)*100//g}%" if g else "n/a"
        tgt = ", ".join(Path(p).parts[2] if p.startswith("rust/crates") else p
                        for p in r.m.rust_paths[:2])
        tgt = tgt + ("+" if len(r.m.rust_paths) > 2 else "")
        flag = " **MISSING RUST PATH**" if r.missing_rust else ""
        if r.missing_go:
            flag += " **GO PKG GONE**"
        A(f"| `{r.m.go_pkg}` | {tgt}{flag} | {r.m.source} | {r.m.risk} | {g} | {e} | {f} | {ref} | {n} | {pct} | {files} |")
    A("")
    A("Notes on non-1:1 mappings:")
    A("")
    for r in reports:
        if r.m.note:
            A(f"- `{r.m.go_pkg}`: {r.m.note}")
    A("")
    A("## Uncovered Go tests (`NONE`), by package")
    A("")
    A("These have no Rust test under any name and no citation anywhere in the Rust")
    A("tree. This is the work list.")
    A("")
    for r in sorted(reports, key=lambda x: (-x.m.risk, x.m.go_pkg)):
        none = [g for g in r.go_tests if r.matches[g.name][0] == "NONE"]
        if not none:
            continue
        A(f"### `{r.m.go_pkg}` (risk {r.m.risk}) -- {len(none)} uncovered")
        A("")
        for g in sorted(none, key=lambda x: (x.file, x.line)):
            A(f"- `{g.name}` -- `{r.m.go_pkg}/{g.file}:{g.line}`")
        A("")

    A("## Near-miss review queue (NOT coverage)")
    A("")
    A("Every entry below is still counted as `NONE` above. These are `NONE` Go")
    A("tests where some Rust test in the same mapped paths shares all but one of")
    A("the Go name's words. **Read one before porting it**, because that is how")
    A("`TestGetValidFloat` -- ranked as an unguarded behavior in the companion")
    A("doc -- stayed uncovered on paper while `convert.rs`")
    A("`source_valid_float_prefix_rows` carried all 23 of its rows: the only")
    A("difference was the Go verb `Get`.")
    A("")
    A("Most rows here are coincidences. Accepting them automatically was measured")
    A("(355 tree-wide) and rejected: a rule that turns `TestMakeRefTo` into")
    A("`a_refresh_makes_a_stale_cache_usable_again` manufactures parity, which is")
    A("worse than overstating the gap.")
    A("")
    for r in sorted(reports, key=lambda x: (-x.m.risk, x.m.go_pkg)):
        if not r.near or r.m.risk < 2:
            continue
        A(f"### `{r.m.go_pkg}` (risk {r.m.risk}) -- {len(r.near)} to read")
        A("")
        for go_name, rust_name in sorted(r.near.items()):
            A(f"- `{go_name}` ~ `{rust_name}`")
        A("")
    return "\n".join(L) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="exit 1 if the doc is stale")
    ap.add_argument("--json", action="store_true", help="dump raw records")
    ap.add_argument("--cache", type=Path, default=None,
                    help="reuse/write the go_test_declaration_inventory TSV here")
    args = ap.parse_args()
    reports = run(args.cache)
    if args.json:
        json.dump([{
            "go_pkg": r.m.go_pkg, "rust_paths": r.m.rust_paths, "source": r.m.source,
            "risk": r.m.risk,
            "go_tests": [vars(g) for g in r.go_tests],
            "rust_tests": r.rust_tests,
            "matches": r.matches,
        } for r in reports], sys.stdout, indent=2)
        return 0
    text = render(reports)
    if args.check:
        old = DOC.read_text() if DOC.exists() else ""
        # the HEAD line changes every commit; compare everything else
        strip = lambda s: "\n".join(l for l in s.split("\n") if not l.startswith("Generated by"))
        if strip(old) != strip(text):
            print("test-coverage-inventory.md is stale; regenerate it", file=sys.stderr)
            return 1
        return 0
    DOC.parent.mkdir(parents=True, exist_ok=True)
    DOC.write_text(text)
    print(f"wrote {DOC.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
