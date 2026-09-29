#!/usr/bin/env python3
"""Render the register and explicit crate coverage from verified audit data."""
import gzip
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
inv = json.loads(gzip.decompress((HERE / 'inventory.json.gz').read_bytes()))
findings = json.loads((HERE / 'findings.json').read_text())
coverage = json.loads((HERE / 'coverage.json').read_text())
intro = '''# Workspace structural mismatch audit — 2026-09-29

The scope now includes **all 88 Rust workspace packages and all 855 Go package
directories**. This pass records **23 source-confirmed structural mismatch
families**. These are shared ownership, representation, composition or lifecycle
problems; they are not 23 completed package ports or an assertion that no other
mismatch exists. No production behavior was changed.

## Reference and coverage

- Go master: `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
- Rust integration baseline: `2c120bc7face45b069dc197ab98a8f001724c140`.
- Git was fetched before the audit; integration was already up to date.
- Inventory: 7,726 Go tree blobs, including 5,826 package-owned artifacts and
  1,900 shared inputs/support/build artifacts. Every tracked Rust artifact is
  also accounted for, including 1,608 outside workspace crate ownership
  (vendored dependencies, shared scripts, documentation and build inputs).
- Source screening: 2,310 `rust/crates/*/src/` Rust files, including inline and
  source-based tests. The 1,229 textual candidate matches and 1,686 ignored-test
  annotations are **unverified leads**, not defect counts. Benchmark exclusions,
  Go-skipped tests and stale comments are present in those lists.
- Cargo graph: 70 workspace packages are possible non-dev dependencies of the
  server on `aarch64-apple-darwin`; 18 are outside that graph. This is neither
  symbol reachability nor proof that optional/platform variants work. Tools and
  test crates outside the server graph are normal.
- 390 Go packages are mentioned by Rust source comments/paths. These are mapping
  hints only; the remaining 465 must not be described as all missing, and a
  mention must not be described as a completed implementation.

[Complete crate coverage](coverage.md) distinguishes selected boundary review
from inventory-only screening. All 855 Go packages and all 88 Rust packages
remain `not_fully_verified`. External Go module versions are pinned by the
inventoried go.mod/go.sum; this pass does not claim exhaustive semantic review of
every package in those external modules. The local dependency graph is one target,
not all platform/build variants. Whole-package acceptance requires production,
generated/platform inputs, original tests/support/fixtures and required gates.

## Findings and repair order

Start with execution owners and context propagation: S01/S02 (DDL), S12 (storage
authority), S04–S06 (background owners), S07 (MPP). Then close wire/executor
contracts S13–S16/S21, typed expression contracts S17–S20, and the remaining SQL
entry points. S23 and S22 are concrete performance/scheduling discrepancies, but
no benchmark improvement is claimed. Repair units remain complete upstream Go
packages even when several Rust crates must change together.

'''
lines=[intro,'| ID | Priority | Structural boundary |\n| --- | --- | --- |']
for f in findings:
    lines.append(f"| [{f['id']}](#{f['id'].lower()}) | {f['priority']} | {f['title']} |")
lines.append('\n## Evidence-backed register\n')
for f in findings:
    lines += [f"<a id=\"{f['id'].lower()}\"></a>\n\n### {f['id']}: {f['title']}\n",f["impact"]+'\n',
              '**Repair boundary:** '+f['repair']+'\n',
              '**Required validation:** '+f['required_validation']+'\n',
              '**Owning Go package units:** '+', '.join('`'+p+'`' for p in f['go_packages'])+'.\n']
    links=[]
    for e in f['evidence']:
        ref=inv['go_ref'] if e['side']=='go' else inv['rust_ref']
        links.append(f"[{e['side']}: {e['path']}:{e['line']}](https://github.com/pingcap/tidb/blob/{ref}/{e['path']}#L{e['line']})")
    lines.append('**Pinned source evidence:** '+ '; '.join(links)+'.\n')
    if f.get('prior_evidence'):
        lines.append('**Earlier differential evidence:** [preserved audit]('+f['prior_evidence']+'). Earlier observations retain their original reference/coverage limits; they were not all replayed here.\n')
    if f.get('runtime_observation'):
        lines.append('**This audit’s runtime observation:** '+f['runtime_observation']+'\n')
lines.append('''## Revalidated leads and limits

- The old separate eager `tidb-exec` SQL engine is retired. The live query route
  uses `tidb-session` → `tidb-executor` → shared `tidb-planner` physical plans.
  Do not classify configured read-only proof binaries as the deployed SQL engine.
- Merge/index-merge helper modules alone are not proof of an independent planning
  pipeline. Current candidates enter `find_best_task/dispatch.rs`; the earlier
  broad split-pipeline allegation is not reopened without a new counterexample.
  Candidate enumeration, pruning, ties, hints and cache properties still need a
  complete Go oracle audit. This does not certify planner parity.
- Sort has spill/merge workers, and hash-join V2 has a live physical-builder call.
  Claims that these implementations are entirely absent are stale.
- Index-lookup costing now uses the concurrency divisor despite an old header
  saying it is unported. Auto-analyze, statistics usage, historical statistics,
  system-session pooling and DDL notifier have live factory wiring.
- Materialized-view step/build helpers exist. That does not prove live worker
  integration: the DDL owner/submission split in S01 must be closed. The old
  comment that every view job simply stays queued is not used as proof.
- A RangerContext default found in a test fixture is not a production context
  leak. Likewise, the literal `unimplemented!()` hits found in the DDL schedule
  expression source are test mocks, not production stubs.
- The old charset-specific case, LIKE matcher and negative-fraction timestamp
  findings were repaired in `cec2e3f475`; this register does not reopen them.
- Privilege/authentication persistence and all plugin/TLS variants, statistics
  estimator/merge/cache concurrency, schema history/MDL, parser/AST completeness,
  BR and DXF package lifecycles, encodings/collators, and utility concurrency
  require deeper package review. An inventory-only row is not a clean bill of
  health. Absent source comments are not proof of absent implementations.

## Reproduction and validation

Run from the repository root:

```sh
cargo metadata --offline --locked --filter-platform aarch64-apple-darwin --format-version 1 --manifest-path rust/Cargo.toml > /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py generate --metadata /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py verify
python3 rust/docs/audits/structural-20260929/report.py
python3 rust/docs/audits/structural-20260929/run-probe.py
```

The metadata command must use the pinned Rust source/dependency inputs. The
inventory reads pinned Git blobs and records complete artifacts, not only files
that contain a mapping comment. Nested workspace crates have distinct ownership;
shared inputs remain explicit. The verifier rejects stale evidence, duplicate or
omitted artifact ownership, and incomplete crate coverage.

`run-probe.py` temporarily appends `rust-probe.txt` to the existing server test
fixture, runs one collector test, and restores the file in `finally`. Run it with
no concurrent source writers. It prints observations; a passing collector is
not a parity pass. `go-probe.txt` is the corresponding Go test overlay, run in the
pinned reference archive with the failpoint wrapper. Exact commands, outcomes and
limitations are in [validation.md](validation.md). Raw observations are retained
in the losslessly compressed probe logs. Source excerpts and hashes are in
[findings.json](findings.json).

Full runtime parity, every build/platform variant, distributed failure behavior,
complete package tests and sysbench/TPCC/TPCH/YCSB performance were not verified.
This is an audit and repair register; the open implementation risks remain.
''')
(HERE/'README.md').write_text('\n'.join(lines))
lines=['# Crate coverage ledger\n','Every row has a complete tracked-artifact inventory. “Selected boundaries” means only the concrete register boundaries were inspected; “Inventory only” means no semantic clearance. All rows remain not fully verified.\n','| Crate | Subsystem | Review depth | Findings | Server dependency | Artifacts |\n| --- | --- | --- | --- | --- | --- |']
for name,row in sorted(coverage.items()):
    depth='Selected boundaries' if row['findings'] else 'Inventory only'
    links=', '.join(f'[{i}](README.md#{i.lower()})' for i in row['findings']) or '—'
    lines.append(f"| {name} | {row['subsystem']} | {depth} | {links} | {'possible' if row['server_nondev_dependency'] else 'no'} | {row['artifact_count']} |")
lines.append('\nThe complete Go-package ledger, including artifacts and mapping hints, is `inventory.json.gz`. The 855 package units are all marked `not_fully_verified`; review remains open for their complete production variants, tests, fixtures, integration decisions and validation gates.\n')
(HERE/'coverage.md').write_text('\n'.join(lines))
print('Rendered',len(findings),'findings and',len(coverage),'crate coverage rows.')
