# `pkg/dumpformat/csvfile` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, and build artifact in
`pkg/dumpformat/csvfile`, compare the CSV writer contract with Rust owners,
and preserve a package-atomic ownership boundary.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all five artifacts (403 lines): BUILD metadata,
  framing config, writer and escaping implementation, and all eleven focused
  tests. Confirm no fixtures, generated/platform variants, fuzz/benchmark
  inputs, or generator sources.
- [x] (2026-09-01) Compare CSV writer consumers and the Rust parser/import
  surface; no dependency-closed Rust CSV output owner exists.
- [x] (2026-09-01) Run the exact detached Go-master package suite and diff
  hygiene check.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded dump-format package.

## Scope and decision

This package is a complete CSV output library with observable escaping,
quoting, NULL, binary encoding, row-width, and byte-count contracts. Rust's
parser support only accepts CSV-related SQL syntax; it does not provide a
dependency-closed export writer or the Go `sql.RawBytes`/`io.Writer` boundary.
Do not add an uncalled Rust-only writer. A future parity change must move the
writer and its ten regressions with an actual Rust consumer.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/csvfile -count=1
git diff --check
```

The detached Go suite passes. No Rust or Bazel source was changed, so the
broader workspace Ready gates are not applicable to this documentation-only
boundary.

## Outcome

The complete inventory and explicit owner decision are recorded in
`rust/testport/receipts/dumpformat_csvfile.md`; the rolling audit continues
with the remaining dump-format writer/parser packages.
