# `pkg/server/internal/dump` audit ExecPlan

This living plan records the complete Go-package inventory and the Rust
protocol ownership decision for binary wire-dump helpers.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all three package
      artifacts (303 lines) in full.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced all dump consumers to `tidb-protocol`'s prepared
      statement and result-row owners; source-derived temporal and width tests
      cover the complete Go helper contract without a duplicate facade.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
tests. The Rust protocol owner already closes this package's dependency graph;
no production delta or Rust-only behavior removal is justified.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/internal/dump -count=1
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol \
      --test all prepared_statement_protocol_source -- --test-threads=1
    make lint
    git diff --check

## Outcome

The complete inventory and protocol ownership boundary are recorded in
`rust/testport/receipts/server_internal_dump.md`; no production source changed.
