# `pkg/server/internal/testutil` audit ExecPlan

This living plan records the complete Go-package inventory and Rust test-only
ownership decision for the server byte-connection helper.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both package artifacts
      (79 lines) in full.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced the helper to the Rust protocol test owner, which
      preserves the complete no-op connection and port contract without adding
      a production mock abstraction.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
support artifacts. This is test-only infrastructure; the Rust counterpart stays
in source tests and no production code change is justified.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/internal/testutil -count=1
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol \
      --test all server_internal_testutil -- --test-threads=1
    make lint
    git diff --check

## Outcome

The complete inventory and test-support boundary are recorded in
`rust/testport/receipts/server_internal_testutil.md`; no production source
changed.
