# `pkg/server/internal/testserverclient` audit ExecPlan

This living plan records the complete Go-package inventory and the Rust
ownership decision for the server integration test harness.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both package
      artifacts (3,159 lines) in full, including every helper and scenario.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced its database/sql, testkit, failpoint, TLS, DDL, and
      metrics dependencies; no dependency-closed Rust integration harness
      exists, so no speculative replacement was added.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
support artifacts. This package remains an explicit integration-harness
boundary until the Go server/testkit lifecycle migrates as one dependency-closed
unit.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/internal/testserverclient -count=1
    make lint
    git diff --check

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/server_internal_testserverclient.md`; no production
source changed.
