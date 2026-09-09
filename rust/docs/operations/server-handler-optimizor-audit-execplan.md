# `pkg/server/handler/optimizor` audit ExecPlan

This living plan records the complete Go-package inventory and Rust ownership
decision for optimizer-trace, plan-replayer, and statistics HTTP handlers.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all seven package
      artifacts (2,219 lines) in full.
- [x] (2026-09-02) Confirmed byte identity with Go master and no
      generated/platform/fixture, benchmark, fuzz, or nested artifacts.
- [x] (2026-09-02) Traced HTTP routing, external storage, topology forwarding,
      Domain, statistics-handle, historical-stat, and priority-queue
      dependencies; Rust leaf owners remain explicit boundaries with no
      dependency-closed handler composition owner.
- [x] (2026-09-02) Ran the failpoint-wrapped full Go package suite under a
      short temporary root and the `tidb-domain` Rust owner suite.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and every
test. The HTTP endpoints remain Go-owned until the server/domain,
external-storage, and statistics composition migrates as one dependency-closed
unit. No speculative Rust facade or production edit belongs in this batch.

## Validation gate

    TMPDIR=/tmp PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      ./tools/check/failpoint-go-test.sh pkg/server/handler/optimizor -count=1
    cd rust && cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain
    make lint
    git diff --check

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/server_handler_optimizor.md`; no production source
changed.
