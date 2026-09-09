# `pkg/server/internal/util` audit ExecPlan

This living plan records the complete Go-package inventory and the split Rust
ownership boundary for protocol helpers versus server transport adapters.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all four package
      artifacts (467 lines) in full.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced protocol helper ownership to `tidb-protocol` and
      confirmed the remaining buffered-connection, CORS, and test-config APIs
      lack a dependency-closed Rust server owner; no speculative adapters were
      added.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
tests. The protocol helper subset is already ported and source-tested; the
transport/config subset remains an explicit boundary until the owning server
listener and session migration is complete.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/internal/util -count=1
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol \
      --test all server_internal_util -- --test-threads=1
    make lint
    git diff --check

## Outcome

The complete inventory and split ownership boundary are recorded in
`rust/testport/receipts/server_internal_util.md`; no production source changed.
