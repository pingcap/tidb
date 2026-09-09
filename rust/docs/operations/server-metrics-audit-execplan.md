# `pkg/server/metrics` audit ExecPlan

This living plan records the complete Go-package inventory and the Rust
ownership decision for server metric wiring.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both package
      artifacts (135 lines) in full.
- [x] (2026-09-02) Confirmed both artifacts are byte-identical to Go master and
      found no tests, fixtures, generated/platform variants, or nested package.
- [x] (2026-09-02) Traced Rust metrics crates and server consumers; no
      dependency-closed Rust owner exists for Go's global server vectors, so no
      speculative facade or Rust-only behavior removal is justified.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
support artifacts. `pkg/server/metrics` is a leaf wiring package whose runtime
consumer is the Go server connection loop. It remains an explicit boundary
until that owner migrates as one dependency-closed unit.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/metrics -count=1
    make lint
    git diff --check

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/server_metrics.md`; no production source changed.
