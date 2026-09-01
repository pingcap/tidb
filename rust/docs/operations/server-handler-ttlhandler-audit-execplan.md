# `pkg/server/handler/ttlhandler` audit ExecPlan

This living plan records the complete Go-package inventory and Rust ownership
decision for the HTTP TTL trigger endpoint.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both package
      artifacts (92 lines) in full.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced the endpoint's router, domain, TTL command, and
      logging dependencies; no dependency-closed Rust HTTP owner exists, so no
      speculative facade was added.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
support artifacts. This endpoint remains an explicit boundary until the
server/domain HTTP lifecycle migrates as one dependency-closed unit.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/handler/ttlhandler -count=1
    make lint
    git diff --check

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/server_handler_ttlhandler.md`; no production source
changed.
