# `pkg/server/handler/extractorhandler` audit ExecPlan

This living plan records the complete Go-package inventory and Rust ownership
decision for the plan-extraction HTTP handler.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all four package
      artifacts (439 lines) in full.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go master and has
      no generated/platform/fixture or nested artifacts.
- [x] (2026-09-02) Traced domain extraction, extstore, HTTP, statement-summary,
      failpoint, and server-bootstrap dependencies; the Rust plan-extraction
      owner remains an explicit gap, so no speculative facade was added.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including BUILD metadata and all
tests. The endpoint remains an explicit boundary until the server/domain and
persisted extraction lifecycle migrates as one dependency-closed unit.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      ./tools/check/failpoint-go-test.sh \
      pkg/server/handler/extractorhandler \
      -run 'TestExtractHandler$|TestExtractHandlerInfoSchemaV2$' -count=1
    make lint
    git diff --check

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/server_handler_extractorhandler.md`; no production
source changed.
