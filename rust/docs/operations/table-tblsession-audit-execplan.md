# `pkg/table/tblsession` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit.

## Purpose / Big Picture

`pkg/table/tblsession` adapts a live TiDB session into the mutation,
allocation, statistics, cache, temporary-table, and exchange-partition
interfaces consumed by table DML. No dependency-closed Rust owner currently
exists.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all three tracked
      artifacts (386 lines), including BUILD metadata and the complete source
      test. Confirmed no doc, fixture, generated/platform, benchmark, fuzz, or
      ownership artifact.
- [x] (2026-09-02) Traced the Rust executor `b151` source inventory and
      confirmed the old standalone `tblctx`/`tblsession` seed was removed as an
      unwired partial carrier; no production Rust owner exists.
- [x] (2026-09-02) Recorded the dependency-closed Go boundary without adding
      a speculative Rust session context.
- [x] (2026-09-02) Ran the tagged package-equivalent Go test, repository lint,
      and diff hygiene under the Ready profile.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete three-artifact Go package. Its behavior is
coupled to session variables, transaction state, auto-ID allocation,
temporary-table state, infoschema, row encoding, and table DML. Keep the Go
implementation authoritative until those dependencies have integrated Rust
owners; do not duplicate the deleted seed context.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/table/tblsession -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No source or BUILD artifact changed, so `make bazel_prepare` and cargo checks
are not required for this boundary-only audit.

## Decision log

- 2026-09-02: Keep `MutateContext` as a Go-only boundary. The prior Rust seed
  was deleted because it was self-contained and not wired to live DML.

## Outcomes and retrospective

The complete package is inventoried and its dependency boundary is recorded;
the source test passes and no speculative parity implementation was added.
