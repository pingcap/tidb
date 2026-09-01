# `pkg/table/tblctx` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit.

## Purpose / Big Picture

`pkg/table/tblctx` owns reusable mutation buffers and the context interfaces
that connect table DML to session state, row encoding, transaction buffers,
statistics, temporary tables, and exchange-partition checks.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all four tracked
      artifacts (654 lines), including BUILD metadata and every test. Confirmed
      no doc, fixture, generated/platform, benchmark, fuzz, or ownership
      artifact.
- [x] (2026-09-02) Compared the current branch's explicit encoder parameter
      delta with Go master and verified the existing source assertions preserve
      value/binlog bytes; no independent missing behavior was found.
- [x] (2026-09-02) Traced the deleted Rust tblctx seed and the executor `b151`
      carrier inventory; recorded the dependency-closed Go boundary without
      adding speculative Rust buffers or context traits.
- [x] (2026-09-02) Ran the tagged package tests, repository lint, and diff
      hygiene under the Ready profile.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete four-artifact package. The explicit encoder
parameter is retained because current tablecodec callers use it and its value
semantics match Go; no source change is justified until a Go behavior
regression is demonstrated.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/table/tblctx -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No source or BUILD artifact changed in this audit batch, so Bazel preparation
and Rust cargo checks are not required.

## Decision log

- 2026-09-02: Keep the branch's explicit codec encoder threading. It is an
  integration API with source-equivalent value encoding, not a Rust-only
  semantic; the former standalone Rust tblctx seed remains deleted.

## Outcomes and retrospective

The complete package is inventoried and its encoder/context boundary is
recorded. Go tests pass and no speculative parity implementation was added.
