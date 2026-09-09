# `pkg/ingestor/errdef` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit
and restoration of a missing sentinel.

## Purpose / Big Picture

`pkg/ingestor/errdef` defines normalized TiKV ingest/global-sort errors and
the HTTP status error used by ingest clients.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both original
      artifacts (76 lines), including BUILD metadata and all production
      declarations. Confirmed no doc, fixture, generated/platform, benchmark,
      fuzz, or ownership artifact.
- [x] (2026-09-02) Identified the deleted `ErrTooManyDataFiles` definition,
      which is required by the current global-sort planner.
- [x] (2026-09-02) Restored the exact Go-master sentinel, added a focused
      message/RFC-code test, and updated the BUILD target (post-fix: three
      artifacts, 110 lines).
- [x] (2026-09-02) Demonstrated fail-before/pass-after, ran the complete
      package tests, attempted required Bazel preparation, and ran the Ready
      lint/diff gates. Bazel is unavailable locally.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete error-definition package. The global-sort
planner and ingest RPC clients remain unported Rust dependencies; no
disconnected Rust catalog was added.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/ingestor/errdef -count=1
    make bazel_prepare
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

`make bazel_prepare` is required for the new top-level test and BUILD target;
it is blocked here because Bazel is not installed.

## Decision log

- 2026-09-02: Restore `ErrTooManyDataFiles` as a Go sentinel rather than
  inventing a Rust-only error; the current Go global-sort consumer is the
  source of truth.

## Outcomes and retrospective

The complete package is inventoried, the deleted sentinel is restored with its
source contract, and the focused regression passes after failing against the
pre-fix package.
