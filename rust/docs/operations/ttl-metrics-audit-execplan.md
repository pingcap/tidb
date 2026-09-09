# `pkg/ttl/metrics` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit.

## Purpose / Big Picture

`pkg/ttl/metrics` owns TTL Prometheus vectors, worker phase counters, delay
bucket updates, and the context-carried phase tracer. No Rust crate currently
owns this package; the TTL session crate intentionally declares a narrow
phase-tracer boundary.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all three Go
      artifacts (353 lines), including BUILD metadata and every test. Confirmed
      no doc, fixture, generated/platform, benchmark, fuzz, or ownership
      artifact.
- [x] (2026-09-02) Searched the Rust workspace for a metrics owner and
      confirmed none exists; traced the existing `PhaseTracer` boundary in
      `tidb-ttl::session` to its three source-used phases.
- [x] (2026-09-02) Recorded the dependency-closed boundary without adding a
      speculative Prometheus registry or Rust-only metric semantics.
- [x] (2026-09-02) Ran the tagged Go package tests, repository lint, and diff
      hygiene; the Ready profile is green for this no-code audit.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete Go package: `metrics.go`, `metrics_test.go`, and
`BUILD.bazel`. A faithful Rust owner needs the process-wide `pkg/metrics`
registry, Prometheus vectors, and context plumbing. Those dependencies are not
transcreated, so this package remains an explicit boundary rather than a
partial or invented implementation.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/ttl/metrics -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No Rust owner or Go source changed, so cargo checks and `make bazel_prepare`
are not required for this audit.

## Decision log

- 2026-09-02: Keep the metrics registry and context tracer as a named
  dependency boundary until `pkg/metrics` and Prometheus registration have a
  complete Rust owner.

## Outcomes and retrospective

The complete Go package is inventoried and its unported dependency boundary is
recorded; no parity implementation was safely possible in this batch.
