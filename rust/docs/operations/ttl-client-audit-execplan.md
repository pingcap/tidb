# `pkg/ttl/client` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit.

## Purpose / Big Picture

`pkg/ttl/client` owns the TTL command request/response protocol and
notification fan-out over etcd v3, plus deterministic in-memory mocks. No Rust
crate currently owns this package.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all four Go
      artifacts (735 lines), including BUILD metadata and every test. Confirmed
      no doc, fixture, generated/platform, benchmark, fuzz, or ownership
      artifact.
- [x] (2026-09-02) Searched the Rust workspace for a TTL command or
      notification owner and confirmed none exists; recorded the etcd/DDL
      utility dependency boundary.
- [x] (2026-09-02) Ran the tagged Go client integration test, repository lint,
      and diff hygiene; the Ready profile is green for this no-code audit.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete Go package: `command.go`, `notification.go`,
`command_test.go`, and `BUILD.bazel`. A faithful Rust owner needs etcd v3 lease
and watch semantics, JSON wire compatibility, and `pkg/ddl/util`'s put helper;
those dependencies are not transcreated. No partial client or Rust-only mock
was added.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/ttl/client -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No Rust owner or Go source changed, so cargo checks and `make bazel_prepare`
are not required for this audit.

## Decision log

- 2026-09-02: Keep etcd command/notification behavior as an explicit boundary
  until the shared etcd and DDL utility dependencies have complete Rust owners.

## Outcomes and retrospective

The complete Go package is inventoried and its dependency boundary is
recorded; no parity implementation was safely possible in this batch.
