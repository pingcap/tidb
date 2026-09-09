# `pkg/table/tables/testutil` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit
and parity restoration.

## Purpose / Big Picture

`pkg/table/tables/testutil` provides the testkit helper that scans an index
prefix and verifies the number of index key/value pairs.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both original
      artifacts (94 lines), including BUILD metadata and the complete helper.
      Confirmed no doc, fixture, generated/platform, benchmark, fuzz, or
      ownership artifact.
- [x] (2026-09-02) Identified the stale branch delta that selected the process
      default collation instead of the table's persisted `UseNewCollate()` mode.
- [x] (2026-09-02) Restored Go-master behavior, removed the obsolete collation
      dependency, and added the focused table-mode regression (post-fix: three
      artifacts, 154 lines).
- [x] (2026-09-02) Demonstrated fail-before/pass-after, ran the complete
      package tests, attempted required Bazel preparation, and ran the Ready
      lint/diff gates. Bazel is unavailable locally.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete helper package. `newIndexEncoder` is a narrow
private seam so the test proves table-mode selection without duplicating the
domain scan harness. Rust key encoding remains the dependency owner; no
Rust-only helper was introduced.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/table/tables/testutil -count=1
    make bazel_prepare
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

The new top-level test and BUILD target require `make bazel_prepare`; that
command is blocked here because `bazel` is not installed.

## Decision log

- 2026-09-02: Use `table.Table.UseNewCollate()` for helper key construction.
  The process-wide default is not authoritative for persisted table snapshots.

## Outcomes and retrospective

The complete package is inventoried, the stale collation selection is fixed,
and the focused regression passes after failing against the old implementation.
