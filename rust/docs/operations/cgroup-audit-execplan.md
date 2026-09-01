# Complete `pkg/util/cgroup` parity

This ExecPlan is a living document and follows `PLANS.md`. Go commit
`e2788410d8d696605e8cb002585877a063ccc909` remains the behavioral authority.

## Purpose / Big Picture

Close the unclaimed cgroup report entry as one atomic package. The Rust owner
must cover all six production Go variants/contracts, both test artifacts, and
the Bazel-selected platform surface. It must preserve cgroup v1/v2 discovery,
namespace and hybrid fallback behavior, CPU quota rounding, memory stat/limit
parsing, and unsupported-target sentinels. A caller can exercise the result
through `tidb_util::cgroup` without a seed-only API or Rust-only cgroup policy.

The Go `SetGOMAXPROCS` mutation is recorded as an explicit boundary: Rust has
no process-global scheduler setting equivalent to Go's runtime knob, so only
the signed quota conversion is exposed. Host-memory and process-RSS helpers
that had been placed in the seed are owned by `tidb_util::memory::process`,
where the existing memory consumers now call them.

## Progress

- [x] (2026-09-01) Re-audited the branch receipts and confirmed no cgroup
      claim existed.
- [x] (2026-09-01) Read all nine pinned Go artifacts, including both platform
      variants, the Bazel target, the live CPU test, and the complete fixture
      matrix; read the full Rust owner, manifest/export, and direct consumers.
- [x] (2026-09-01) Corrected controller-count and mount-separator parsing,
      relocated memory-only Rust helpers, and changed quota conversion to a
      signed Go-compatible result.
- [x] (2026-09-01) Added source-derived memory/CPU matrices, a mount-parser
      regression, duplicate-controller regression, live container-test
      carrier, and public unsupported-platform checks.
- [x] (2026-09-01) Ran the Ready profile, updated the receipt with exact
      results, and reviewed the package diff. The atomic commit/push is the
      remaining handoff action for this goal.

## Surprises & Discoveries

- The seed's `controller_matches` compared the deduplicated set length with
  the requested controller count. Go compares the raw comma-field count, so
  duplicate controller fields were a latent mismatch. The implementation now
  retains Go's raw-length guard while still using a set for membership.

- The seed searched every mountinfo field for `-`; Go starts at field 7
  because fields 1–6 are fixed. A mountpoint or optional field containing `-`
  could therefore select the wrong separator. The parser now starts at the
  same index and has a focused regression.

- The seed's `effective_memory_limit` and process-RSS functions came from
  later memory-authority work, not `pkg/util/cgroup`. They remain needed by
  Rust memory consumers, so they were moved to `memory::process` rather than
  deleted or left as cgroup behavior.

## Decision Log

- Decision: keep cgroup fixture tests in the owner module instead of making
  private parsers public solely for an integration test.
  Rationale: Go's `cgroup_mock_test.go` is package-internal and covers private
  helpers. The Rust module tests preserve that boundary; the external carrier
  checks only public target-selected fallbacks.
  Date/Author: 2026-09-01 / Codex

- Decision: use `i64` for the Rust quota-conversion result.
  Rationale: Go returns `-1` on unsupported platforms and on cgroup errors;
  `usize` could not represent that source sentinel. Positive worker counts
  remain identical.
  Date/Author: 2026-09-01 / Codex

- Decision: preserve Go's hybrid memory-usage mount choice even though the
  v2 mount is available.
  Rationale: the pinned source joins the v2 cgroup path to `mount[0]` for
  `getCgroupMemUsage`, unlike its inactive-file and limit helpers. The
  compatibility test locks this observable choice.
  Date/Author: 2026-09-01 / Codex

## Plan of Work

First inventory the pinned tree and existing Rust owner, then compare each
parser and platform branch against Go. Correct only the discovered semantic
differences and remove the seed's cgroup-foreign public helpers.

Next add source-derived deterministic fixtures for every `cgroup_mock_test.go`
matrix: missing files/controller/mounts, v1, namespace-relative paths, v2
numeric/max/malformed values, controller-order variants, and hybrid fallback.
Carry the Linux live test's ten-worker/container guard and public
unsupported-target assertions separately.

Finally update the receipt and this plan, run focused Rust tests and the
affected-crate checks, use the Ready profile (including `make lint`), review
the diff, make one batch commit, and push it to the requested
`hparser-integration` branch.

## Validation and Acceptance

Acceptance requires:

1. All nine pinned Go artifacts appear in the receipt and map to Rust code,
   tests, build selection, or an explicit boundary.
2. v1/v2 CPU and memory behavior, hybrid fallback order, namespace-relative
   mount construction, `max -> math.MaxInt64`, signed quota sentinels, and
   container detection match the pinned source fixtures.
3. The cgroup owner contains no host-memory/RSS or scheduler recommendation
   behavior that belongs to another Rust owner.
4. Focused owner tests, public fallback tests, formatting, affected-crate
   compilation/tests, `git diff --check`, and Ready `make lint` pass.
5. The final commit contains only this package claim and its necessary memory
   integration relocation, with Linux live/container and cross-target limits
   reported honestly.

## Idempotence and Recovery

All fixture tests use temporary directories and do not modify system cgroup
files. Formatting and checks are repeatable. If a cross-target build exposes a
platform-only issue, retain the source inventory, fix the target-selected
branch, and rerun the affected checks; do not claim Ready from a host-only
success until the limitation is recorded.

## Artifacts and Notes

The receipt is `rust/testport/receipts/util_cgroup.md`. The Rust owner is
`rust/crates/tidb-util/src/cgroup.rs`, exported by `src/lib.rs`; memory-only
helpers moved to `src/memory/process.rs` and are re-exported by
`src/memory/mod.rs`. The public fallback carrier is
`rust/crates/tidb-util/tests/cgroup_source.rs`.

## Outcomes & Retrospective

The Rust cgroup owner now has a complete pinned nine-artifact inventory,
source-derived fixture coverage, corrected controller/separator parsing, and
Go-compatible signed quota fallbacks. Memory-only host/RSS helpers no longer
inflate the cgroup package claim; they remain in the ordinary memory owner and
all existing consumers compile against that boundary. Ready validation passes
on the macOS host. Linux live cgroup execution, Windows, and unsupported-target
runtime execution remain explicitly unverified and are covered by the source
fixture/platform mapping rather than implied as host evidence.
