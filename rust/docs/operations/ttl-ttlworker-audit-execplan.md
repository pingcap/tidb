# `pkg/ttl/ttlworker` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit
and restoration of behavior that had been removed on the Rust integration
branch.

## Purpose / Big Picture

`pkg/ttl/ttlworker` owns TTL job scheduling, task ownership, scanning,
deletion, session setup, timer synchronization, and the integration-test
harness. Go remains authoritative because the Rust workspace has no
dependency-closed owner for this orchestration package.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all 25 tracked
      production, test, integration-support, and BUILD artifacts (12,187
      lines) before editing. Confirmed no doc, fixture, generated/platform,
      benchmark, fuzz, or ownership artifact.
- [x] (2026-09-02) Compared the package with Go master and identified the
      branch delta removing external-workload recycling, TTL owner election,
      constructor options, BUILD dependencies, and focused tests.
- [x] (2026-09-02) Restored the Go-master production behavior and focused
      tests, adapting only the test fake to the branch's current extworkload
      interface.
- [x] (2026-09-02) Ran the focused failpoint-enabled regression and the Ready
      lint/diff gates; attempted `make bazel_prepare` as required, but Bazel is
      unavailable in this environment.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete `pkg/ttl/ttlworker` package plus its nested
`integrationtest` support package. Restoring the removed Go behavior was
necessary to retain Go-master semantics. A Rust implementation is deferred
until owner election, external workload control, domain/session/timer storage,
and TiKV/testkit dependencies have complete owners.

## Validation gate

Run from the repository root:

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      ./tools/check/failpoint-go-test.sh pkg/ttl/ttlworker \
      -run 'TestCheckFinishedJob(RecyclesExternalTTLTask|DoesNotRecycleExternalTTLTaskFromMaster)$' -count=1
    make bazel_prepare
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

The package's failpoint wrapper is mandatory. `make bazel_prepare` is required
for the restored BUILD dependencies and test functions; it is blocked locally
because `bazel` is not installed.

## Decision log

- 2026-09-02: Treat the removed owner/recycle code as missing Go behavior, not
  as a Rust feature to preserve. Restore the exact Go-master implementation and
  its focused tests.
- 2026-09-02: Keep Rust ownership explicitly deferred because no
  dependency-closed `ttlworker` crate exists.

## Outcomes and retrospective

The complete package is inventoried and the branch's missing Go behavior is
restored. The focused owner/recycle regressions pass; Bazel metadata generation
remains unverified due to the missing local tool.
