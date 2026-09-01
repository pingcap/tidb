# `pkg/infoschema/perfschema` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package audit
and restoration of profile-request logging.

## Purpose / Big Picture

`pkg/infoschema/perfschema` registers virtual PERFORMANCE_SCHEMA tables,
serves session/status views, and fetches local/remote pprof profiles.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all eight artifacts
      (1,572 lines plus two binary fixtures) before editing. Decoded the gzip
      pprof fixture with `go tool pprof` and inspected the binary TiKV profile.
- [x] (2026-09-02) Identified the branch delta removing
      `logTiDBProfileRequest` and its logutil/zap dependencies.
- [x] (2026-09-02) Restored the exact Go-master logging helper and BUILD
      dependencies; no Rust performance-schema owner exists.
- [x] (2026-09-02) Ran the failpoint-enabled package suite, attempted required
      Bazel preparation, and ran the Ready lint/diff gates. Bazel is unavailable
      locally.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decisions

The atomic unit is the complete package, including profile fixtures and test
harness. Logging is part of the Go observable profile-request behavior and is
restored without inventing Rust virtual tables or pprof transports.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      ./tools/check/failpoint-go-test.sh pkg/infoschema/perfschema -count=1
    make bazel_prepare
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

The failpoint wrapper is mandatory for this package. Bazel preparation is
required for the restored BUILD imports and is blocked because Bazel is not
installed.

## Decision log

- 2026-09-02: Treat profile-request logging as missing Go behavior and restore
  the source helper rather than adding a Rust-only observability path.

## Outcomes and retrospective

The complete package and fixtures are inventoried, profile-request logging is
restored, and the failpoint-enabled suite passes after the package fix.
