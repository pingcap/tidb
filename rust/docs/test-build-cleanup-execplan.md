# Remove runner build overrides and obsolete workflow prose

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Live runner scripts currently override caller CARGO_BUILD_JOBS with twelve
jobs, including explicit -j12 arguments. This defeats Cloud's one-job heavy
link policy. One transaction test runner also forces a separate release cache
without consuming a release binary path. Remove these overrides as one batch,
preserve intentional server benchmark profiles, and keep dependency resolution
locked. Work in /workspace/tidb on hparser-integration from
604048e99c73fc0b4db90216c0ccfd97d9e4b85b. Refreshed Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. No Rust/Go application behavior changes.

## Progress


- [x] Inventory all runner Cargo boundaries and reproduce ignored caller job limits.
- [x] Remove forced jobs and one unnecessary release-test profile; lock runner commands.
- [x] Replace historical retirement prose in scripts/README.md with current operations.
- [x] Verify command boundaries before/after, shell syntax, retained cleanup guards and lint.
- [ ] Pass actual hook and fresh pre-push locked build, verify remote and Cloud checkpoint.

## Milestones and Plan of Work


Remove CARGO_BUILD_JOBS=12 and -j12 from run-*.sh Cargo calls. Let Cargo use
the caller's environment or its standard default. Add --locked to build/test
calls that lack it. Only run-realtikv-pessimistic-prewrite-recovery.sh loses
--release: it invokes tests directly and has no release binary consumer.
Keep --release and binary paths in scan-pushdown, sysbench-ladder and
lost-update-check. Preserve all package/target/filter, ignored-test, offline,
phase marker, cleanup, endpoint and protocol assertions.

Shorten scripts/README.md by removing dated retirement narratives already
indexed in current-audit/README.md. Keep invocation directories, Cloud activation,
job/profile policy, aggregate registrations, isolated global-state suites,
shared SQL checks, generator commands, native sync and cache-safety instructions.

## Validation and Acceptance


Run external check.py before and after from
/workspace/.cloud-setup/runner-build-cleanup. It executes each actual Cargo
command boundary through a recorder function with caller job limits 1 and 3.
Before removal it must observe forced 12-job execution. After removal every
boundary must retain the requested limit, omit explicit job flags and use
--locked. This validates invocation construction, not a live TiKV run.

Run bash -n on changed scripts, then from repository root:

    bash rust/scripts/test-prepared-write-paths.sh
    bash rust/scripts/test-optimistic-2pc-paths.sh
    make lint
    git diff --check

Compare all script text after undoing only approved command substitutions to
its before-image; all SQL/lifecycle/cleanup logic must remain byte-identical.
No new permanent source-shape test or harness is added. All validation tooling
for this bounded maintenance stays outside the repository. Required Rust-path
publication gates remain: actual hook cd rust && cargo build --locked -p
tidb-server and a fresh identical build immediately before normal push.

## Surprises & Discoveries


The documented Cloud job limit was overridden by both a shell assignment and
a Cargo CLI flag, so removing only one would leave the problem. The prewrite
recovery test selected release although it only consumes Cargo's test result.
Other release runners name release server binaries and retain that profile.

## Decision Log


Keep live assertions and safety guards. Remove obsolete overrides and duplicated
historical instructions, not source-backed tests. Respect caller job limits;
do not introduce another wrapper or runtime dependency. Lock dependency reads
without changing manifests. No broad finding repair or package acceptance.

## Outcomes & Retrospective


Implementation and validation complete: 52 command-boundary checks pass
(previously all failed at least one policy expectation), all 26 scripts pass
shell syntax, both existing cleanup guards pass, and lint/diff/continuity pass.
Removed 56 net README lines. Publication gates pending; external final
handoff records their later results. No live SQL or TiKV validation claimed.

## Recovery, Artifacts and Dependencies


Before-images: git show 604048e99c:<path>. Restore individual files only;
preserve concurrent work. Logs: /workspace/.cloud-setup/runner-build-cleanup.
Durable receipt: rust/docs/parity/current-audit/runner-build-cleanup-validation.json.
Native client remains unchanged. Cloud save, Publish and fresh-task restoration
remain separate. Revision: replace completed compiler/result removal with
runner build-policy cleanup and concise operating instructions.
