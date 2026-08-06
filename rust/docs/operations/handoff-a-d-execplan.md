# Complete the delegated Go-to-Rust handoff bundle

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The source request is `/Users/chenhuansheng/Downloads/handoff-tidb-rust.md`.

## Purpose / Big Picture

After this work, encrypted spill files no longer fall back to plaintext, the differential-test coverage claims are based on current source rather than old batch counts, previously unreadable integration topics can be compared when their harness constraints are removable, and stacked cherry-picks have a tested resolver for the recurring ratchet-constant conflict shape. A coordinator can cherry-pick the local commits in parent order, run the clean-worktree workspace gate, and push them without reconstructing hidden measurements.

## Progress

- [x] (2026-08-06) Read the handoff, repository policy, verification skill, test guidance, and current branch state.
- [x] (2026-08-06) Resume the dedicated `codex/handoff-tidb-rust` worktree at base `d93e689e89b67fc940f0cfacee9e96ac513c9a58`.
- [x] (2026-08-06) Port, validate, commit, and mutation-probe Task A's checksum-over-AES-CTR spill stack (`82e0e9add2ed9f0a810f87c4a3220396628f7de3`).
- [x] (2026-08-06) Finish Task D validation and four mutation probes for the six-constant Ruby resolver and formatter rollback.
- [x] (2026-08-06) Diagnose and align all fourteen Task C topics, enroll the one eligible topic, and complete five mutation probes.
- [x] (2026-08-06) Re-derive, document, commit, and complete four mutation probes for Task B's three measurements on the post-Task-C tip.
- [x] (2026-08-06) Run the handoff gates and repository Ready profile, self-review the 27-path diff, and prepare the parent-ordered SHA handoff.

## Surprises & Discoveries

- Observation: the resumed worktree deliberately starts from `d93e689e89b67fc940f0cfacee9e96ac513c9a58`, not the handoff's stale `0619041e0b` text.
  Evidence: `git log -1 --format=%H` before Task A printed `d93e689e89b67fc940f0cfacee9e96ac513c9a58`.
- Observation: Task A's AES-CTR primitive already exists in `tidb-util`; the missing behavior is the spill writer/reader composition and configuration choice in `tidb-chunk`.
  Evidence: `rust/crates/tidb-util/src/encrypt/aes_layer.rs` defines `CtrCipher`, `Writer`, and `Reader`, while `rust/crates/tidb-chunk/src/chunk_util.rs` stores only `checksum::Writer<File>`.
- Observation: `cargo fmt --manifest-path rust/Cargo.toml` has no default target because the root manifest is virtual; formatting must use the nearest package manifest.
  Evidence: the first synthetic CLI probe printed `Failed to find targets`, while `cargo fmt --manifest-path rust/difftests/Cargo.toml --check` succeeded.
- Observation: the current upstream tip does not pass `cargo fmt --all --check` before this task's Rust changes.
  Evidence: the command reports pre-existing diffs in `tidb-executor/src/driver/multi_dml.rs`, `tidb-executor/src/window/operand.rs`, `tidb-expr/src/arg_eval_type.rs`, `tidb-expr/src/string_fn.rs`, `tidb-expr/src/tests/etint_argument.rs`, and `tidb-session/src/tests_multi_table_dml.rs`; Task D touches none of them.
- Observation: the first checked-in conflict resolver was too narrow for the actual stacked conflicts and could leave a resolved file behind when formatting failed.
  Evidence: it accepted only `KNOWN_DIVERGENCES`, while the current ratchets also use `COMPARED`, `BOTH_AGREE`, `RECORDED_MERGE_PAIRS`, `AGREED_MERGE_PAIRS`, and `EXTRA_MERGE_PAIRS`; the replacement's `test_restores_the_conflict_when_formatting_fails` detects removal of the rollback write.
- Observation: the old re-census report's `40/16/892` and `732` writable-unread counts are stale on this branch.
  Evidence: `env LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 ruby -EUTF-8:UTF-8 rust/difftests/tools/sysvar-census.rb` prints `census: declared=948 runtime_behavior=42 set_or_validation_only=16 behaviorally_unread=890 sum=948` and `writability: writable_declared=785 writable_behaviorally_unread=730 read_only_or_scope_none_unread=160`; the two additional runtime readers are `txn.rs`'s `autocommit` and `stmt_ctx.rs`'s `version`.

## Decision Log

- Decision: resume the already-created dedicated worktree and preserve its `d93e689e8` base rather than create or rebase another branch.
  Rationale: the handoff continuation already contained a validated but uncommitted Task A implementation on that base; changing bases would discard or mix that evidence.
  Date/Author: 2026-08-06 / Codex
- Decision: finish in the order A, D, C, B.
  Rationale: A was already in progress. D is isolated tooling. C changes the replay corpus and denominator, so B must measure the post-C tip instead of publishing an immediately stale warning count.
  Date/Author: 2026-08-06 / Codex
- Decision: commit each task before its mutation probe, restoring mutations from explicit saved copies rather than Git checkout.
  Rationale: this is a non-negotiable handoff rule and gives each probe a stable committed baseline.
  Date/Author: 2026-08-06 / Codex

## Outcomes & Retrospective

All four requested tasks are implemented, committed, restored after their mutation probes, and covered by the combined Ready validation. The only red command is the required `cargo fmt --all --check`, which reports seven pre-existing formatting drifts outside the A-D diff; the touched `difftest-result-tests` package passes its scoped fmt check.

Task D now uses the standard-library-only Ruby resolver at `rust/difftests/resolve-ratchet-conflict.rb`. Its four synthetic tests cover both-side narrative preservation, exact constant de-duplication without touching same-named constants outside the conflict, caller-owned values for all six integration and join-shape ratchets, formatter invocation, and byte-for-byte restoration of the original conflict when formatting fails.

Task C aligns all 257 integration topics at the recording-reader layer. The replay gate grows from `integrationtest replay over 109 topics: 8099 of 10972 statements compared` to `integrationtest replay over 110 topics: 8234 of 11465 statements compared`; `planner/core/integration_partition` is the only newly aligned topic below the five-divergence enrollment bar.

Task B replaces the stale sysvar and oracle claims with executable measurements. The current source has 42 runtime-behavior readers, 16 SET/validation-only readers, 890 behaviorally unread variables, and 730 writable-but-unread variables. The replay compares warnings on only 62 of 11,465 statements, and its 715 `BothRejected` statements discard both engines' error details rather than compare errno or text.

## Context and Orientation

Task D belongs under `rust/difftests/` and resolves the recurring conflict shape in `rust/difftests/result-tests/tests/integration_diff.rs` and `join_shape.rs`: duplicated narrative comment blocks plus duplicated ratchet constants. The tool must accept the intended stacked value, preserve both sides' narrative, keep exactly one constant, and format the touched Rust files.

Task B reads `rust/crates/tidb-vardef`, `rust/crates/tidb-session/src/variables.rs`, and `rust/difftests/result-tests/tests/integration_diff.rs`. It must distinguish declared system variables from variables actually read by behavior, variables only accepted/stored, and truly unread variables. It must quote the warning gate output and inspect whether error message text participates in comparison.

Task C belongs to `rust/difftests/result-tests`. Its oracle is the checked-in Go integration test SQL/result corpus. The fourteen currently unaligned topics fall into non-UTF-8 result files, missing referenced collation result files, and account authentication setup. Result files are immutable; only the reader and setup may change.

Task A belongs to `rust/crates/tidb-chunk/src/chunk_util.rs`, `chunk_in_disk.rs`, and `row_in_disk.rs`, with Go fixtures under `rust/difftests/chunk-tests/fixtures/`. Go composes checksum outside AES-CTR: logical spill bytes enter the checksum writer, checksum-framed bytes enter the encrypting writer, and ciphertext reaches the file. Readback reverses those layers and overlays both writers' unflushed plaintext caches at their logical offsets.

## Plan of Work

For Task D, add a small script with a narrow parser for Git conflict markers in Rust source, tests using synthetic conflicts, and a one-line invocation in the closest tooling README. Reject malformed or ambiguous inputs loudly. Verify comment merging, constant de-duplication, caller-supplied values, target-file formatting, and rollback on formatter failure.

For Task B, derive the variable census from parsed source registries and semantic read sites instead of name grep alone. Add or update an operations report that quotes reproducible command output. Run the warning survey to capture its emitted numerator and denominator. Trace the error comparison path and rank blind spots by client likelihood and semantic impact.

For Task C, first reproduce the survey and save the before line. Extract the fourteen topics from the harness's own classification rather than copying the old brief. Run each with `INTEGRATION_TOPIC` and trace enabled under a bounded timeout. Implement encoding-aware result reading and test account setup only where evidence demands it. Re-run every topic, enroll only topics with at most five named divergences, and quote the final compared line.

For Task A, add an explicit spill-encryption mode/API to `DiskFileReaderWriter`. Preserve plaintext as the default. In encrypted mode, build checksum-over-encryption on writes and decryption-under-checksum on reads, including both cache overlays. Extend the Go fixture generator to emit deterministic, Go-authored encrypted file bytes without changing production Go packages, check in the vectors, and add Rust tests for exact bytes, random-access reads across 1024-byte boundaries, unflushed tails, round trips, and cleanup for both row- and chunk-addressed containers.

## Concrete Steps

All Cargo commands run from `rust/` with `CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-handoff-rust/tgt` and 12 build jobs. During iteration, run the smallest named test. Before each task handoff, run the touched crate and `cargo nextest run -p difftest-result-tests -j12`. Run `cargo fmt --all --check` and `cargo clippy --all-targets`. The final Ready profile also runs `make lint` from repository root. Never run `make bazel_lint_changed`.

After a task's green implementation commit, copy every file to be mutated to a temporary saved path, alter one rule at a time, run the named test and require failure, then restore by copying the saved bytes back. Do not use `git checkout` or `git stash`. Record each command and the failing test line here before moving on.

## Validation and Acceptance

Task D accepts a synthetic conflict, produces one requested ratchet constant, retains unique narrative from both sides, removes all conflict markers, and runs Rust formatting. Its named test must fail when any of those rules is mutated.

Task B is accepted when every reported count is emitted verbatim by a reproducible command or test, the warning gate line is current, and the report proves whether error text is compared. A stale expected count must fail a named test or report checker.

Task C is accepted when all fourteen old alignment failures have current diagnoses, the removable harness failures align, eligible topics are enrolled, no result file changes, and the harness prints a larger compared numerator with per-topic divergence narratives.

Task A is accepted when Go-generated encrypted bytes equal Rust's on-disk bytes, encrypted files do not expose a named plaintext marker, encrypted row/chunk containers read back correctly before and after buffer boundaries, and plaintext vectors remain byte-for-byte unchanged.

The overall handoff is accepted only after exact per-crate commands, result-test commands, format, clippy, Ready validation, risk notes, and unverified surfaces are recorded.

## Idempotence and Recovery

The resolver refuses an already-resolved file because there is no conflict left to prove it handled. On a real conflict it restores the original bytes if formatting fails. Census and fixture generators write deterministic output; rerunning them must produce no diff. Topic setup must use fresh in-memory sessions and leave no external database state. Spill tests use crate-scoped temporary-storage locking and remove their own directories. Mutation probes restore from saved copies and verify `git diff` returns to the committed baseline after every probe.

## Artifacts and Notes

Resumed branch evidence:

    d93e689e89b67fc940f0cfacee9e96ac513c9a58 rust: prove the reader lock sets accumulate, and pin the commit-at-my-own-ts boundary
    82e0e9add2ed9f0a810f87c4a3220396628f7de3 rust: add encrypted spill file stack

Task D replacement mutation evidence, run with `ruby rust/difftests/tools/resolve-ratchet-conflict-test.rb` after saving the script and test in `/tmp/tidb-task-d-probes.IkiwQi`:

    Drop the incoming narrative -> test_accepts_all_stacked_join_shape_values_in_one_conflict and test_merges_narratives_and_deduplicates_the_integration_ratchet FAILED
    Remove constant-name de-duplication -> three named tests FAILED
    Ignore caller-supplied values -> three named tests FAILED
    Remove formatter rollback write -> test_restores_the_conflict_when_formatting_fails FAILED
    Restore both saved files -> 4 runs, 41 assertions, 0 failures, 0 errors, 0 skips

Task C current-oracle evidence:

    integrationtest replay over 109 topics: 8099 of 10972 statements compared
    integrationtest replay over 110 topics: 8234 of 11465 statements compared
    warning gate reaches 62 of 11465 statements across 110 topics
    257 topics align, 0 do not

The fourteen formerly blocked topics now report these `(matched, diverged, total)` tuples:

    executor/charset (135, 16, 214)
    executor/insert (1082, 88, 1400)
    expression/charset_and_collation (496, 49, 733)
    new_character_set (95, 14, 110)
    new_character_set_builtin (154, 39, 221)
    planner/core/integration (1189, 173, 1598)
    planner/core/integration_partition (132, 3, 493)
    planner/core/tests/prepare/issue (275, 19, 321)
    collation_agg_func (51, 15, 71)
    collation_check_use_collation (79, 9, 92)
    collation_misc (63, 12, 90)
    collation_pointget (72, 28, 105)
    ddl/sequence (187, 27, 268)
    executor/simple (220, 9, 357)

Task C mutation evidence, after saving the four mutated files in `/tmp/tidb-task-c-probes.JzOnwP`:

    Replace the production byte reader with UTF-8 decoding -> integrationtest_replay_matches_recorded_tidb_output FAILED on integration_partition.result
    Drop the live collation recording suffix -> collation_topics_select_the_recording_for_the_live_mode FAILED
    Install privilege checks before the mysqltest initial database -> initial_database_is_selected_before_sql_use_privilege_checks FAILED
    Remove unsupported CREATE USER account-row recovery -> unsupported_account_annotations_still_leave_the_recorded_account_row FAILED
    Remove the eligible topic from enrollment -> warning_comparison_covers_only_enable_warnings_statements FAILED at 57 of 10972 instead of 62 of 11465
    Restore all four saved files -> cmp reported identical; git status showed only untracked tgt/

Task B evidence on the current tip:

    census: declared=948 runtime_behavior=42 set_or_validation_only=16 behaviorally_unread=890 sum=948
    writability: writable_declared=785 writable_behaviorally_unread=730 read_only_or_scope_none_unread=160
    Go hook occurrences: Validation=95; SetSession=278
    warning gate reaches 62 of 11465 statements across 110 topics
    named tests: warning_comparison_covers_only_enable_warnings_statements PASS; allow_empty_tables_name_live_registry_entries PASS

Task B mutation evidence, after saving the three mutated files in `/tmp/tidb-task-b-probes.MWFTXn`:

    Remove autocommit from runtime readers -> test_current_source_counts_are_pinned FAILED at 41/16/891 and 731 writable-unread
    Remove tidb_retry_limit from the priority report -> test_priority_classification_is_present FAILED
    Restore stale enable_resource_metering allow-empty name -> allow_empty_tables_name_live_registry_entries FAILED
    Restore singular tidb_capture_plan_baseline allow-empty name -> allow_empty_tables_name_live_registry_entries FAILED
    Restore all three saved files -> cmp reported identical; 2 runs, 12 assertions, 0 failures, 0 errors, 0 skips

Final Ready evidence:

    cargo nextest run -p tidb-chunk -p tidb-util -p tidb-session -j12 -> 1310 passed, 9 skipped
    cargo nextest run -p difftest-result-tests -j12 -> 99 passed, 5 skipped
    ruby rust/difftests/tools/resolve-ratchet-conflict-test.rb -> 4 runs, 41 assertions, 0 failures
    ruby rust/difftests/tools/sysvar-census-test.rb -> 2 runs, 12 assertions, 0 failures
    cargo fmt -p difftest-result-tests --check -> PASS
    cargo fmt --all --check -> FAIL on seven pre-existing files outside this bundle
    cargo clippy --all-targets -j12 -> PASS with three pre-existing warning classes outside the changed lines
    make lint -> exit 0; its macOS run still prints the existing gobinaryrow internal-package and BSD find diagnostics
    git diff --check d93e689e89b67fc940f0cfacee9e96ac513c9a58..HEAD -> PASS
    self-review -> 27 expected paths, no integration result oracle changes, no probe files, and tgt/ remains untracked

The handoff forbids pushing. The final artifact is a local branch name plus `git log --format="%h %p %s"` in parent order.

## Interfaces and Dependencies

Task D depends only on Ruby's standard library plus the workspace's installed `cargo fmt`; introducing another package dependency would be disproportionate for conflict text processing.

Task C uses the existing `difftest` replay APIs and `tidb-session` in-memory execution surface. It must not add an alternate SQL parser or edit Go oracle results.

Task A reuses `tidb_util::encrypt::{CtrCipher, Writer, Reader}`, `tidb_util::checksum::{Writer, Reader}`, `tidb_util::layered_io::{CloseWrite, ReadAt}`, and `row_in_disk::ReaderWithCache`. New public configuration should be a narrow Rust enum rather than a stringly typed session dependency because `tidb-chunk` currently has no server configuration object.

Revision note: created on 2026-08-06 from the four-task handoff; reconciled on the dedicated resumed worktree and updated through all four task-level mutation gates.
