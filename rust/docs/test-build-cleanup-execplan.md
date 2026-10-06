# Retire the mixed metadata test carrier

This living ExecPlan follows root PLANS.md. The preceding model target cleanup
is published as 7328420b5007703491f11edf2f88ef186cc2d4ee; its final evidence is in
/workspace/.cloud-setup/model-test-owner-cleanup/final-handoff.json.

## Purpose and Context


Remove repeated metadata test setup and synthetic representation checks while
retaining Go behavior coverage. Work in /workspace/tidb on hparser-integration.
The refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. The Rust owner
is rust/crates/tidb-model; the source contract is pkg/meta/model. Its mixed
src/tests_pkg_meta_model_part2.rs carrier contains eight tests spanning column,
index, table, TTL and partition owners. Owners already cover several cases.

## Progress


- [x] Compare the eight cases with Go and current owning tests.
- [x] Retire the carrier, migrate unique vectors, and remove representation probes.
- [x] Complete grouped tests, continuity checks, lint and diff review.
- [ ] Commit through the actual hook, fresh locked prepush build and normal push.
- [ ] Verify the remote and save the cloud checkpoint.

## Milestones and Plan of Work


First merge the five-case legacy/current plain/BIT JSON matrix into
column.rs::tests::default_value, retaining its stronger SQL error checks and
existing extra_column_constructors assertions. Move the complete index prefix,
FK partial-condition and pointer-identity case into index.rs::tests, with its
fixture helpers local to that test. Move TestModelBasic to table_info.rs::tests;
discard only its unused local clone/toggle, which no assertion observes.

Next retire the duplicate movement case and check_offsets helper: the existing
move_column_ports_the_exact_upstream_sequence_and_signed_panics tests every
source transition and additional signed/panic behavior. Extend table_tests.rs
clone and interval tests with TTL mutation isolation, 200h and both parsed
source defaults. Set HASH before the existing partition reset test, preserving
the removed test's nonzero input. Remove the carrier and its lib.rs registration.

Finally remove the GoAny type-name substring and native flag-size probes from
pkg_meta_model_package_anchors.rs. The retained high-bit flag roundtrip and GoAny
behavior owners exercise the contracts. Replace synthetic string labels with
direct map allocation, JSON inequality, clone identity and placement assertions.
Update b008's eight mappings and the historical materialized-view receipt's
reference. Finding dispositions and complete package obligations stay unchanged.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh, then from /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --lib -- column::tests index::tests table_info::tests table::tests partition::tests --test-threads=1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --test all -- --test-threads=1
    cargo metadata --locked --no-deps --format-version 1

Require actual selected tests to pass. Verify metadata is byte-identical and
production source prefixes before cfg(test) are unchanged; lib.rs only loses a
test module declaration. Compare the complete moved index case and model-basic
body against their before-images. From repository root run make lint,
git diff --check and rustfmt --check on edited Rust files. No Go or Bazel change
requires bazel_prepare. No production bug or speedup is claimed by this cleanup.

Commit with executable hooks/pre-commit selected by core.hooksPath=hooks; it must
run cd rust && cargo build --locked -p tidb-server. Repeat the same locked build
immediately before normal push to origin hparser-integration, and verify the
remote SHA. Do not bypass hooks or force-push.

## Surprises & Discoveries


The movement owner already executes all eight source moves plus signed-offset
failure cases. The carrier's pk_col clone is toggled and then never read. Anchor
checks include type_name text and native size assertions despite retained
behavioral flag roundtrips. These checks do not establish Go semantics.

## Decision Log


On 2026-10-06 remove the mixed carrier as a unit. Preserve the five JSON cases,
all FK vectors and source clone/reset inputs rather than deleting meaningful
Rust assertions solely because their names differ from Go. Use existing owner
fixtures and one grouped validation boundary. Keep NextGen acceptance unresolved.

## Outcomes & Retrospective


Implementation removes the 606-line carrier, six net test registrations and two
representation-only probes; meaningful cases move to existing owners. Net Rust
reduction is 289 lines. All 116 selected tests, lint, formatting, metadata and
source continuity pass. Publication remains pending; external final-handoff.json
will record the completed hook, prepush build, remote SHA and cloud checkpoint. Production behavior and all 56 unresolved
structural findings are unchanged.

## Recovery, Artifacts and Dependencies


Before-images are available with git show 7328420b5007703491f11edf2f88ef186cc2d4ee:<path>.
Do not overwrite concurrent work. Durable evidence belongs in
rust/docs/parity/current-audit/model-carrier-cleanup-validation.json and external
logs in /workspace/.cloud-setup/model-carrier-cleanup. Dependencies, APIs and
Cargo target declarations remain unchanged. The previous static tests/all.rs
registration and process-default isolation remain mandatory. Cloud draft saving
does not establish environment Publish or fresh-task restoration.

Revision 2026-10-06: replace the completed target-consolidation plan with removal
of the mixed test carrier and direct behavioral assertions.
