# Share session SQL test helpers

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove repeated executable harness code while preserving every SQL case and
assertion. Work in /workspace/tidb on hparser-integration from
cb1d4eb031beb75272134dc851cbac6ac35eb60e. Refreshed Go master is
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Go pkg/testkit centralizes query
execution and result rendering. The Rust session integration target already
shares one executable, but 223 modules still repeat 245 helper bodies.

## Progress


- [x] Refresh Go master and inspect Go testkit ownership and repeated Rust helpers.
- [x] Replace 245 copies with 27 shared helpers; preserve other module bytes.
- [x] Run all 256 affected SQL tests: 246 pass, ten identical baseline failures; verify source continuity.
- [x] Pass lint, metadata, formatting and diff checks; record validation and update pointers.
- [ ] Commit through actual hook, fresh locked build, push and verify remote.
- [ ] Save verified recovery bundle and Cloud checkpoint.

## Milestones and Plan of Work


Move identical helpers from rust/crates/tidb-session/tests into support/mod.rs.
Register support once in all.rs. Import the original local function name in each
caller. Retain distinct integer, string, byte, NULL, error-length and statement
result conventions; do not normalize expectations or delete failing cases.
The new module is test support, not a new production or Cargo target.

Compare every modified caller to its before-image after substituting its shared
import back with the original function: the complete file must match byte for
byte. Compare shared function bodies to the originals after renaming. Run all
223 affected module filters in one Cargo invocation; preserve and investigate
any failures against the original helpers. Then run make lint and git diff
--check, update this plan and the current-audit cleanup receipt and pointers.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in build shells; run Cargo from rust/.
Use cargo test --locked -p tidb-session --test all -- followed by the affected
module filters listed in the receipt. All SQL inputs, assertions, test names,
attributes and registrations remain identical. No production code, dependencies,
fixtures or Go/Bazel files change. Compare Cargo metadata before/after; no target
change is expected. No new redundant helper tests are necessary.

From repository root run make lint and git diff --check. Commit normally with
core.hooksPath=hooks; the actual hook must pass cd rust && cargo build --locked
-p tidb-server. Repeat that locked build immediately before normal push to
origin hparser-integration, then verify remote SHA. Never bypass hooks or force
push. Keep native client-rust unchanged.

## Surprises & Discoveries


The same SQL helper is copied up to 38 times. Two groups differ only in their
local function name and share one implementation. Other differences include
panic context, NULL spelling and error truncation; these remain explicit.

## Decision Log


Move only exact duplicates, preserving function bodies and import aliases.
This follows shared Go testkit ownership without claiming complete testkit
transcreation. Keep unique helpers and all existing behavioral tests. The
unrelated lint-only and atomic-only tests reviewed earlier are outside this
connected session-harness batch.

## Outcomes & Retrospective


Implementation removes 218 duplicated helper definitions and 3531 net Rust
harness lines. The selected suites report 246 passes and ten failures, zero ignored.
All ten failure messages reproduce identically with original helpers.
Metadata, source continuity, shared-module formatting, lint and diff checks pass.
Publication remains pending. No parity root closes: 86 tracked, 30 repaired,
56 unresolved (27 open, 29 partial). No measured speedup is claimed.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show
cb1d4eb031beb75272134dc851cbac6ac35eb60e:<path>, preserving concurrent work.
External inventory, migration and command logs live in
/workspace/.cloud-setup/session-sql-helper-cleanup. The committed receipt will be
rust/docs/parity/current-audit/session-sql-helper-cleanup-validation.json.
Saving the Cloud draft, publishing settings and testing a fresh restore are
distinct operations. No dependencies change.

Revision: replace completed statistics documentation retirement with the
connected session SQL helper cleanup and exact source-continuity checks.
