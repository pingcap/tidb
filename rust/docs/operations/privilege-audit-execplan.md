# `pkg/privilege` parity audit ExecPlan

## Objective

Keep the complete Go-master `pkg/privilege` inventory and the Rust
`tidb-session` privilege owner aligned as one atomic package boundary. Read all
Go production, test, fixture, and Bazel artifacts before editing; record the
ownership decision, measured fix, regression, and Ready validation in
`rust/testport/receipts/privilege.md`.

## Completed this batch

1. Inventoried all 22 tracked Go artifacts (8,415 lines): manager/connection
   interfaces, cache and manager implementations, LDAP source and tests,
   JWT source/tests, 50/4-shard Bazel targets, goleak harness, and embedded
   certificate/key fixtures. No generated or platform-specific Go artifact
   exists.
2. Compared the cache matcher with the Rust `PrivilegeRegistry` owner and
   reproduced a Unicode database-name mismatch: Go's `strings.ToUpper` is
   Unicode-aware but Rust used ASCII-only folding.
3. Added the focused regression
   `database_matching_folds_non_ascii_like_go_strings_to_upper`; it failed
   before the production change and passes with Unicode folding in
   `registry_ops::database_matches`.
4. Fixed INFORMATION_SCHEMA privilege-table metadata parity by preserving
   logical mem-table output names in the physical plan and setting each virtual
   scan column's `orig_name`; planner and owner-level regressions cover both
   boundaries and the full owner suite is green (50 passed, 3 ignored).
5. Keep the package as an explicit SEED/boundary: LDAP/JWKS, extension hooks,
   manager/session integration, and storage reload lifecycle do not have a
   dependency-closed Rust owner and were not invented here.

## Validation gate

- [x] Focused regression fails before the fix and passes after it.
- [x] Rust privilege owner suite (50 passed, 3 ignored).
- [x] Workspace `cargo check --offline --locked`.
- [x] `cargo fmt --all -- --check`.
- [ ] Ready profile `make lint`.
- [ ] Fetch remote, create one meaningful batch commit, push to
      `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The complete Go package remains broader than the Rust owner. LDAP network
behavior, JWT/JWKS key rotation, extension callbacks, privilege-manager
session wiring, lazy/full storage reload, and the Go integration tests remain
explicit gaps for a future dependency-closed package batch. The repository
package loop continues after this receipt; this plan is not a whole-repo
completion claim.
