# `pkg/util/password-validation` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All three pinned artifacts were read in full: `password_validation.go`,
`password_validation_test.go`, and `BUILD.bazel`. The package has one
production file, five exported validators, five unit tests, and one Bazel
library/test pair. It has no package doc, README, harness, fixture, benchmark,
generated file, platform variant, or ownership file. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/password_validation.rs` owns the five validators.
`GlobalVarAccessor`, `PasswordUser`, and `PwdError` are the minimal Rust bridge
for Go's imported accessor, user-identity, and error interfaces; they do not
add policy. Passwords and both user names now use `GoString`, preserving Go's
arbitrary-byte domain for the raw `bytes.Contains` and byte-reversal checks,
while lowercasing and rune classification decode invalid UTF-8 with Go's
replacement behavior.

The audit removed the package-level `validation_enabled` helper, eight public
sysvar constants/catalog entries, the public error-code helper, non-required
clone/equality derives, the extra module policy document, one supplemental
test, and supplemental rows embedded in two source tests. Exactly the five Go
test functions remain.

Enablement now belongs to the same callers as Go. The expression
`VALIDATE_PASSWORD_STRENGTH` path propagates a global-variable read error. The
session account-DDL path treats that read error as disabled, matching
`SimpleExec.isValidatePasswordEnabled`. The executor source-suite carrier owns
its own vardef spellings rather than extending the validator package API.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --lib --locked password_validation::tests::
  --no-fail-fast` — passed (exactly five source tests).
- `cargo check -p tidb-expr --lib --locked` — passed.
- `cargo test -p tidb-expr --lib --locked
  builtin_ext::crypto::tests::validate_password_strength_go_vectors --
  --exact` — passed.
- `cargo test -p tidb-expr --lib --locked
  tests::crypto_encryption_source::test_validate_password_strength --
  --exact` — passed.
- `cargo test -p tidb-executor --lib --locked
  tests_passwordtest_source::validate_password_policy_matrix_over_the_shared_validator
  -- --exact` — passed.
- `cargo test -p tidb-session --lib --locked
  tests_grants::password_policy::validate_password_enforces_account_writes_and_scores_sql_values
  -- --exact` — passed.
- `cargo test -p tidb-session --lib --locked
  tests_global_vars::statement_context_reads_global_sysvars_through_the_live_accessor
  -- --exact` — passed.
- `rustfmt --edition 2021 --check` on the changed owner/session/executor files
  and `git diff --check` — passed. The changed expression file compiles and its
  two targeted tests pass; a whole-file rustfmt check is blocked by unrelated
  pre-existing long-line drift later in that file.
- `go test ./pkg/util/password-validation -count=1` — blocked before this
  package compiled by the workspace's existing missing
  `pkg/util/hack.checkMapABI` build selection and
  `google.golang.org/grpc/internal/transport` / `http2.TrailerPrefix`
  dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; byte strings, caller-specific enablement errors,
  policy ordering, warnings, and all five source tests now match Go.
- Compatibility: intentionally removes Rust-only public constants, helper,
  derives, and error-code surface; all production and test consumers were
  migrated.
- Performance: neutral to improved; raw byte checks avoid an eager lossy
  password conversion, while rune-based checks perform the same decoding work
  Go requires.
