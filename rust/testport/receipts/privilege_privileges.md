# `pkg/privilege/privileges` — user-attribute and OPERATE VIEW receipt

Status: complete direct-package inventory for the bounded privilege-cache and
user-attribute filter batch. The nested `pkg/privilege/privileges/ldap`
package and its certificate fixtures are a separate package boundary.

Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete Go inventory

The direct Go-master package has ten tracked artifacts and 7,411 lines:
`BUILD.bazel`, four production Go files (`cache.go`, `errors.go`,
`privileges.go`, and `tidb_auth_token.go`), the new
`user_attributes_filter.go`, and four existing test/bootstrap files. This
batch adds `user_attributes_filter_test.go`, yielding eleven direct artifacts
and 7,495 lines in the branch. All direct files were read before further
editing, covering 231 upstream top-level function declarations and 71
test/benchmark/example declarations plus the three focused new tests. There is
no direct `doc.go`, fixture/testdata directory, generated source, or
platform-specific variant. The nested LDAP package contains its own Go files,
BUILD target, tests, and certificate/key fixtures and is not claimed here.

## Go behavior restored

The privilege cache now loads and recognizes `OPERATE VIEW` at global,
database, and table scope and treats it as sufficient to make a database
visible. The new `UserAttrFilter` implements MySQL's USER_ATTRIBUTES row
visibility rules: unrestricted for SELECT/UPDATE on `mysql.user`, unrestricted
for CREATE USER plus SYSTEM_USER, non-system rows for CREATE USER without
SYSTEM_USER, and self-only otherwise. A nil or non-TiDB privilege manager
preserves the existing unrestricted fallback.

Focused source-level regressions cover the nil-manager fallback, self/non-system
filtering, SELECT visibility, and the OPERATE VIEW database-visibility bit.
The Go-master cache loader tests now include the new privilege column and set
member. The SEM-v2 columnar-storage privilege regression is also restored and
passes against the already-restored system-variable path.

The Go-master `TestInfoSchemaUserAttributes` integration case was read in
full but remains an explicit consumer boundary: its row filtering is wired by
`pkg/executor/infoschema_reader.go`, not this package. It currently fails
before that executor package is restored (an ordinary user still sees all
rows), so it is not added to this package batch. The focused filter tests pin
the package-owned behavior without hiding that boundary.

No separate Rust privilege-package facade was added: the bounded
`pkg/executor` consumer alignment uses the existing `PrivilegeRegistry` host
matching and dynamic-privilege APIs directly. The executor receipt
(`receipts/executor_user_attributes.md`) owns that SQL-visible row materializer;
this receipt remains limited to the Go privilege-cache and filter semantics.

## Regression and validation

```text
./tools/check/failpoint-go-test.sh pkg/privilege/privileges \
  -run '^Test(UserAttrFilterNilManagerAllowsAllRows|UserAttrFilterRestrictsSelfAndNonSystemRows|OperateViewMakesDatabaseVisible|LoadUserTable|LoadDBTable|DBIsVisible|CaseInsensitive)$' \
  -count=1 -vet=off
# PASS

./tools/check/failpoint-go-test.sh pkg/privilege/privileges \
  -run '^TestColumnarStorageEnabledSEMV2$' -count=1 -vet=off
# PASS

make lint
# PASS (shared Ready run)

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS (shared Ready run)

git diff --check
# PASS

make bazel_prepare
# BLOCKED: make: bazel: No such file or directory
```

## Risks and unverified surfaces

- Correctness risk is concentrated in matching the viewing account and
  distinguishing SYSTEM_USER targets; focused unit tests cover those modes.
- The Rust executor consumer and end-to-end INFORMATION_SCHEMA filtering are
  covered by the focused session regression, but the full executor package
  remains an explicit unverified boundary.
- Bazel analysis, Windows/platform builds, and full-workspace tests were not
  run locally.
