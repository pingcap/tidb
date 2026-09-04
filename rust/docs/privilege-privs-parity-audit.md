# privilege privs parity audit: Go `pkg/parser/mysql/privs.go` vs `privs.rs`

Audit date: 2026-09-05. Method: mechanical extraction of the four scope
lists from both trees, element order and count compared.

## Results

| List | Go | Rust | Verdict |
| --- | --- | --- | --- |
| `AllGlobalPrivs` | 32 | 32 | MATCH (order identical) |
| `AllDBPrivs` | 19 | 19 | MATCH |
| `AllTablePrivs` | 13 | 13 | MATCH |
| `AllColumnPrivs` | 4 | 4 | MATCH |

The only textual difference is enumeration spelling: Go's
`ShowDBPriv`/`CreateTMPTablePriv` correspond to Rust's semantic
`ShowDatabases`/`CreateTemporaryTables` variants — same privilege, same
position, same print name. `GrantOption` is correctly never a member of
any `ALL_*` list on either side.

## Remaining surface

Dynamic privileges (Go `dynamicPrivs` in
`pkg/privilege/privileges/privileges.go`), password-expiry policy, and
SET-ROLE semantics are per-behavior audits, not table diffs.

## Dynamic privileges (2026-09-05, second pass)

Go's `dynamicPrivs` registry (21 entries, `privileges.go:60-82`) matches
Rust's `DYNAMIC_PRIVS` element for element. Go canonicalizes names to
upper case at registration and refuses names over 32 characters; Rust's
`is_dynamic_privilege` compares case-insensitively, which is the same
observable matching. `RegisterDynamicPrivilege` (plugin extension) is
deliberately unported — this tier loads no plugins and the module doc
records the `const` decision. Still open as a behavior surface:
SET-ROLE/role-graph semantics and password-expiry policy.

## Password expiry (2026-09-05, third pass)

`check_password_expired` (`registry_ops.rs:812`) ports Go's decision
(`privileges.go:490-513`) faithfully: the `Password_expired` flag, the
lifetime ladder (`NULL` falls to the `default_password_lifetime` global,
`0` is NEVER, positive lifetimes age out), the sandbox-mode branch that
admits the login instead of refusing, and error 1862 with the verbatim
message.

One recorded micro-divergence: Go ages the password with
`AddDate(0, 0, days).Before(now)` — calendar days in the server's
location, which shifts by up to an hour across DST transitions — while
Rust compares exact seconds. Observable only when a password's exact
expiry instant lands inside a DST shift; porting calendar-day arithmetic
would need the session timezone in the registry.

## Role graph (2026-09-05, fourth pass)

`effective_roles` (`registry_ops.rs:944`) is the BFS transitive closure
over the granted role edges with a visited set — activation is
direct-only while inheritance through an activated role is transitive,
matching Go's `FindAllRole` walk — and `RequestVerification`'s identity
order (self first, then roles) is preserved by
`identities_for_check`. The dynamic grant/revoke pair carries Go's
subtle rules verbatim: re-granting overwrites `with_grant_option`, and
`REVOKE ALL ON *.*` deletes every dynamic privilege row. SET-ROLE
statement semantics remain the one unexamined slice.

## SET ROLE (2026-09-05, final pass) — AUDIT CLOSED

`set_role_stmt` (`account.rs:725`) ports Go's `executeSetRole` family
completely: all five selections (NONE, ALL, DEFAULT, ALL EXCEPT, named
list), the 3530 `ErrRoleNotGranted` gate for a role the account does not
hold, duplicate-tolerance through a set-keyed check, the rejected-set-
leaves-previous-state rule, and the privilege-bypass fast path —
captured-verified per the in-file notes. `set_default_role_stmt` carries
the authorization gate (self needs nothing; others need UPDATE on
mysql.default_roles or global CREATE USER) and the CURRENT_USER
resolution order. The privilege audit is closed with no open items.
