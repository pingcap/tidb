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
