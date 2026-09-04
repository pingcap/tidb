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
