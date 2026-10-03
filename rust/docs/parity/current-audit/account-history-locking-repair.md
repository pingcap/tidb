# Shared account history and locking-image repair

This cloud batch closes A04's recorded accepted-as-no-op contract and advances
A02's durable account image. The register now has **65 unresolved (55 open, ten
partial), 21 repaired, 86 tracked**. These are tracked ownership findings, not a
count of all possible defects or complete Go package acceptance.

## Source and ownership

Work started at integration `cac85ecaab211c337149f547e7afb18931ed1755`, a merge
preserving the local account prerequisites and remote schema-acknowledgement
commits through `ee637c3a3992dc6acb4a669e9a5dbba846573474`. Freshly fetched Go master
is `93a01d31f6da205ae4bf376825293903a6899fdb`; native client-rust is unchanged at
`19a56ccda1e128218cd33c69709038219aced9bc`. Master selects client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`; integration/oracle pins are not substituted
for this comparison. No native source change or synchronization was needed.

Go `pkg/executor/simple.go` owns option loading, whetherSavePasswordHistory,
getUserPasswordLimit, passwordVerification, count/time checks, deletion/insertion,
and CREATE/ALTER/SET/DROP/RENAME transactions. Go
`pkg/privilege/privileges/privileges.go::PasswordLocking.ParseJSON` owns stored
locking limits/counts/epochs. The complete original package inventories remain in
[package-coverage.json](package-coverage.json): executor 191 artifacts, privileges
10, and the selected simpletest support package three. Their sources, generated,
platform, build, original test and fixture obligations remain **unaccepted**.

Rust extends the existing Session registry, tidb-exec system-row reader/writer and
tidb-server account transaction/publication bridge. The locking DTO is shared
between load and Session; its duplicate definition was removed after callers
migrated. Account metadata and secondary-password JSON patching now use that
shared record, replacing the alternate SQL JSON patch path.

## Repaired behavior

Nullable history/reuse limits retain DEFAULT versus explicit zero and use live
global defaults. Timestamped history retains UTC microseconds, nullable Password
and orphan identities during load/export/write/reload. Reuse checks protect the
union of recent-count and time windows before changing credentials. Successful
changes prune only rows outside both windows; native credentials use encoded
comparison and SHA-2/SM3 use the existing salted verifier. Code 3638 is rendered
by the existing TiDB catalog/shared formatter, including the exact Go message.

CREATE initializes history; ALTER and SET enforce it; plugin changes clear
incompatible history; DROP and RENAME migrate every row. Empty **encoded**
credentials are excluded. Go's salted plugins can encode empty plaintext as a
nonempty credential. CREATE excludes auth-token history; ALTER/SET additionally
exempt LDAP. These distinct source behaviors are retained. Count/time cutoffs
preserve Go's process-Local text followed by session TIMESTAMP interpretation.
The standalone mirror converts UTC registry timestamps into session-local text;
the cluster row writer encodes UTC timestamps directly.

Full raw attributes survive unrelated writes. Failed-login limits, counts,
automatic-lock state and original epochs are restored without starting a new
lock. Unknown metadata/locking keys and secondary credentials are preserved.
ACCOUNT UNLOCK resets state through the existing owner; removing both locking
options removes their JSON member while retaining unrelated attributes.

Two real failures discovered by the account tests were also repaired:
`GRANT ALL` includes static CREATE ROLE/DROP ROLE, as Go composeGlobalPrivUpdate
requires, and synthesized SCHEMATA rows pass the existing visibility owner, as
Go setDataFromSchemata requires.

## Test review and validation

The controlled baseline run reproduced 31 existing account/grant failures:
80 passed, 31 failed. Reversing the saved batch patch and restoring it in a
finally block preserved source changes. Go master formatAccountName/stringutil.Escape
uses backticks by default; 106 obsolete output literals were replaced. Input SQL,
valid assertions and test functions remain. Kill fixtures now use valid even
32-bit IDs instead of Go-truncated odd IDs. PROCESS's explicit metrics_schema
exception and today's system-schema inventory replace stale expectations.
The valid ALL and SCHEMATA regressions were fixed in production code.

History reuse and lost locking JSON each fail through real embedded unistore SQL
before repair. Self-review regressions also reproduced the standalone timezone
mirror error and CREATE's LDAP history omission before their fixes. Storage
round-trip tests cover microsecond identities, orphan/NULL history and unchanged
writes. Production SQL covers unchanged credentials on rejection, multi-account
rollback, rename, SET rejection, plugin reset, DROP cleanup, raw locking-state
preservation, unlock and durable ALL role privileges.

The final targeted suites pass 156 distinct tests: 115 Session account/grant,
15 storage, four policy, 17 publication and seven server account tests (two
publication tests overlap). Affected all-target checking and root make lint pass.
Exact commands, exits, counts and SHA-256 log receipts are in
[account-history-locking-validation.json](account-history-locking-validation.json).
It also rechecks all 66 prior unresolved recorded source references: 63 are
unchanged, three changed (A02/A04 advanced; O02's bootstrap-version selector
remains missing). This continuity review is not 66 fresh runtime reproductions.

`make lint` initially returned zero despite module-loading errors. Required large
archives redirected to denied storage.googleapis.com. Exact upstream GitHub tags
were packaged with x/mod/zip and accepted only after Go checked the existing
go.sum. A mismatching lz4 directory archive was rejected and unused; the exact
Git-tag VCS archive matched. Final lint runs without dependency errors. No
lockfiles, expected checksums, TLS verification or package-signature rules changed.

## Remaining scope and publication

A02 stays partial: mysql.global_priv/TLS, durable wire-login counter updates,
secondary-password authentication consumers and broader cache invalidation are
unfinished. Historical/DST abbreviation lookup and the complete platform matrix
are not accepted. Full upstream Go suites, live multi-node TiKV/mixed-node
security, TLS policy and sysbench/TPC-C/TPC-H/YCSB performance were not verified.
No speedup or complete parent-package transcreation is claimed.

The actual tracked hook passed with the staged batch (logged in the JSON receipt),
is active via core.hooksPath=hooks, and enforces
`cd rust && cargo build --locked -p tidb-server`. Every push requires that same
fresh locked build immediately beforehand, normal push only, and remote SHA
verification. The observed permission blocker is:

    remote: Permission to pingcap/tidb.git denied to ngaut.
    fatal: unable to access 'https://github.com/pingcap/tidb.git/': The requested URL returned error: 403

This is repository-scoped write denial; the response does not distinguish account
role from GitHub-app installation scope. Preserve pingcap/tidb hparser-integration
and ngaut/client-rust master. No alternate fork or laptop credentials were used.
Local commits and a verified recovery bundle must be retained in the cloud
snapshot; fetching unpublished commits from GitHub or fresh-task restoration is
not established while publication is blocked.

Source implementation commit: `4b1aebe025a6b832f3f6cf9986e47e4f70569b77`. The
actual commit hook passed, then a fresh locked build passed immediately before
the normal push attempt. Push exited 128 with the diagnostic above; the remote
was verified unchanged at ee637c3a3992dc6acb4a669e9a5dbba846573474. This
documentation follow-up records those outcomes without another identical denied
push. Future publication still requires the fresh locked build.
