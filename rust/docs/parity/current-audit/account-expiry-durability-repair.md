# Preserve account expiry policy through its durable owner

Cloud integration start: 05ed8bc4606dcf592258b96c5ac72a3bbb28d2b6. Fresh comparison
Go master: 93a01d31f6da205ae4bf376825293903a6899fdb. Native client remains
19a56ccda1e128218cd33c69709038219aced9bc; no native change or sync is required.

## Result and source contract

The existing account loader now projects password_lifetime and
password_last_changed. It reads nullable integer/TIMESTAMP values with an explicit
UTC decoder timezone. Missing older columns remain distinct from fabricated new
password-change timestamps. Registry publication restores the stored expiry flag,
nullable lifetime and timestamp directly, including locked/passwordless rows.

The shared UserRecord retains one typed nullable timestamp. Its login epoch is
derived from that value, eliminating an independently mutable epoch copy. Account
creation and an actual password change produce the timestamp; reload, export and
unrelated account writes do not. The export retains Go's packed temporal value and
the public row's existing ordering traits. The same account mutation owner now
writes typed lifetime and timestamp values, preserving SQL NULL, explicit zero
(NEVER), original epochs and zero timestamps. Its existing storage/index/transaction
machinery still owns the mutations. Unchanged images produce no mutations.

Go ownership: pkg/privilege/privileges/cache.go decodeUserTableRow reads nullable
Password_lifetime and Password_last_changed. privileges.go CheckPasswordExpired
uses per-user lifetime or the live global default and the stored change time;
executor/simple.go account writes change the timestamp with the password. This
batch maintains those existing Rust owners; it is not a complete Go-package port.

## Evidence

The publication regression failed on the pre-change code: lifetime Some(7) was
rebuilt as None. The durable regression also failed with the insertion-side expiry
write withheld, while retaining the new DTO so the test compiled: Some(7) again
read back as None. Both pass after the repair.

The real embedded-store SQL regression also passes: CREATE persists interval 7,
a direct stored timestamp edit survives ACCOUNT LOCK, NEVER and DEFAULT write
0 and NULL respectively, and ALTER password/SET PASSWORD use the same owner.

Tests cover old timestamps, nullable DEFAULT versus explicit NEVER, live global
lifetime selection, unchanged export/write behavior, password-change epoch refresh,
NULL versus zero TIMESTAMP, locked accounts and all existing account/grant writer
controls. All 37 distinct targeted tests pass, as do the affected all-target check, root
make lint and locked server build. Final commands, test counts, exit codes and
hashes are recorded in
[the machine receipt](account-expiry-durability-validation.json). Cloud logs are
under /workspace/.cloud-setup/account-batch2. The first intermediate storage run
exposed the row decoder's required timezone; the loader was corrected and the
entire account writer target was rerun successfully. The cloud disk later filled
with 18 GiB of rebuildable Cargo incremental caches; those caches alone were
removed, and the SQL test was rerun successfully with 17 GiB free. No source or
committed evidence was removed.

## Remaining scope and publication

A02 remains partial: global_priv/TLS policy, locking JSON/epochs and other user
attributes remain uncarried. A04 password history/reuse remains open. The count
stays 66 unresolved (56 open, ten partial), 20 repaired, 86 tracked. This does not
claim mixed-node TiKV authentication, non-UTC calendar-day/DST parity, full Go
suites, complete upstream package acceptance or measured workload improvements.
The prior broader parser library run still has 16 retained failures.

Actual hooks/pre-commit must run cargo build --locked -p tidb-server for the
commit, and the same build must be rerun immediately before a push. The existing
GitHub permission blocker was Permission to pingcap/tidb.git denied to ngaut,
HTTP 403; preserve the exact destination and all unpublished cloud commits.
