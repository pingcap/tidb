# Cloud source review and test maintenance

Cloud checkout `/workspace/tidb` starts at b6520ebe83c992a34cf75dc7e381b1eef20e288f
on hparser-integration. Freshly fetched Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb; native client is unchanged at
19a56ccda1e128218cd33c69709038219aced9bc. Go selects client-go
v2.0.8-0.20260928031501-8edb23f6c7ee.

## Finding dispositions

All 66 carried unresolved IDs were compared with the prior per-ID source hashes.
44 retain unchanged recorded evidence; 22 reference changed owners. The intervening
DDL changes implement MDL/non-MDL synchronization, not ordinary DDL dispatch,
parallel scheduling, complete reorg/delete-range or seed acceptance. Statistics
maintenance does not compose store GC, TTL, RU writers, cross-keyspace management,
workload learning or inference. Removal of private server interpreters does not
supply server-ID leasing, DXF or TopSQL managers. Configuration repair leaves native
default consumers and TLS verification open. These changes do not close any entire
remaining root finding. Per-ID hashes and carried assessments are in
[the validation receipt](cloud-account-test-validation.json). These are source
review dispositions, not 66 freshly reproduced runtime failures.

After the empty-account subrepair, there are **66 unresolved: 56 open and ten
partial**, and 20 repaired (86 tracked). A02 becomes partial; no whole upstream
package is accepted. The larger A02 durable policy and A04 password reuse batch
remains incomplete.

## Tests removed, retained and corrected

Removed `the_anonymous_row_is_not_created_at_all`, which asserted that the loader
must discard an empty username. Go cache.go decodeUserTableRow retains that row;
matchUser indexes the requested username without a different-user fallback.
Its replacement checks load count, identity/host matching, privileges, refusal to
match a different username, write admission and full account-image roundtrip.
The production discard and its resulting write-refusal guard were removed together.
With the old production discard restored, the replacement fails (zero accounts
versus one); after repair all 14 cluster-privilege tests pass.

The hint-source tests remain useful Go obligations. Three stale parse_hint API
calls now supply the explicit column origin. The newly executable suite exposed
a valid failure: bare parenthesized hints reported only the line number. Direct
ParseHint observations from freshly exported Go master report EOF columns 19,
22 and 19 with `near ""` for NO_DECORRELATE, SEMI_JOIN_REWRITE and USE_PLAN_CACHE.
The existing parser diagnostic owner now retains that EOF location; all four
hint-source tests pass, including an exact Go-output regression. No diagnostic
assertion was weakened or removed. Go probe: `go run -mod=readonly ./cloudhintprobe`
in the disposable master pkg/parser export; exit 0.

The broader parser library run has 719 passing and 16 failing tests. Those tests
were retained; removing genuine unsupported or incorrect behavior from the test
suite would hide work. A02 expiry/TLS and locking probes fail because policy fields
are still omitted. The A04 SQL sequence CREATE USER ... PASSWORD HISTORY 3,
ALTER ... second, ALTER ... first wrongly succeeds. Fail-before logs remain in
`/workspace/.cloud-setup/account-batch/{server-before,session-before}.log` and their
acceptance requirements remain in the account ExecPlan. These exploratory probes
are not presented as implemented passing regression coverage.

## Validation and publication

Commands/statuses are recorded in the JSON receipt: 14 cluster-privilege tests
and four hint-source tests pass; affected all-target checking, root make lint,
and cargo build --locked -p tidb-server pass. The larger parser library retains
16 failures (719 passing), so the whole parser package is not declared ready.
The actual hooks/pre-commit is configured and must pass that locked build to
create the commit; logs remain in the cloud snapshot. git diff --check passes. No multi-node TiKV,
whole Go suite, complete package acceptance or workload speedup is claimed.

Prior TiDB push diagnostic: `Permission to pingcap/tidb.git denied to ngaut.`;
HTTP 403. It establishes repository authorization failure but does not distinguish
account role from GitHub-app installation scope. The user will arrange access;
retain pingcap/tidb hparser-integration and never force-push or bypass hooks.
