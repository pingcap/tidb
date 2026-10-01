# Restore the complete shared PD error owner

This living ExecPlan follows TiDB PLANS.md. The acceptance unit is the entire
pinned github.com/tikv/pd/client/errs package, not an error string or a subset of
transport. It includes errno.go and errs.go, every global/function/type, imported
error/logging contracts, module/build/license inputs and every source consumer.
There is no doc.go, original test file, generated input, fixture, package build
file or platform variant in this package.

## Purpose and context


PD transport must retain coded error identity and wrapped causes so reconnect,
retry and logging decisions use Go's shared contract. Native has no errs owner;
its circuit breaker returns a private uncoded enum. Implement the package before
transport interception rather than add local string conventions. Go master is
93a01d31f6da205ae4bf376825293903a6899fdb and PD remains
v0.0.0-20260805103528-afa43111d149. Native baseline is
69877b9cc651c07d0643c968c194bd8ced88daba; TiDB baseline is
9bd91c282c7c7766c141229b361af9932802f73c. Both branches were pulled and are current.
TiDB selects pingcap/errors 306e305bcf41 and zap 1.27.1; verify source oracles under
those versions as well as the PD module's own dependency pins.

## Progress


- [x] Refresh branches and inspect complete transport/error sources and existing callers.
- [x] Execute regression proving the current open breaker loses the PD code.
- [x] Implement every error definition and helper; migrate existing native producers.
- [x] Run independent Go oracles, native package/library/static checks and inventories.
- [ ] Publish native, sync TiDB, validate adapters/lint, actual hook and fresh locked server build; push TiDB.

## Plan of work and milestones


First reproduce the open-breaker diagnostic against unchanged production in
src/pd/circuitbreaker_tests.rs. It must return the full source code and message.
The source package has no original tests, so add an independent Go oracle that
executes all definitions, wrapped causes, classifiers and ZapError, including nil,
identity-versus-code distinctions and resource-group cause precedence.

Implement src/pd/errs.rs as the one definition/classification owner. Definitions
retain source names, RFC codes and message templates. Native constructors keep
messages and error causes separately; use typed formatting for the source's
formatted definitions instead of implementing a second general-purpose printf.
Use native std::error::Error chains and borrowed/owned structured logging fields.
Preserve exact direct singleton recognition in IsLeaderChange: generated or
wrapped stream-closed errors must not automatically count as the direct sentinel.
IsNetworkError is only Unavailable and DeadlineExceeded; ResourceExhausted is
not a network error. The resource-group wrapper gives explicit cause text priority
while preserving its underlying error regardless of which message wins.

Migrate the existing circuit-breaker producers and tests to the shared error,
remove their old enum category. Migrate equivalent native TSO stream-EOF and count
mismatch producers to source errors without claiming complete TSO lifecycle or
changing completion/cancellation policy. Inventory every upstream caller; future
transport/security/resource/service owners must use these definitions. Do not
pretend the external errors or zap packages themselves have been transcreated.

## Validation and acceptance


From native root, expect the red test to fail before production edits, then pass:

    cargo test --locked --lib source_open_breaker_error_retains_pd_code -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check

From an isolated copy of PD's module/source, run the committed oracle test with
output directory PD_ERRS_ORACLE and go test -mod=readonly -race ./errs -count=1.
The source errs package has no failpoints. Inventory and hash every source and
support/build artifact; compare generated oracle output byte-for-byte. Keep all
oracle .go.txt inputs gofmt -s formatted for TiDB's actual commit hook.

After native publication to master, from TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-pd-client --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: use the shared Go PD error owner'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No actual Go/import/module/Bazel change requires bazel_prepare. Do not bypass
hooks or substitute a previous build. Do not claim live-cluster/Linux or benchmark
results. Preserve incoming commits and revalidate any changed integration scope.
Scratch source/oracle runs are repeatable; never alter original module-cache files.
If a gate fails, retain source and diagnostic evidence, repair and rerun that gate.

## Surprises & Discoveries


The prior circuit-breaker implementation retained a Rust-only uncoded error enum.
The transport package additionally requires URL/dial error codes and cause chains.
Implementing only those two errors would duplicate a source package boundary.

## Decision Log


Decision (2026-10-01): complete errs before grpcutil. This is an independent whole
package prerequisite, and its existing circuit-breaker/TSO callers can be migrated
without introducing a partial transport owner. Whole grpcutil requires additional
connection-lifecycle and option work; it remains open.

## Outcomes & Retrospective


All 36 definitions, six constants, six source functions/methods and 32 source
consumer files are inventoried. The original source has no tests; independent
all-definition/helper oracles pass with race under PD-own and TiDB-master error/
zap dependencies, producing byte-identical fixtures. Three runtime regressions
fail under the old producers and pass after migration. Native full library passes
1,523 cases with two existing ignored; final 147 PD cases, strict Clippy, all-target
compilation, formatting and source/oracle checks pass. Native and TiDB publication
follow the recorded gates. Full grpcutil/root/discovery/TSO and
routing acceptance remain open; no existing top-level structural finding closes
merely because this shared dependency is implemented.


Revision note (2026-10-01): final API makes prototype fields private to prevent
forged singleton identity; constructors and read-only metadata access remain public.
Native PD and resource-group envelopes retain typed owners and causes through
standard error chains. TSO EOF and count regressions additionally verify producer
integration without changing pending-request completion policy.
