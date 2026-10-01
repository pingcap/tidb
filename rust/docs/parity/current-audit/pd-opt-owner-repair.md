# Complete PD options owner and existing caller migration

## Source and acceptance boundary


Fresh TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb` still selects PD
client `v0.0.0-20260805103528-afa43111d149`. Both implementation branches were
refreshed and clean: native baseline `2fd0ecebadf0e8274a2b10ebfed729b03284a52b`,
TiDB baseline `41330e49321c82f4a948dc7f308908b2d7222d16`.

The atomic source unit is all of `opt`: `option.go` and `option_test.go`, all
38 production functions, eleven named types, source defaults/discriminants,
original `TestDynamicOptionChange`, `TestOptions`, `TestMain` and both test
helpers. No doc.go, generated source/input, package build file, platform source
or fixture exists. The [native inventory](../../../third_party/tikv-client-rs/doc/pd-opt-package.json)
hashes both artifacts, all five shared test-support files including Linux/non-Linux
variants, four module/build/license inputs and 22 source caller files. The
[native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-opt-owner.md) records
implementation, native adapters and validation steps.

Acceptance is the complete options data/notification/constructor package. It
does not accept PD root, service discovery, TSO, grpcutil, metrics initialization
or transport behavior. Full configurable construction and actual consumption of
follower/router/proxy/concurrency settings remain obligations of those owners.
The options package itself stores policy and sends notifications, as Go does.

## Changes and native representations


`src/pd/opt.rs` owns static timeout/retry/forwarding/proxy/metrics/backoffer/dial
settings; all five typed atomic dynamic values; three capacity-one notification
channels; every client, store, scatter/split, region and metadata constructor.
Boolean setters notify only on a successful change; full notification slots
coalesce updates. Interval and concurrency setters make exactly one CAS, with no
retry loop or additional validation. Interval limits are inclusive 0..10ms with
nanosecond precision. Source permits negative concurrency/retry values at this
storage layer, represented as native signed machine-width integers.

Static options are applied before sharing the owner through Arc. Native
Duration cannot express negative Go durations. The Tokio receiver mutex adapts
its single-receiver interface and releases correctly when a receive is canceled;
no option worker is spawned. Caller-closed channels panic only on a real change,
matching Go's send-after-close behavior. Go's original follower-handle test has a
stale no-notification comment; production does notify, and native tests cover it.

Reusable option closures preserve order. Dial options append opaque native
Endpoint callbacks; interpreting them belongs to the pending transport owner.
Optional shared label maps and the existing retry policy preserve reference
identity. Range-end bytes use a fixed Arc slice of AtomicU8: shared byte updates
are visible, byte storage cannot change length through another alias, and None
preserves nil versus an empty slice. This is a safe Rust representation, not a
new metadata operation. Metadata revision/lease/limit remain signed i64.

The old two-field `RegionScanOptions` struct is removed. Its exported name is
only an alias to the complete `GetRegionOp`. All native cache/codec/cluster and
TiDB `tikv_pd_bridge` callers migrate to the source field
`output_must_contain_all_key_range`; struct literals include `..Default::default()`.
This is a source-level API change for external Rust literal users. Production
initialization obtains its default attempt count from Options and retains its
existing explicitly supplied timeout. Public `pd_options` exposes the package;
`pd_backoff` exposes the already-implemented policy accepted by WithBackoffer.
There are no new dependencies, generated changes or lockfile changes.

Changed native files are `src/pd/{opt,opt_tests,mod,retry,cluster,codec}.rs`,
`src/region_cache.rs`, `src/lib.rs` and the two native documents. TiDB receives
those through its maintained sync script, plus one bridge field migration,
the sync log and current plan/audit evidence.

## Evidence and validation


The original-case native tests first fail to compile on unchanged production:
`crate::pd::opt` does not exist. `/private/tmp/pd-opt-red.log` records that missing
package API; it is not presented as a runtime defect reproduction. The complete
owner makes those tests executable. Additional tests cover all constructors,
shared identity, repeated application, bounded/coalesced notifications, invalid
interval retention, concurrent updates, receiver cancellation and caller closure.

From native client-rust:

    cargo test --locked --lib pd::opt -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check
    python3 /private/tmp/pd-opt-work/check.py

Original Go source runs unchanged in `/private/tmp/tidb-pd-opt-go-source`:

    go test -mod=readonly -race ./opt -count=1

The original suite passes with race detection and goleak (1.632s). Direct opt
source/test checks find no failpoint use, so instrumentation is not required.
Every isolated source/module/support byte matches the pinned module afterward.
No TiDB Go/import/module/Bazel/test-target edits trigger bazel_prepare.

After native publication, from TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    python3 /private/tmp/pd-opt-work/check.py --synced
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared PD options'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

All **103 focused PD tests** (including eight option cases) and **1,478 native
library tests** pass, with two pre-existing ignored cases. Strict Clippy,
all-target compilation, formatting, diff checks, all 38 production-function
mappings and original-source/hash checks pass. Native master publishes
`df0d4ccc5b595959f496b3cf6e6b87f22b337bf4`.

TiDB synchronization fetches exactly df0d4cc and applies all four maintained
patches. Protocol regeneration makes no generated or lockfile changes. All
**23 driver tests** and **26 PD tests** pass, with one pre-existing live-PD
ignore. Transaction-crate all-target compilation, root lint, exact source identity,
receipt links and the unchanged 85/77/8 register pass. The actual pre-commit hook
must build the locked server; a fresh locked build after that commit gates the
normal push. The publication response records both results and the final TiDB
revision. Logs are `/private/tmp/pd-opt-*.log`.

Disk maintenance reclaimed **6.05 GiB** of file contents from 34 ignored
incremental cache directories whose entire contents were older than 24 hours.
Process inspection confirmed no cargo/rustc was active before removal. Sources,
final binaries, test results and recent caches were preserved. Manifest:
`/private/tmp/pd-opt-cache-removed.json`.

## Limits and remaining work


The known register remains 85 tracked / 77 unresolved / eight repaired; P06
remains partial and P03/P07 remain open. Completed dependency data ownership does
not close discovery, public PD shutdown, TSO dispatcher or metadata concurrency.
No Linux, live PD/service topology or sysbench/TPC-C/TPC-H/YCSB benchmark was run,
and no performance improvement is claimed. The two inherited configured-TopN
tie-order failures recorded in the preceding retry receipt are outside this
package; no assertion or SQL implementation is changed here.
