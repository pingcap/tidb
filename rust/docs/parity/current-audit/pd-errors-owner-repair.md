# Shared PD error ownership

## Source and acceptance boundary


The complete pinned `github.com/tikv/pd/client/errs` package contains errno.go and
errs.go: 36 error definitions (31 normalized and five internal), six protocol
strings, four helpers, one resource-group wrapper and both of its methods. It
has no doc.go, original test/support files, package-local build files, fixtures,
generated inputs or platform variants. The synchronized
[package inventory](../../../third_party/tikv-client-rs/doc/pd-errors-package.json)
hashes both production files, module/build/license inputs and all 32 source
consumer files. The [ExecPlan](../../../third_party/tikv-client-rs/doc/pd-errors-owner.md)
records implementation, source behavior and repeatable validation.

Both branches were pulled and already current. Go master remains
`93a01d31f6da205ae4bf376825293903a6899fdb`; PD is
`v0.0.0-20260805103528-afa43111d149`. Native baseline is
`69877b9cc651c07d0643c968c194bd8ced88daba`, TiDB baseline is
`9bd91c282c7c7766c141229b361af9932802f73c`.

The grpcutil review requires this shared dependency before introducing its
URL/dial wrappers, per-RPC error classification and interception. This receipt
accepts the error package only. It does not introduce a partial grpcutil owner or
claim complete root/discovery/TSO, routing, error-library or logger implementation.

## Implementation and removals


Native src/pd/errs.rs owns every definition, code, message template and helper.
Definitions are immutable private-field handles to the source prototypes; consumers
do not reassign the source globals. Error instances retain independent diagnostic
text and an Arc to their concrete cause. Native Error::Pd and PdResourceGroup
transport envelopes expose the typed owner through std::error::Error::source,
which retains further gRPC/context causes. No code is selected by parsing a message.

Typed native constructors cover all 12 formatted definitions. They use Rust
Display for source %v values, u32 for source callers' keyspace IDs, and source
fixed float precision including NaN, infinities and signed zero. Runtime stack
frames are Rust backtraces; this is an imported-library adapter, not a second
Go printf implementation or a claim to identical Go stack text.

Leader-change classification preserves Go's direct singleton check: the bare
TSO-stream-closed prototype qualifies, but the generated, wrapped, stacked and
same-text versions do not. The four source substrings still classify independently.
Network errors are only Unavailable and DeadlineExceeded. Callee mismatch remains
case-sensitive. The resource-group wrapper prefers explicit cause text, then the
underlying error message, then unknown error; its underlying error remains available
in every case.

ZapError returns a structured error field or no field for nil. It preserves
foreign/generated error identity, wraps only a direct normalized error, uses only
the first supplied cause and distinguishes absent causes from an explicitly nil
cause. Basic field values match the executed Go encoder; native logger/stack
representation remains the imported logging adapter's responsibility.

Remove Error::CircuitBreakerOpen and replace all three native breaker rejection
sites with the shared source prototype. Existing breaker and region-cache tests
use that shared identity. Native TSO EOF and count-mismatch producers now use the
source stream-closed and length errors with stack ownership. No TSO scheduling,
pending-result delivery, cancellation, connection policy or retry loop changes.
Those parent ownership gaps remain open.

Files changed in native are src/pd/{errs,errs_tests,mod,circuitbreaker,
circuitbreaker_tests,timestamp,timestamp_tests}.rs, src/common/errors.rs,
src/region_cache.rs, source inventory/oracle/plan artifacts and the historical
circuit-breaker dependency receipt. TiDB uses the maintained sync script and
updates its audit receipts. No dependency or lockfile changes are required.

## Validation


The open-breaker regression fails before implementation: native returns
`circuit breaker is open`, while Go returns the PD-coded diagnostic. Both new TSO
regressions also fail when their original producer bodies are temporarily restored;
a finally block restores the fixed source afterward. Logs are
/private/tmp/pd-errs-red.log and /private/tmp/pd-errs-tso-red.log. All three pass
with the shared owner.

Upstream errs has no original tests. The added independent Go oracle executes all
36 definitions and every helper/method, and checks registry completeness against
the Go AST. It covers codes/templates, cause wrapping, all 17 gRPC codes, direct
identity versus wrapped/generated errors, nil/first-cause logging, every formatted
template and resource-group cause precedence. It passes under both PD's own
module pins and TiDB master's selected pingcap/errors 306e305bcf41 and zap 1.27.1,
with the race detector. Generated JSON is byte-identical between the two runs.
Original package source bytes remain unchanged; errs has no failpoints.

Native full-library validation passes 1,523 cases with two existing ignored.
Final API encapsulation passes all 147 focused PD cases, strict Clippy, all-target
compilation and formatting. TiDB passes 28 PD cases (one existing ignored), all 23
transaction-driver cases, affected all-target compilation and root make lint. These
adapter suites cover native API compatibility and existing error classification/
cancellation/TSO integration. Existing unrelated compiler warnings remain. This is correctness evidence, not a workload benchmark.

Exact native-root commands:

    cargo test --locked --lib source_open_breaker_error_retains_pd_code -- --test-threads=1
    cargo test --locked --lib source_pd_error_owner_reports_ -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Copy the pinned source go.mod/go.sum and both errs files into an isolated scratch
module. Copy doc/pd-errors-oracle/oracle_test.go.txt as errs/native_oracle_test.go.
Set PD_ERRS_ORACLE to an existing output directory, then run:

    PD_ERRS_ORACLE=/private/tmp/pd-errs-work go test -mod=readonly -race ./errs -count=1

Save the output from the PD-owned dependency pins. In that scratch module only,
select the dependencies used by TiDB master, resolve sums, then rerun locked:

    go mod edit -require=github.com/pingcap/errors@v0.11.5-0.20260508054701-306e305bcf41 -require=go.uber.org/zap@v1.27.1
    PD_ERRS_ORACLE=/private/tmp/pd-errs-work go test -mod=mod -race ./errs -count=1
    PD_ERRS_ORACLE=/private/tmp/pd-errs-work go test -mod=readonly -race ./errs -count=1

Compare both errors.json results byte-for-byte with the committed output. It is
generated by Go and must not be hand-edited. Oracle .go.txt inputs are gofmt -s
formatted for the actual commit hook.

TiDB-root integration/publication commands:

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

No Go/import/module/Bazel changes trigger bazel_prepare. Both mandatory locked
server builds must pass; the actual hook must run and a fresh build must follow
the final commit before push. Optional goword is absent; its spelling check is
not claimed.

## Publication, compatibility and remaining work


Native master publishes `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`. The maintained
sync script applies all four patches and regenerates protocol outputs; all 15
changed native files are byte-identical to that revision, with no generated-output
or lockfile drift. TiDB publication is conditioned on the actual commit hook and
a fresh post-commit locked server build; their results and the TiDB commit are
reported with publication.

The source/API compatibility
change removes the private Error::CircuitBreakerOpen variant in favor of the
shared Error::Pd owner and includes the source PD code in Display. All existing
native/TiDB callers must compile against this change. Failure diagnostics change
for the two TSO producers; success paths and resource ownership remain unchanged.

The full grpcutil package still needs ordered per-RPC interception, source dial
options/backoff/TLS, nonblocking setup, connection-cache lifetime, address/metadata
helpers and shutdown. Native eager dialing and separate PD/keyspace channels,
logical breaker placement and pending TSO failure delivery remain parent work.
The 85-tracked / 77-unresolved / eight-repaired register remains unchanged.
The previously recorded configured TopN stable-tie assertion is unrelated and
unchanged. Linux, native ThreadSanitizer, live PD topology and sysbench/TPC-C/TPC-H/
YCSB benchmarks were not run; no workload improvement is claimed.
