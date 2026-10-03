# Repair the shared cache dependency and its production consumers

This is a living ExecPlan maintained according to root `PLANS.md`.

## Purpose / Big Picture


Close the existing B01, B02, C03 and C04 findings together: hot bindings and
coprocessor results must participate in Go's frequency admission, binding
refreshes must preserve that history, disabled coprocessor caches must work,
and statistics must retain only fallback metadata after oversized admission.
The preceding account batch closed only one ID; this batch's completion unit
is multiple existing findings, not a count of related edits.

## Context and Orientation


The editable checkout is `/workspace/tidb`, branch `hparser-integration`,
starting at `b38a25eb0e361739460d81263a17bc75455a3974`. Freshly fetched Go
master remains `93a01d31f6da205ae4bf376825293903a6899fdb`, selecting
`github.com/dgraph-io/ristretto v0.1.1`. The native transaction client in
`/workspace/client-rust` is unchanged at `19a56ccda1e128218cd33c69709038219aced9bc`.
Source comparisons use `/workspace/.cloud-setup/go-master` and the verified Go
module cache. Source/build work stays in cloud. The setup skill has already
prepared tools; activate `/workspace/.cloud-setup/env.sh` for commands.

Read `rust/docs/parity/current-audit/shared-cache-owner-review.md` and
`ristretto-v0.1.1-inventory.json`. The pinned root package has six production
files, six test files, 73 tests and five benchmarks. Its entire acceptance
unit precedes integration. Separate `z`, simulation and contribution packages
retain explicit integration decisions rather than implied acceptance.
Ristretto buffers new writes until admission, immediately replaces residents,
counts lossy batches of reads, and compares their estimated frequency against
sampled eviction candidates. It is shared code, not a shared global budget.

## Progress


- [x] Read the current findings and shared owner design; fetch integration and
  Go master without disturbing the five local unpublished commits.
- [x] Capture failing regressions for the existing consumers and inventory all
  source tests, runtime helpers and platform decisions.
- [x] Implement and validate the complete pinned root cache owner: 73 translated
  source cases, three additional regressions, and five benchmark smoke cases
  pass; all 91 artifact hashes match. Original Go root race tests and both
  languages' five benchmark commands pass. No speedup claim is made.
- [x] Migrate LFU, binding refresh/GC/usage, and configured coprocessor owners;
  retire Stretto and private FIFO stores after callers migrate.
- [x] Restore original meaningful probabilistic assertions, remove stale FIFO
  expectations, and validate all four findings through production callers.
- [x] Update both registers, receipts and full ExecPlan; run Ready checks,
  actual precommit hook and a fresh locked server build immediately before push.
- [x] Record publication denial and the recovery/configuration handoff; final bundle and draft metadata are retained in the cloud publication receipt.

## Milestones and Plan of Work


First add `rust/crates/tidb-ristretto` with native ownership for the pinned
root package. Keep sharded value storage, one bounded write channel, an
independent lossy frequency channel, TTL buckets, policy metrics and joined
workers. Callbacks must run outside policy locks and support statistics
callbacks queuing trigger entries. Translate every original root test and
benchmark, recording explicit native decisions for Go runtime/hash helpers.
Do not enable the cache in consumers until its complete source suite passes.

Then migrate `tidb-stats-handle-cache-internal-lfu`, retaining its sharded
fallback metadata and public close guard. Both currently ignored C04
regressions must pass without weakening their assertions. Restore policy
metric assertions. Migrate `tidb-session/src/binding_cache.rs` and
`tidb-server/src/cluster_binding_seam.rs` as one live binding owner, including
incremental watermark, owner GC and usage persistence. Migrate
`tidb-distsql/src/copr_cache.rs` and `tidb-exec/src/real_tikv_read.rs` with
effective TiKV configuration, optional disabled cache and shutdown ownership.
Review complete upstream consumer inventories and existing translations;
do not claim complete parent packages if unrelated seeds remain.

Finally run relevant original and translated suites, production SQL paths,
all-target checks and root lint. Recheck the diff and update evidence before
committing. No performance improvement or multi-node parity is claimed from
unit tests. Inference remains its separately tracked complete-package task.

## Validation and Acceptance


Logs belong in `/workspace/.cloud-setup/cache-batch`. Run commands from
`/workspace/tidb` after sourcing the cloud environment. Use targeted
`cargo test --locked -p <owner>` commands from `rust`, original pinned
`go test -race .` for the root dependency, and original consumer cases where
their prerequisites permit. The baseline C04 admission/pressure probes must
fail, then pass after migration. Hot-key tests must show frequency protection
without demanding a particular probabilistic victim. Binding reload tests
must retain owner identity and exercise incremental deletes, GC and usage.
Coprocessor constructors must accept disabled and enabled effective settings.

Before publication run `git diff --check`, appropriate all-target cargo
checks, `make lint`, and the actual tracked precommit hook. The hook and a
separate build immediately before each push must both execute
`cd rust && cargo build --locked -p tidb-server`. Verify remote SHA after push.
Push only to `pingcap/tidb` `hparser-integration`, never force. GitHub currently
denies this account; retain validated local commits and report denial if it
persists. Do not substitute a fork.

## Idempotence and Recovery


Preserve concurrent work; fetch before publishing and merge only after review.
Do not delete source or dependency caches to reclaim disk; disposable Cargo
incremental output can be removed when no Cargo process is running. Refresh
the verified recovery bundle only after completed commits. Saving the cloud
configuration draft neither publishes it nor establishes fresh-task restore.

## Surprises & Discoveries


All four candidate findings still describe live source. A new Ristretto
instance on each binding reload would leave B01 behavior unresolved; B02 is
therefore part of this batch. Existing Stretto differs in publication and
dropped-write callbacks, so a wrapper-only policy fix cannot close C04.

## Decision Log


2026-10-03: Repair B01, B02, C03 and C04 as one dependency/consumer batch.
Follow the already reviewed shared-cache design, retain independent budgets,
and do not include the absent inference runtime or distinct instance plan
cache in this repair. Remove tests only when they assert unsupported behavior;
keep regressions that expose real dependency gaps.

## Outcomes & Retrospective


Implementation and consumer validation are in progress. No finding is closed
until the final gates pass. Both old C04 probes failed (eager visibility and a
136-byte retained payload); both pass with the new owner. Binding/coprocessor
hot-key probes failed on FIFO, and the binding probe now passes. The first LFU
consumer run exposed two stale native tests: an obsolete negative-ID panic
expectation was removed; callback recovery now injects a real failure.
An additional red regression demonstrated lost acknowledgements after a later
usage batch fails; the shared usage writer now acknowledges each committed batch.

Cargo incremental data exhausted the 32 GiB volume twice. After all Cargo
processes exited, only incremental output was deleted. The tested cloud env
and installer now export CARGO_INCREMENTAL=0. Existing logs and dependencies
remain. Package-wide rustfmt touched 17 unrelated files; those formatter-only
edits were restored immediately. Use rustfmt with skip_children on changed
files for the remaining work.

## Artifacts and Interfaces


The shared cache exposes configured construction, hashed Get/Set/TTL/Del,
Wait, Clear, Close, capacity updates and optional policy metrics. Values are
cloned reference holders where Go shares pointers. Rust owns and joins workers
without retaining a strong cycle through consumer callbacks. Root receipts
must map all 91 module artifacts, including non-root package decisions.


2026-10-03 final validation update: all selected consumer suites pass, including
both C04 probes, 54 session and 8 server binding cases, configured coprocessor
construction and the incoming MDL regression. Root/LFU Go race suites pass 83
original tests. The seven affected crates pass all-target checking and make lint
passes. Remote integration advanced to 7b991676da; its effective-MDL-default
change was reviewed and merged without conflict before affected checks reran.
Both finding registers now close B01/B02/C03/C04: 61 unresolved, 25 repaired.
The final server link exhausted the disk despite incremental compilation being
disabled. After Cargo stopped, seven obsolete completed test executables were
removed; no compiled library dependencies, sources or logs were removed. Retry
uses two Cargo jobs. Actual hook, fresh pre-push build, wire smoke and publication
receipt are the remaining gates.


The live wire startup caught a duplicate metric-registration panic: the new
binding updater and pre-existing server dashboard declared the same three gauges.
The dashboard now re-exports the session-owned gauges. A retained metric-owner
regression covers initialization plus cache refresh; it raises the distinct Rust
case count to 475. Repeat the server build and wire smoke after this correction.


Final runtime gates now pass: shared metric-owner regression, all nine server
binding tests, repeated server all-target check, locked server build and both
real MySQL wire checks (baseline SQL plus CREATE/match/DROP global binding).
Both registries agree on 61 unresolved and 25 repaired. Actual commit-hook and
fresh pre-push builds remain publication gates, recorded after execution.


Implementation merge 2336faceb4 passed the actual locked-build hook. A fresh
locked server build immediately before push also passed. GitHub denied the
exact requested destination with HTTP 403; remote remains 7b991676da. This final
receipt commit uses the hook and remains local without another unchanged denied
push. The recovery bundle and cloud draft retain exact local heads; consult
/workspace/.cloud-setup/cache-batch/publication-final.json for their final IDs
and hashes. Fresh-task restore and GitHub publication remain separately blocked
or unverified; implementation and current-instance validation are complete.
