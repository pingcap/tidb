# Share the client-go replica candidate owner

This living ExecPlan follows PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose

TiDB and client-rust must use the same client-go candidate filtering, scoring and tie selection. A follower that returned DataIsNotReady may receive one replica-read retry, including idle-replica selection. Region epochs, metadata loading and request lifetimes remain with their existing owners. This is maintenance of existing ports, not acceptance of the complete internal/locate package.

## Progress

- [x] Refresh both repositories and inspect all candidate-policy consumers.
- [x] Reproduce native idle-selection rejection after DataIsNotReady (expected follower 12, received None).
- [x] Extend the existing native candidate snapshot with busy/load facts; share its filtering, scoring and random selection across native paths.
- [x] Publish native 568a68d9 and public-facade follow-up 5777c01c, synchronize TiDB, and remove TiDB's parallel score/filter/tie loop and per-query selection seed.
- [x] Native library: 1,405 passed/two ignored, strict Clippy and formatting passed. TiDB affected suites: 884 passed/14 ignored; root lint passed.
- [x] Commit through the locked-server-build hook and pass the separate post-commit locked server build. Final publication must repeat the latter after this receipt amendment, then push hparser-integration.

## Source and context

Baselines are TiDB 2ba4ea91e6 and client-rust b23c6d37. Fresh Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go v2.0.8-0.20260928031501-8edb23f6c7ee. Its internal/locate package has no doc.go. Source replica_selector.go: ReplicaSelectMixedStrategy.next, isCandidate and calculateScore jointly own attempt eligibility, busy filtering, five-bit scoring and random ties. TiDB currently repeats these in region/health_policy.rs and region/cache/replica_routing.rs. Native locate.rs already owns the ordinary mixed policy, but region_cache.rs::select_idle_replica independently filters attempts to zero.

## Surprises & Discoveries

Go's isCandidate permits two attempts for a nonleader flagged DataIsNotReady regardless of a positive busy threshold. Native idle selection filters all previously attempted candidates before the shared policy can apply that rule. TiDB additionally chooses tied candidates with one query seed, whereas Go calls randIntn at selection time. Tests that depend on a fixed tied peer must assert the eligible set or constrain their setup to one candidate.

## Decision Log

Expose the existing native candidate snapshot/policy through the public tikv facade for embedding (region_cache is a private module). Add busy flag and decayed load facts to that snapshot so the native idle path also uses the same predicate. Split predicate and score like Go, allowing existing TiDB score-contract tests to call the owner. Keep TiDB's store/label matching adapter; remove its independent scoring branches, candidate attempt policy and tie loop. Remove the unused DistSQL query seed lifecycle when no selector consumes it. Do not move the duplicate algorithm into another TiDB module or remove the still-required region cache/health state in this milestone.

## Validation and implementation

Extend the existing native replica-candidate integration fixture with one attempted DataIsNotReady follower; it must fail before production changes. Cover exhausted, busy and overloaded controls after repair. Run native targeted selector tests, full library tests, Clippy and formatting from /Users/qiliu/projects/client-rust. Commit and push to master, then run bash rust/scripts/sync-tikv-client-rs.sh from TiDB root. Use the existing source regeneration/compatibility patches.

From TiDB rust/, run affected tidb-txnkv replica health, request selector, region recovery and full transaction integration tests; run tidb-distsql candidate/dispatch tests and compare any broader failure against unchanged HEAD. Test score precedence, labels, store preference, learner/mixed/prefer-leader, busy thresholds, attempt limits, DataIsNotReady, forwarding and no-idle fallback. Run root make lint. No Go/Bazel inputs change, so bazel_prepare is unnecessary. Commit through TERM=xterm git -c core.hooksPath=hooks commit, requiring cargo build --locked -p tidb-server in the hook. Rerun cargo build --locked -p tidb-server from rust/ immediately before pushing hparser-integration.

## Outcomes & Retrospective

The shared candidate policy repair passes all affected tests. Stale-read fixtures use Mixed mode (the only supported stale mode) and store preference to establish their intended first peer. DistSQL assertions retain exact request flags while accepting random ties. A label-requested case deterministically covers leader DataIsNotReady; the known-leader recovery case accepts either initial tied peer. No production selection seed remains.

The actual commit hook and separate post-commit locked server build both passed. The final receipt amendment uses the same hook and is followed by another locked build before push. The detailed receipt is [replica-selection-owner-repair.md](parity/current-audit/replica-selection-owner-repair.md). Full cache/sender migration (T02), table assertion ownership (T01), complete package acceptance and benchmarks remain open. No throughput improvement or full parity is claimed.
