# Retire empty transport and expression test shells

This living ExecPlan follows root PLANS.md. Maintain Progress, discoveries, decisions and outcomes.

## Purpose and context

Remove non-executable test claims while preserving the missing Go obligations. The user requests continued removal following Go, with grouped validation and no push. Starting integration HEAD is 1e91cdb62d920b116a9edb1a424745a1fcba5f07; Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. Remaining routing code has live callers and cannot be deleted safely in this cleanup. No native sources, production algorithm, manifest or lockfile changes are intended.

## Progress

- [x] Inspect routing owners and identify literal empty ignored tests in tidb-txnkv, tidb-distsql, tidb-expr and tidb-chunk.
- [x] Preserve all 80 contracts, attributes, source lines and complete original files in current-audit/transport-expression-empty-test-obligations.json.
- [x] Remove the 80 shells from 21 files; retain every executable test.
- [x] Retire five pure placeholder modules and their four explicit module declarations.
- [x] Reconcile candidate index; prove all retained tests unchanged.
- [x] Group affected tests, checks, formatting and lint; inspect passing results and four original-source failures (one loopback case passes with permission).
- [ ] Actual normal-commit locked server build gate.
- [ ] Normal local commit through actual locked-build hook; preserve recovery. No push.

## Surprises & Discoveries

The shared native candidate scorer and health types are already used. T02 still has competing cache/request-transition owners with live ordinary/coprocessor consumers. Removing these without migration would break users. This batch therefore retires test shells only. Historical claims that a contract is unported are retained as historical evidence, not a fresh behavioral audit. Go's runtime-statistics and select-iterator cases exist in pkg/distsql; moving empty Rust functions to a ledger does not implement them. Nonempty ignored live-cluster and performance tests are retained.

## Decision Log

Decision: remove only #[test] functions with an ignore attribute and a literally empty body. Preserve original full file contents and contract comments in a durable ledger so no upstream obligation is lost. Date/author: 2026-10-04 / Codex.

## Work and validation

Remove pure modules only after confirming they contain no items. Remove matching declarations in tidb-expr/src/tests/mod.rs; DistSQL aggregate discovers remaining files automatically. Relocate candidate-gaps.tsv rows whose original line belongs to removed code to the ledger. Verify retained test functions byte-for-byte against the preimage.

From rust/, source /workspace/.cloud-setup/env.sh and run cargo test --locked -p tidb-expr -p tidb-chunk -p tidb-distsql --lib, then cargo test --locked -p tidb-distsql --test all. Check affected targets with cargo check --locked -p tidb-expr -p tidb-chunk -p tidb-distsql --all-targets. Format changed Rust files with rustfmt --check --edition 2021 --config skip_children=true. From root run make lint and git diff --check. Run cargo build --locked -p tidb-server from rust/ and commit normally with TERM=xterm; actual hooks/pre-commit must enforce the same locked build. Capture logs and underlying exit status under /workspace/.cloud-setup/empty-test-batch. A test failure remains a failure, not a cleanup exemption.

## Idempotence and Recovery

Commands are safe to rerun except the removal script, which is a one-time migration. Original source is recoverable from the ledger or starting commit; never reset concurrent work. Preserve the verified unpublished bundle until its replacement verifies. No push or dry run is authorized. Counts remain 86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial); test cleanup closes no structural finding and certifies no whole Go package. Validation outcomes will be recorded in current-audit/transport-expression-empty-test-removal.md.

## Outcomes & Retrospective

All 80 literal empty shells are removed and five pure-placeholder modules retired. Original obligations and 102 historical candidate rows remain in the ledger; every retained executable test is unchanged. The grouped library run exposes three failures also reproduced against original sources. The local HTTP fixture is sandbox-blocked and passes under approved execution. Final checks and hook outcomes belong in the receipt. No structural finding status changed.
