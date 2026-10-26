# Extract the MV inspection and comparison parser prerequisite


This completed ExecPlan follows `PLANS.md` and records the parser extraction, independent source audit, local environment changes, and verification evidence.

## Purpose / Big Picture


Provide the parser definitions required by the later materialized view (MV) SHOW and COMPARE feature PRs. SQL text can be parsed, restored, classified, and traversed independently of their planner/executor implementations. The feature split and combined implementation are preserved on `mv_pr6_misc_for_master`; this branch extracts its parser portion and checks it against the latest `cp_mv_for_master` source.

## Progress


- [x] Fetch `origin/master` and `xufei/cp_mv_for_master`, confirm a clean workspace, and pin the references below.
- [x] Create `mv_pr6a_misc_parser_for_master` from current `origin/master` in the existing directory.
- [x] Extract the combined reference's changes under `pkg/parser` without carrying SHOW/COMPARE runtime implementations.
- [x] Audit original-source MV grammar, keyword additions, AST fields, and SHOW/COMPARE implementation; replay 98 original-source SQL cases with no differences.
- [x] Regenerate parser, keyword, AST visitor, and required Bazel artifacts; parser files still match the combined reference exactly.
- [x] Pass the complete parser race suite with failpoints enabled and restore instrumentation to zero afterward.
- [x] Complete server compilation, lint, persistent Bazel cache verification, and final diff/source checks.
- [x] Commit the extraction and rebase it onto the latest fetched `origin/master` at `e2f259adf7ddb775cdc02e1514712b534498d05f`, preserving upstream hint changes.
- [x] Repeat Bazel preparation, generation checks, complete parser race tests, source replay, server compilation, and lint on the rebased branch before publication.

## Surprises & Discoveries


Bazel started with a cold repository cache and its configured SDK mirror downloaded Go slowly. A bounded official download populated a temporary `--distdir`; a task-local Bazel rc disables the remote action cache for every preparation step. Repository build configuration remains unchanged.

At the user's request, Bazel data was subsequently migrated from temporary directories to `/Users/feixu/.cache/bazel`. The user rc now sets `startup --output_user_root=/Users/feixu/.cache/bazel/output` and both disk/repository caches to `/Users/feixu/.cache/bazel/cache`. Existing cache data was preserved, the old temporary locations and temporary SDK download directory were removed, and `bazel info output_base` confirms the persistent location. These are local environment changes outside the PR.

Moving the output tree left absolute cached symlinks and Gazelle's generated `go.env` pointing to the old location. Relocate those symlinks and regenerate the repository environment with `bazel build @bazel_gazelle_go_repository_cache//:go.env`, preserving/restoring its Go build and module cache directories. A complete `make bazel_prepare` then passed using the persistent output/cache locations. No repository source or build configuration was changed for this repair.

Master advanced from the combined port's base `63ec01b2440281b0debbfb05c5dfd378e94474d1` to `ff54393a0cbb21f263456d29f910805f8b7a8f9d`. No files under `pkg/parser` or the applicable agent/testing instructions changed between these bases, so the extracted parser changes initially match the combined reference exactly.

Before publication, master advanced to `e2f259adf7ddb775cdc02e1514712b534498d05f` with TiFlash LM-filter hint support. Rebase applied cleanly, preserving upstream changes in `ast/misc.go`, `ast/misc_test.go`, `misc.go`, and the hint parser files. `git range-diff` reports the port commit unchanged across rebase; the parser-only stable patch ID is `fe8e26dea03488119f45805038be245b17ec057b` both before and after rebase. The current parser tree consequently includes those upstream additions beyond the combined reference.

`pkg/parser/ast/misc.go` uses `failpoint.Inject`, and its Bazel target depends on failpoint. Parser tests therefore run through `tools/check/failpoint-go-test.sh`, which restores instrumentation afterward. The extraction adds top-level tests in an existing file, requiring `make bazel_prepare` before build/test.

## Decision Log


Decision: Extract all parser inputs, generated outputs, and parser tests from the combined implementation, then regenerate the outputs. Rationale: keep the agreed parser prerequisite intact and verify generated artifacts against their source inputs. Date: 2026-10-26.

Decision: Audit the original source separately from the combined reference. Rationale: equality with the reference alone cannot detect omissions inherited from the original port. Existing MV parser functionality already merged into master must be distinguished from this PR's remaining SHOW/COMPARE work. Date: 2026-10-26.

Decision: Rebase onto the latest fetched master before pushing and opening the PR, then repeat affected checks. Rationale: the user requested this ordering, and the new base changes shared parser files. Preserve upstream hint behavior and verify both generated parsers. Date: 2026-10-26.

## Outcomes & Retrospective


The 16 parser files are extracted and regenerated. Their MV changes remain identical to the combined reference after the clean rebase, while the current tree preserves the new master's hint changes. The independent original-source SQL audit, complete parser race suite, server compilation, lint, and final review passed on the initial extraction. Bazel preparation, both generated parsers, the parser race suite, the 98-case source replay, server compilation, and lint passed again after rebase. The branch is `mv_pr6a_misc_parser_for_master`. Later runtime PRs own preprocessing, permission checks, planning, virtual metadata readers, and SHOW/COMPARE execution.

## Context and Orientation


The repository is `/Users/feixu/dev/pingcap/tidb`. Current destination base: `origin/master` at `e2f259adf7ddb775cdc02e1514712b534498d05f`; the initial extraction used `ff54393a0cbb21f263456d29f910805f8b7a8f9d`. Combined reference: `mv_pr6_misc_for_master` at `e45b61539374560d0499fa673c1dba09f42502d9`. Original source: local `cp_mv_for_master` and fetched `xufei/cp_mv_for_master`, both at `fd4636bb442de52a1a5c6321f3f29092472aa6f4`. The original feature diff starts at `f08c648a20380ea723449c6c3eb5b171d96fd567`.

The parser is a separate Go module under `pkg/parser`. Grammar in `parser.y` produces `parser.go`; `generate.go` produces `keywords.go`; `ast/visitor_codegen` produces `ast/visitor_inplace_generated.go`. The AST stores statement structure; restore methods emit SQL; visitor methods let callers traverse or replace children; SEM command methods identify statements for SQL execution mode checks.

## Plan of Work


Extract the six SHOW statement types and their restoration, the COMPARE statement and its children, grammar/token/keyword support, statement labeling/classification, and parser/AST tests. Regenerate the three generated artifacts rather than editing them. Audit the original branch's feature delta, including already merged MV DDL/refresh/purge/cancel syntax, privilege tokens, internal aggregate names, and full outer joins. Record which differences are existing master adaptations or unrelated test changes.

The parser prerequisite must compile with the destination server and pass parser tests independently of SHOW and COMPARE runtime implementations. It enables syntax only; functional SQL execution remains owned by the later feature PRs.

## Original Source Audit


The original feature diff adds or changes 36 MV grammar productions. The destination contains their behavior. The source's `MViewRefreshOnClauseOpt` is consolidated into `MViewStartWithOrNextOpt` in master, with the same empty/scheduled forms and initialization of the refresh clause. Existing master accepts optional `=` for COMMENT; preserve that established behavior. Other reviewed grammar differences are formatting or equivalent action construction.

The six SHOW forms include all 24 original-source SHOW parse/restore cases. The COMPARE statement's fields, Restore method, and legacy Accept method match the original source. The reference additionally supplies current master's SEM methods, statement label, generated in-place visitor, and receiver-inventory updates.

All 12 keyword additions in the original feature diff are present after extraction. MV DDL/ALTER/DROP, refresh/purge/cancel syntax, OPERATE VIEW privilege maps, internal aggregate names (`sum_int`, `min_count`, `max_count`), full outer joins, and MV option helpers are already present in master. The source's unrelated `terror` stack-depth expectation is outside this extraction.

A temporary audit program read the original `parser_test.go` using Go's standard AST parser and replayed its relevant SQL/expected-validity/restore triples against this branch. It selected MV statements, OPERATE VIEW, full outer joins, and the three internal aggregate functions, and checked 98 unique cases with zero differences. This independently tests inherited source coverage without importing source test scaffolding into the PR. All 24 original SHOW cases are also present in this branch's permanent tests.

## Concrete Steps


Commands run from repository root unless a working directory is specified. Bazel preparation passed with the following invocation. The task-local rc disabled the remote action cache for all preparation steps, including the recipe's tazel invocation, and initially supplied the verified SDK archive through `--distdir`. The temporary SDK directory and its rc entry were removed after the dependency archive was cached persistently.

    make bazel_prepare BAZEL_GLOBAL_CONFIG='--bazelrc=/tmp/mv-pr6a-parser.bazelrc' BAZEL_CMD_CONFIG='--jobs=8 --remote_cache= --http_timeout_scaling=0.1 --remote_timeout=5s --remote_retries=0 --remote_upload_local_results=false'
    GOMAXPROCS=8 GOFLAGS=-p=8 make parser_ast_visitor parser
    GOMAXPROCS=8 GOFLAGS=-p=8 make -C pkg/parser generate
    GOMAXPROCS=8 GOFLAGS=-p=8 make parser_fmt
    GOMAXPROCS=8 GOFLAGS=-p=8 make parser_yacc

Run parser tests and server compilation after generation:

    GOMAXPROCS=4 GOFLAGS=-p=4 ./tools/check/failpoint-go-test.sh pkg/parser -race -count=1 ./...
    GOMAXPROCS=4 GOFLAGS=-p=4 go build -o /tmp/mv-pr6a-misc-parser-tidb-server ./cmd/tidb-server
    GOMAXPROCS=4 GOFLAGS=-p=4 make lint
    git diff --check

The following audit command ran from `pkg/parser`; the temporary program extracts source test triples with Go's AST parser, checks expected parse success/failure, and compares restored SQL with the original expectation:

    GOMAXPROCS=4 GOFLAGS=-p=4 go run /tmp/mv-pr6a-parser-source-case-audit.go -sourcefile /tmp/mv-pr6a-parser-source-parser-test.txt

Its output was `Source parser audit: 98 unique relevant cases, 0 differences`. Before rebase, `git diff --exit-code mv_pr6_misc_for_master -- pkg/parser` also passed after regeneration. After rebase, compare the parser patch IDs below to distinguish unchanged MV port behavior from the preserved upstream hint additions. Inspect the original source with `git show cp_mv_for_master:<path>` and `git diff f08c648a20380ea723449c6c3eb5b171d96fd567..cp_mv_for_master -- pkg/parser` to repeat the source inventory.

Rebase verification repeats Bazel preparation and the following checks. Force regeneration of both grammar outputs to verify the upstream hint parser as well as this PR's SQL parser:

    GOMAXPROCS=4 GOFLAGS=-p=4 make parser_ast_visitor parser
    GOMAXPROCS=4 GOFLAGS=-p=4 make -C pkg/parser -B parser
    GOMAXPROCS=4 GOFLAGS=-p=4 make -C pkg/parser generate
    GOMAXPROCS=4 GOFLAGS=-p=4 make parser_fmt
    GOMAXPROCS=4 GOFLAGS=-p=4 make parser_yacc
    GOMAXPROCS=4 GOFLAGS=-p=4 ./tools/check/failpoint-go-test.sh pkg/parser -race -count=1 ./...
    GOMAXPROCS=2 GOFLAGS=-p=2 go run /tmp/mv-pr6a-parser-source-case-audit.go -sourcefile /tmp/mv-pr6a-parser-source-parser-test.txt
    GOMAXPROCS=4 GOFLAGS=-p=4 go build -o /tmp/mv-pr6a-misc-parser-rebased-tidb-server ./cmd/tidb-server
    GOMAXPROCS=4 GOFLAGS=-p=4 make lint

Run the source replay from `pkg/parser`. These patch checks run from repository root; the first compares the published combined reference with its base, and both commands yield the stable patch ID recorded above:

    git diff 63ec01b2440281b0debbfb05c5dfd378e94474d1 e45b61539374560d0499fa673c1dba09f42502d9 -- pkg/parser | git patch-id --stable
    git diff origin/master HEAD -- pkg/parser | git patch-id --stable

The final persistent-cache check reran the same `make bazel_prepare` invocation after repairing the migrated cache, with `/tmp/mv-pr6a-parser.bazelrc` containing the following task-local overrides:

    build --remote_cache=
    build --remote_timeout=5s
    build --remote_retries=0
    build --remote_upload_local_results=false
    build --jobs=8

`build/get_changed_bazel_pkgs.sh origin/master` selects `//:all` plus the three owning parser packages because this note maps to the root target. The user-supplied build flow permits skipping `make bazel_lint_changed` for `//:all`; that broad sweep is skipped. Scoped parser checks, Bazel preparation, and Ready lint passed.

## Validation and Acceptance


New SHOW forms, LIKE/WHERE options, and COMPARE snapshot/output forms must parse and restore correctly. Invalid forms must remain rejected, keywords must retain their intended identifier compatibility, and COMPARE children must participate in both visitor paths. Parser tests cover generated receiver inventories and statement classification. The server must compile using the parser prerequisite alone. Ready validation requires lint; the parser-only scope does not require RealTiKV or runtime feature integration suites.

All initial and rebased checks above passed. Parser tests used `-tags=intest,deadlock` through the failpoint wrapper, with race detection and `-count=1`; cleanup reported a zero failpoint reference count. The initial server binary was written to `/tmp/mv-pr6a-misc-parser-tidb-server`, and the rebased binary to `/tmp/mv-pr6a-misc-parser-rebased-tidb-server`. After rebase, SQL parser, hint parser, keyword, and visitor regeneration produced no changes to the rebased code, and the complete parser race suite, 98-case source replay, server compilation, and Ready lint passed again. The source audit found no missing parser behavior in this slice.

Compatibility changes are the additional SQL syntax and AST/command classifications. Keywords remain unreserved and parser tests cover existing syntax. This prerequisite supplies parsing; SHOW/COMPARE execution belongs to later PRs. RealTiKV, runtime feature integration suites, and full CI were not run on this parser-only branch.

## Idempotence and Recovery


The combined reference branch is already committed and pushed; keep it intact. Rerun parser and Bazel generation after changing their inputs. Wait for failpoint cleanup before regenerating or reviewing final source. Inspect any generated differences before accepting them, and do not copy unrelated runtime implementations into this prerequisite to hide a compilation failure.

## Artifacts and Notes


The extraction changes 16 existing parser files: the statement labels, SHOW/COMPARE AST and classification definitions, visitor generator/inventory tests, grammar/token/keyword handling, parser/AST tests, and three regenerated artifacts. This execution record is the only additional file. Local logs under `/tmp/mv-pr6a-parser-*` contain generation, parser race, source audit, server build, lint, Bazel preparation, and cache migration checks. This plan records durable results rather than depending on those temporary logs alone.

## Interfaces and Dependencies


The new `ast.CompareMaterializedViewStmt` implements the existing AST statement/visitor interfaces, and the new SHOW kinds use `ast.ShowStmt`. Downstream runtime PRs consume these definitions without additional parser changes. No new third-party dependency, bootstrap schema, or metadata table is needed.

Revision: record the requested rebase onto `e2f259adf7d`, preservation of upstream hint changes, unchanged parser patch IDs, and repeated verification before publication. The user-requested persistent Bazel cache migration and cleanup remain verified.
