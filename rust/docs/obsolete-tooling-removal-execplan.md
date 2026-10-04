# Remove obsolete development gates and duplicated reports

This living ExecPlan follows PLANS.md. The user explicitly requests aggressive removal of stale code, docs, tests and scripts, following Go. No push is authorized.

## Purpose and context


Remove the source-line-count gate and its restoration instructions, the SELECT1 profiling executable that manually reconstructs obsolete statement phases, and the temporary parser probe module. Preserve its useful Go left-associativity assertion in the existing expression suite. Consolidate competing historical readiness/blocker reports into the current audit and one readiness reference, retaining exact before-images in Git.

Start: /workspace/tidb hparser-integration880246825ccaea2c945b963a7d1d49a500d6cf1a. Fresh Go master93a01d31f6da205ae4bf376825293903a6899fdb is exported in /workspace/.cloud-setup/go-master. Native master19a56cc remains unchanged; remote integration91010d96bb remains preserved unmerged. Full Go packages and57 unresolved findings remain unaccepted.

## Progress


- [x] Read local instructions, refresh Go, inspect all consumers and record initial Cargo targets.
- [x] Remove the obsolete gate/executable/probe and consolidate stale documentation.
- [x] Validate relocated parser behavior, exact target removal, retained caller references, affected checks, lint and self-review together.
- [ ] Normal local commit with actual locked-build hook; refresh recovery and Cloud draft. Exact boundary outcomes go in external final-handoff.json.

## Work and acceptance


Delete scripts/check-source-size.sh, its bounds file, its difftest-result-tests target/test and source-size-restoration-execplan. This retires a Rust file-layout requirement; it does not fix or conceal a Go SQL failure. Current user removal instructions supersede that plan's historical restoration request.

Delete tidb-server/src/bin/select-one-profile.rs. It rebuilds a private MySQL authentication client and manually times control_transaction/apply_set_stmt/statement_kind/run_with_columns; ordinary sessions now use parse_at_statement_boundary. Cargo auto-discovers this binary, so its mere presence adds an unrelated link to the required server build. Retain normal server, actual live-cluster tools and measured workload runners.

Move the pipes probe's existing SQL/tree assertion into tests/expr.rs::pipes_as_concat_sql_mode_matches_go. Master parser.y's left-associative pipes/or production and lexer.go's pipesAsOr conversion own this contract. Remove the unconditional production module declaration and its private renderer.

Replace the3915-line historical blocker diary and stale blocker workflow with concise links to current registers and recoverable historical evidence. Remove two duplicated CURRENT readiness reports; keep one concise readiness reference and repair its historical consumer. Update scripts/README to remove stale release12-job and broad cache/worktree cleanup advice.

## Validation


Activate /workspace/.cloud-setup/env.sh; run Cargo from rust/. Run cargo test --locked -p tidb-parser --lib -- pipes_as_concat_sql_mode_matches_go operator_precedence --test-threads=1 and affected all-target checking. Compare cargo metadata --locked --no-deps before/after: only select-one-profile and source_size_ratchet targets disappear. Root make lint, bash syntax for retained readiness scripts, git diff --check and actual hooks/pre-commit cargo build --locked -p tidb-server remain required. Use one build job for heavy links. No full suites, live multi-node or benchmark claim.

## Surprises & Discoveries


The source-size script still traverses every Rust crate despite the scripts README explicitly rejecting file-size gates. Its restoration plan points at an obsolete toolchain/workspace. The temporary parser module is compiled into the ordinary library, causing unused imports/function warnings. Historical reports also prescribe per-symptom pushes, contrary to the current no-push batch workflow.

## Decision Log


- Remove obsolete development policy rather than reorganizing correct Go owners to meet arbitrary line counts. Keep actual upstream obligations and failed validation receipts. Date:2026-10-04.
- Keep readiness delay/death/timeout checks and cleanup-path safety tests: they still exercise retained runners. No deletion merely because a Rust harness lacks an identically named Go file. Date:2026-10-04.

## Outcomes & Retrospective


Eight files are deleted; selected obsolete artifacts shrink from4800 to92lines before the small assertion relocation and receipt additions. Exactly two Cargo targets disappear, with every other target retained. Both parser cases, affected all-target checking, formatting, make lint and retained shell syntax checks pass. Exact removal inventory and prior source hashes are recorded under parity/current-audit/obsolete-tooling-removal-validation.json. The historical report before-images remain available through git show880246825c:<path>; local /tmp receipts mentioned there are not claimed to exist in Cloud.

## Recovery


Restore only selected paths from the recorded parent commit if needed; never reset concurrent work or force-push. Retain the verified unpublished bundle. Final local SHA, hook outcome, bundle and saved exact-ref draft belong in /workspace/.cloud-setup/obsolete-tooling-removal/final-handoff.json after completion.
