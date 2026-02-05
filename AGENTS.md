# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Developing Environment Tips

### Code Organization

**Package Structure:**

- `/pkg/bindinfo/` - Handles all global SQL bind operations and caches SQL bind info from storage.
- `/pkg/config/` - Configuration definitions.
- `/pkg/ddl/` - Data Definition Language (DDL) execution logic.
- `/pkg/distsql/` - Abstraction of distributed computing interfaces between executor and TiKV client.
- `/pkg/domain/` - Storage space abstraction (domain/namespace), manages information schema and statistics.
- `/pkg/errno/` - MySQL error code, message, and summary definitions.
- `/pkg/executor/` - Execution logic for most SQL statements (operators).
- `/pkg/expression/` - Expression-related code, including operators and built-in functions.
- `/pkg/infoschema/` - Metadata management for SQL statements and information schema operations.
- `/pkg/kv/` - Key-Value engine interface and public methods; storage engine adaptation layer.
- `/pkg/lock/` - Implementation of LOCK/UNLOCK TABLES.
- `/pkg/meta/` - SQL metadata management in storage engine; used by infoschema and DDL.
- `/pkg/meta/autoid/` - Generates globally unique, monotonically increasing IDs for tables and databases.
- `/pkg/metrics/` - Metrics information for all modules.
- `/pkg/owner/` - Coordinates tasks that must be executed by a single instance in a TiDB cluster.
- `/pkg/parser/` - MySQL-compatible SQL parser and AST/data structure definitions.
- `/pkg/planner/` - Query optimization logic.
- `/pkg/planner/core/base/` - interfaces for logical and physical plans.
- `/pkg/planner/core/operator/logicalop` - Logical plan operators.
- `/pkg/planner/core/operator/physicalop` - Physical plan operators.
- `/pkg/plugin/` - TiDB plugin framework.
- `/pkg/privilege/` - User privilege management interface.
- `/pkg/server/` - MySQL protocol and connection management.
- `/pkg/session/` - Session management code.
- `/pkg/sessionctx/binloginfo/` - Binlog output information.
- `/pkg/sessionctx/stmtctx/` - Runtime statement context for sessions.
- `/pkg/sessionctx/variable/` - System variable management.
- `/pkg/statistics/` - Table statistics code.
- `/pkg/store/` - Storage engine drivers, wraps Key-Value client for TiDB.
- `/pkg/structure/` - Structured API on transactional KV API (List, Queue, HashMap, etc.).
- `/pkg/table/` - Table abstraction in SQL.
- `/pkg/tablecodec/` - Encode/decode SQL data to/from Key-Value.
- `/pkg/telemetry/` - Telemetry collection and reporting.
- `/pkg/types/` - Type definitions and operations.
- `/pkg/util/` - Utilities.
- `/cmd/tidb-server/` - Main entry for TiDB service.

### Source Files

- When creating new source files (for example: `*.go`), include the standard TiDB copyright (and Apache 2.0 license) header at the top; copy the header from an existing file in the same directory and update the year if needed.

### Notes

- Notes directory: `docs/note/<component>/` is the canonical location for component notes. If missing, create it and add an entry here.
- Notes rules: update existing sections when topics overlap; append new sections only for new topics. Purpose: capture decisions, pitfalls, and test patterns.
- DDL module-only rules (applies to changes under `pkg/ddl/` and `docs/note/ddl/`):
  - **REQUIRED**: Before making/reviewing any DDL changes in the DDL module, read `docs/note/ddl/README.md` first and use it as the default map of the execution framework.
  - Debugging: You may reference `docs/note/ddl/*`, but you **MUST NOT** treat it as authoritative. Treat it as hypotheses until verified in code/tests (avoid hallucination/outdated assumptions).
  - Doc drift: If implementation and `docs/note/ddl/*` differ, you **MUST** update the notes to match reality and call it out in the PR/issue. Do not defer.
- Planner rule notes: `docs/note/planner/rule/rule_ai_notes.md`.
- If a single notes file exceeds 2000 lines, split by functionality into multiple markdown files and update references here.
- Predicate pushdown testdata (`pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_in.json`) should contain SQL-only cases; put DDL in the test setup to avoid `EXPLAIN` parsing DDL during record runs.
- Integration test recording uses `./run-tests.sh -r <name>` (not `-record`).

## Building

### Bazel bootstrap (`make bazel_prepare`)

Run `make bazel_prepare` **before building** when:
- You just cloned the repo / set up a new workspace
- You changed Bazel-related files (for example: `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`)
- You added/removed/renamed/moved any Go source files (for example: `*.go`) in this PR; ALWAYS run `make bazel_prepare` and include any resulting `*.bazel/*.bzl` changes in the PR.
- You changed Go module deps used by the build (for example: `go.mod`, `go.sum`), such as **adding a new third-party dependency**
- You added new unit tests (UT) or RealTiKV tests and updated Bazel test targets accordingly (for example: adding new `_test.go` files to a `go_test` rule `srcs`, adjusting `shard_count`, or creating/updating `BUILD.bazel` under `tests/realtikvtest/`), which may require refreshing Bazel deps/toolchain
- You hit Bazel dependency/toolchain errors locally

Recommended local build flow:

```bash
# one-time (or when bazel deps/toolchain change)
make bazel_prepare

# build
make bazel_bin

# optional: regenerate generated code if needed
make gogenerate

# optional: keep Go modules tidy if go.mod/go.sum changed
go mod tidy
```

## Testing 

### Unit Tests

Standard Go tests throughout `/pkg/` packages

#### How to run unit tests

```bash
# in the root directory of the repository
pushd pkg/<package_name>
go test -run  <TestName>  -record --tags=intest
popd
```

- If the execution is successful, please check whether the result set file has been modified. If it has been modified,
  please verify that the modifications are correct and notify the developer.
- If the execution fails, please check the error message and notify the developer.
- Prefer targeted test runs (use `-run <TestName>`); avoid running all tests in a directory/package unless necessary (e.g. broad refactors, reproducing CI failures, or updating shared testdata/golden files) because it is time-consuming.

#### When to enable failpoint

Before running unit tests, check if the target package uses failpoint:

```bash
grep -R -n "failpoint\\." pkg/<package_name>
grep -R -n "testfailpoint\\." pkg/<package_name>
# Optional (Bazel): if BUILD.bazel exists, check failpoint dependency.
test -f pkg/<package_name>/BUILD.bazel && grep -n "@com_github_pingcap_failpoint//:failpoint" pkg/<package_name>/BUILD.bazel
```

**Rules:**
- If grep returns matches → `make failpoint-enable` is required
- If grep returns nothing → do NOT enable failpoint (unnecessary overhead)

**Note:** `--tags=intest` is a separate build tag and does not enable failpoints. Enable/disable failpoints via `make failpoint-enable` / `make failpoint-disable` (or `make bazel-failpoint-enable` for Bazel).

**Ensure failpoint is always disabled** (use this pattern):

```bash
make failpoint-enable && (
  pushd pkg/<package_name>
  go test -run <TestName> --tags=intest;
  rc=$?;
  popd;
  make failpoint-disable;
  exit $rc
)
```

**Bazel note:** If you run tests via Bazel, use `make bazel-failpoint-enable` before `bazel test` / `make bazel_test`. You can still run `make failpoint-disable` afterward to restore the workspace (Bazel currently doesn't provide `bazel-failpoint-disable`).

#### Unit Tests Specification

The following points must be achieved:
1. Within the same package, there should not be more than 50 unit tests. The exact number can be referenced from the `shard_count` in the `BUILD.bazel` file under the test path.
2. Existing tests should be reused as much as possible, and existing test data and table structures should be utilized. Modifications should be made on this basis to accommodate the new tests.
3. Some tests use the JSON files in `testdata` as the test set (`xxxx_in.json`) and the validation set (`xxxx_out.json` and `xxxx_xut.json`). It is necessary to modify the test set before running the unit test.

#### Regression tests for bug fixes

- A bug fix should include a regression test that reproduces the issue.
- Verify the new/updated test fails on the buggy code (before the fix), and passes after the fix.
  - Example approaches: run the test on `upstream/master` (or the target base commit) before applying the fix, or temporarily revert the fix and confirm the test fails.
- Include the exact test command in the PR description under `Tests` (for example: `go test -run TestXxx --tags=intest ./pkg/...`).

### Integration Tests

Integration tests are located in the `/tests/integrationtest` directory.

The test set is located at `/tests/integrationtest/t`, and the result set is in `tests/integrationtest/r`. The result set does not need to be modified, but it is necessary to verify its correctness after running the tests.

#### How to run integration tests

```bash
# in the root directory of the repository
pushd tests/integrationtest
./run-tests.sh -r <TestName>
popd
```

If you modify the test set `t/planner/core/binary_plan.test`, then the `TestName` will be `planner/core/binary_plan`.

### RealTiKV Tests

RealTiKV tests are located in the `/tests/realtikvtest` directory. These tests run against a real TiKV cluster (not mocktikv/unistore).

#### When to use the RealTiKV Tests

- Tests that require real TiKV / TiUP Playground / TiKV real environment
- Tests located under `tests/realtikvtest/` directory tree

#### 1. Start TiDB Playground

Before running `realtikvtest`, start a minimal PD + TiKV cluster (`tikv-slim`). Most test cases use PD address `127.0.0.1:2379` by default.

**Must run in background (do not omit `&`)**:

```bash
tiup playground --mode tikv-slim &
```

(Optional) Use `--tag` to distinguish different playgrounds:

```bash
tiup playground --mode tikv-slim --tag realtikvtest &
```

**Note:** Using `--tag` will keep the data dir after exit. Remember to remove `${HOME}/.tiup/data/<tag>` in the cleanup step.

If `127.0.0.1:2379` is not available (for example: shared dev machine, port conflict, or multiple playgrounds on one host), you can change the PD address:
- Use `--pd.port` to set PD port explicitly
- Or use `--port-offset` to shift all default ports

Examples:

```bash
tiup playground --mode tikv-slim --pd.port 12379 &
# or
tiup playground --mode tikv-slim --port-offset 10000 &
```

#### (Optional) Verify cluster is ready

Recommended to avoid flaky failures caused by PD/TiKV not ready yet:

```bash
PD_ADDR=127.0.0.1:2379
curl -f "http://${PD_ADDR}/pd/api/v1/version"
until curl -sf "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; do sleep 1; done
```

#### 2. Run Tests

```bash
go test -run <TestName> --tags=intest ./tests/realtikvtest/<dir>/...
```

If target test uses failpoints, enable them first (see the **When to enable failpoint** section above), and ensure they are disabled afterward.

If you use a non-default PD address, pass it via `-args -tikv-path`:

```bash
go test -run <TestName> --tags=intest ./tests/realtikvtest/<dir>/... -args \
  -tikv-path "tikv://127.0.0.1:12379?disableGC=true"
```

**Note**: Do not add `-v` by default to avoid excessive log output. Only add `-v` when debugging.

#### 3. Cleanup

**Required**: If you started TiUP Playground, you must clean up after tests.

```bash
# 1) Stop playground (will also stop pd/tikv)
pkill -f "tiup playground" || true

# 2) If you used `--tag <tag>`, remove its data dir (TiUP will not auto-clean it)
#    Example: rm -rf "${HOME}/.tiup/data/realtikvtest"
rm -rf "${HOME}/.tiup/data/<tag>"

# 3) (Optional) Clean component data directories
tiup clean --all
```

Verify stopped (should fail to connect):

```bash
PD_ADDR=127.0.0.1:2379
curl -f "http://${PD_ADDR}/pd/api/v1/version"
```

#### Key Tips

- **Failpoints**: Use `failpoint` and `testfailpoint` to simulate abnormal behavior.
- **Atomicity**: Use `atomic` variables to track logic in concurrent tests.
- **Environment check**: Check for running playground processes before starting.
- **Fmt-only changes**: If PR only involves code formatting (gofmt, indentation), do NOT run time-consuming `realtikvtest`. Just ensure local compilation passes.

## Issue Instructions

- When submitting an issue, follow the GitHub templates under `.github/ISSUE_TEMPLATE/` and fill in all required fields.
- Bug reports should include minimal reproduction steps, expected/actual behavior, and the TiDB version (for example: the output of `SELECT tidb_version()`).
- Search existing issues/PRs first to avoid duplicates (try `gh` first; for example: `gh search issues --repo pingcap/tidb --include-prs "<keywords>"`), and include any relevant logs/configuration/SQL plans to help diagnosis.
- Apply labels to help triage:
  - `type/*` is usually applied by the issue template; add `type/regression` when applicable.
  - Add at least one `component/*` label (for example: `component/ddl`, `component/br`, `component/parser`).
  - For bug/regression issues, `severity/*` and affected-version label(s) are required (for example: `affects-8.5`; use `may-affects-*` if unsure).
  - If you don't have permission to add labels, include a `Suggested labels: ...` line in the issue body.

## Pull Request Instructions

### PR title

The PR title **must** strictly adhere to the following format. It uses the package name(s) affected or `*` if it's a non-package-specific change or too many packages involved:

**Format 1 (Specific Packages):** `pkg [, pkg2, pkg3]: what is changed`

**Format 2 (Repository-Wide):** `*: what is changed`

### PR description

The PR description **must** strictly follow the template located at @.github/pull_request_template.md and **must** keep the HTML comment elements like `Tests <!-- At least one of them must be included. -->` unchanged in the pull request description according to the pull request template. These elements are essential for CI and removing them will cause processing failures.

### Language

Issues and PRs **must** be written in English (title and description).

### Force push

Avoid force-push whenever possible; prefer adding follow-up commits and letting GitHub squash-merge. If a force-push is unavoidable, use `--force-with-lease` and coordinate with reviewers.
