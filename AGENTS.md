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

## Building

### Bazel bootstrap (`make bazel_prepare`)

Run `make bazel_prepare` **before building** when:
- You just cloned the repo / set up a new workspace
- You changed Bazel-related files (for example: `WORKSPACE`, `DEPS.bzl`, `BUILD.bazel`)
- You changed Go module deps used by the build (for example: `go.mod`, `go.sum`), such as **adding a new third-party dependency**
- You added new unit tests (UT) or RealTiKV tests and updated Bazel test targets accordingly (for example: adding new `_test.go` files to a `go_test` rule `srcs`, adjusting `shard_count`, or creating/updating `BUILD.bazel` under `tests/realtikvtest/`), which may require refreshing Bazel deps/toolchain
- You hit Bazel dependency/toolchain errors locally

Recommended local build flow:

```bash
# one-time (or when bazel deps/toolchain change)
make bazel_prepare

# build
make

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
  Please verify that the modifications are correct and notify the developer.
- If the execution fails, please check the error message and notify the developer.

#### When to enable failpoint

Before running unit tests, check if the target package uses failpoint:

```bash
grep -R -n "failpoint.Enable" pkg/<package_name>
grep -R -n "testfailpoint.Enable" pkg/<package_name>
```

**Rules:**
- If grep returns matches → `make failpoint-enable` is required
- If grep returns nothing → do NOT enable failpoint (unnecessary overhead)

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

#### Unit Tests Specification

The following points must be achieved:
1. Within the same package, there should not be more than 50 unit tests. The exact number can be referenced from the `shard_count` in the `BUILD.bazel` file under the test path.
2. Existing tests should be reused as much as possible, and existing test data and table structures should be utilized. Modifications should be made on this basis to accommodate the new tests.
3. Some tests use the JSON files in `testdata` as the test set (`xxxx_in.json`) and the validation set (`xxxx_out.json` and `xxxx_xut.json`). It is necessary to modify the test set before running the unit test.

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

#### When to use realtikvtest

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

#### How to verify cluster is ready

Check PD API - if it returns JSON, the cluster is ready:

```bash
curl -f http://127.0.0.1:2379/pd/api/v1/version
```

Wait until ready:

```bash
until curl -sf http://127.0.0.1:2379/pd/api/v1/version >/dev/null; do sleep 1; done
```

#### 2. Enable Failpoint (if needed)

If target test uses `failpoint` / `testfailpoint`, enable failpoint before running tests.

**How to check if needed**:
```bash
grep -R -n "failpoint.Enable" tests/realtikvtest/<dir>
grep -R -n "testfailpoint.Enable" tests/realtikvtest/<dir>
```

If needed, use this pattern to ensure failpoint is always disabled:

```bash
make failpoint-enable && (
  go test -run <TestName> --tags=intest ./tests/realtikvtest/<dir>/...;
  rc=$?;
  make failpoint-disable;
  exit $rc
)
```

#### 3. Run Tests

```bash
go test -run <TestName> --tags=intest ./tests/realtikvtest/<dir>/...
```

**Note**: Do not add `-v` by default to avoid excessive log output. Only add `-v` when debugging.

#### 4. Cleanup

**Required**: If you started TiUP Playground, you must clean up after tests.

```bash
# 1) Stop playground (will also stop pd/tikv)
pkill -f "tiup playground" || true

# 2) (Optional) Clean component data directories
tiup clean --all
```

Verify stopped (should fail to connect):

```bash
curl -f http://127.0.0.1:2379/pd/api/v1/version
```

#### Key Tips

- **Failpoints**: Use `failpoint` and `testfailpoint` to simulate abnormal behavior.
- **Atomicity**: Use `atomic` variables to track logic in concurrent tests.
- **Environment check**: Check for running playground processes before starting.
- **Fmt-only changes**: If PR only involves code formatting (gofmt, indentation), do NOT run time-consuming `realtikvtest`. Just ensure local compilation passes.

## Pull Request Instructions

### PR title

The PR title **must** strictly adhere to the following format. It uses the package name(s) affected or `*` if it's a non-package-specific change or too many packages involved:

**Format 1 (Specific Packages):** `pkg [, pkg2, pkg3]: what is changed`

**Format 2 (Repository-Wide):** `*: what is changed`

### PR description

The PR description **must** strictly follow the template located at @.github/pull_request_template.md and **must** keep the HTML comment elements like `Tests <!-- At least one of them must be included. -->` unchanged in the pull request description according to the pull request template. These elements are essential for CI and removing them will cause processing failures.
