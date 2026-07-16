# Metadata Revision Draft for pingcap/tidb#69425

This document contains proposed replacement text for:

- Issue [pingcap/tidb#69399](https://github.com/pingcap/tidb/issues/69399)
- Pull request [pingcap/tidb#69425](https://github.com/pingcap/tidb/pull/69425)

The wording below makes the execution-mode scope explicit. Automatic pre-splitting follows the existing manual `PRE_SPLIT_REGIONS` target-keyspace behavior:

- txn reorg splits the normal index keyspace;
- ingest and txn-merge reorg split the temporary index keyspace used by concurrent DML;
- this feature does not additionally pre-split the normal index keyspace for fast reorg.

## Proposed Issue Content

### Proposed title

```text
Support automatic TopN-based index region pre-splitting for add-index jobs
```

### Proposed body

````markdown
## Feature Request

**Is your feature request related to a problem? Please describe:**
<!-- A clear and concise description of what the problem is. Ex. I'm always frustrated when [...] -->

TiDB supports manually pre-splitting index regions for add-index jobs through `PRE_SPLIT_REGIONS`. However, users or support engineers must understand the expected index-key distribution, estimate suitable split boundaries, and specify them before the DDL job starts.

Without suitable pre-splitting, writes to the new index keyspace can initially be concentrated in only a few TiKV Regions or stores. The affected write path depends on the add-index reorganization mode:

- In txn reorg, backfill and concurrent DML write the normal index keyspace.
- In ingest and txn-merge reorg, concurrent DML writes the temporary index keyspace while historical backfill writes the normal index keyspace.

The existing manual `PRE_SPLIT_REGIONS` behavior splits the normal index keyspace in txn reorg and the temporary index keyspace in ingest or txn-merge reorg. Users currently have no statistics-driven way to derive those manual split boundaries automatically.

Users also need a SQL-visible way to inspect whether automatic pre-splitting was applied, skipped, unsupported, or failed, instead of relying only on TiDB logs.

**Describe the feature you'd like:**
<!-- A clear and concise description of what you want to happen. -->

Add an opt-in automatic index-region pre-split feature for add-index jobs on non-partitioned tables. When the feature is enabled and the user has not explicitly specified `PRE_SPLIT_REGIONS`, TiDB should use available statistics to derive a bounded split plan from hot TopN values of the leading indexed column.

Automatic pre-splitting must preserve the target-keyspace behavior of explicit `PRE_SPLIT_REGIONS`:

- txn reorg: split the normal index keyspace;
- ingest or txn-merge reorg: split the temporary index keyspace used by concurrent DML.

The feature should:

1. Preserve existing manual behavior. An explicitly specified `PRE_SPLIT_REGIONS` option takes precedence over automatic pre-splitting.
2. Capture the session setting in the DDL job so owner changes and retries use the value from submission time.
3. Use only reliable, sufficiently healthy statistics and keep the number of generated split keys bounded.
4. Use the leading indexed column's TopN values. Later-column split planning is out of scope because ordinary column statistics do not describe their conditional distribution under each leading-column value.
5. Skip partitioned tables conservatively instead of planning per-partition or global-index split keys.
6. Be best-effort. Planning, splitting, scattering, or unsupported storage must not cancel the add-index job.
7. Expose the per-index automatic pre-split result in the `COMMENT` column of `SHOW DDL JOBS`.

The SQL-visible result should identify the affected index and show whether automatic pre-splitting was applied, skipped, unsupported, or failed. When available, it should include split-key, split-Region, and scattered-Region counts, plus the reason for a skipped or failed attempt.

Non-goals:

- This feature does not change the established manual `PRE_SPLIT_REGIONS` target-keyspace semantics.
- In ingest and txn-merge reorg, it does not additionally pre-split the normal/final index keyspace used by historical backfill.
- It does not replace ingest's own Region management during SST import.
- It does not derive split boundaries by scanning user table data.

**Describe alternatives you've considered:**
<!-- A clear and concise description of any alternative solutions or features you've considered. -->

- Continue requiring users to specify `PRE_SPLIT_REGIONS` manually. This remains available and takes precedence, but requires workload-specific knowledge before the DDL starts.
- Split both the normal and temporary index keyspaces in fast reorg. This would expand the behavior beyond existing manual pre-split semantics and is not part of this feature.
- Read table data to derive multi-column conditional distributions. This would add significant complexity and I/O before backfill, so the proposed implementation uses leading-column statistics only.

**Teachability, Documentation, Adoption, Migration Strategy:**
<!-- If you can, explain some scenarios how users might use this, situations it would be helpful in. Any API designs, mockups, or diagrams are also helpful. -->

The feature is disabled by default and enabled through a `GLOBAL | SESSION` system variable. Existing DDL behavior is unchanged when the variable is OFF or when the statement contains an explicit `PRE_SPLIT_REGIONS` option.

Documentation should explain the mode-specific target keyspace:

- txn reorg pre-splits the normal index;
- ingest and txn-merge reorg pre-split the temporary index used by concurrent DML;
- fast reorg does not gain an additional normal-index pre-split from this feature.

Examples of `SHOW DDL JOBS` comments:

```text
auto_split_hot_region=idx(split, split_keys=3, split_regions=3, scattered_regions=2)
auto_split_hot_region=idx(skipped, reason="stats pseudo")
auto_split_hot_region=idx(skipped, reason="partitioned table")
```
````

## Proposed Pull Request Content

### Proposed title

```text
ddl: add TopN-based automatic pre-splitting for add index
```

### Proposed body

````markdown
<!--

Thank you for contributing to TiDB!

PR Title Format:
1. pkg [, pkg2, pkg3]: what's changed
2. *: what's changed

-->

### What problem does this PR solve?
<!--

Please create an issue first to describe the problem.

There MUST be one line starting with "Issue Number:  " and
linking the relevant issues via the "close" or "ref".

For more info, check https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/contribute-code.html#referring-to-an-issue.

-->

Issue Number: ref #69399

Problem Summary:

TiDB supports manually pre-splitting index Regions for add-index jobs through `PRE_SPLIT_REGIONS`, but users must understand the expected index-key distribution and choose suitable split boundaries before the DDL starts.

This PR adds an opt-in, statistics-driven way to derive bounded split boundaries from hot TopN values. It intentionally preserves the existing manual pre-split target-keyspace semantics:

- txn reorg splits the normal index keyspace;
- ingest and txn-merge reorg split the temporary index keyspace used by concurrent DML.

This PR does not additionally pre-split the normal/final index keyspace used by historical backfill in ingest or txn-merge reorg.

### What changed and how does it work?

- Add the `tidb_ddl_enable_auto_split_hot_region` system variable.
  - Scope: `GLOBAL | SESSION`.
  - Default: `OFF`.
  - The session value is captured in the DDL job so retries and DDL-owner execution use the value from submission time.
- When the variable is enabled and the new index does not specify `PRE_SPLIT_REGIONS`, derive a bounded pre-split plan for non-partitioned tables.
  - Use available table statistics and selected TopN values from the leading indexed column.
  - Load the leading-column TopN directly from storage with normal priority when the cached statistics are not fully loaded.
  - Skip automatic pre-splitting when statistics are missing, pseudo, outdated, unhealthy, or below the table-size threshold.
  - Skip partitioned tables conservatively.
- Preserve existing manual `PRE_SPLIT_REGIONS` behavior and precedence.
- Preserve existing mode-specific target selection:
  - txn reorg splits normal index keys;
  - ingest and txn-merge reorg convert the generated boundaries to temporary-index keys.
- Do not add normal-index pre-splitting for ingest or txn-merge historical backfill. That behavior remains outside the scope of this PR.
- Keep automatic pre-splitting best-effort. Planning, split, scatter, or unsupported-storage results do not cancel the add-index job.
- Record a compact result for each index and expose it in the `COMMENT` column of `SHOW DDL JOBS`, including:
  - status: `split`, `skipped`, `failed`, or `unsupported`;
  - split-key, split-Region, and scattered-Region counts when available;
  - the reason for skipped or failed attempts.
- Preserve automatic pre-split results for normal add-index jobs and multi-schema-change subjobs.

Example comments:

```text
auto_split_hot_region=idx(split, split_keys=3, split_regions=3, scattered_regions=2)
auto_split_hot_region=idx(skipped, reason="stats pseudo")
auto_split_hot_region=idx(skipped, reason="partitioned table")
```

### Check List

Tests <!-- At least one of them must be included. -->

- [x] Unit test
- [x] Integration test
- [x] Manual test (add detailed scripts or steps below)
- [ ] No need to test
  > - [ ] I checked and no code files have been changed.
  > <!-- Or your custom  "No need to test" reasons -->

Unit tests cover:

- non-partitioned TopN-based split planning for skewed integer values;
- partitioned-table skip behavior;
- missing, pseudo, outdated, and small-table statistics;
- loading leading-column TopN directly from storage;
- the automatic gate, manual `PRE_SPLIT_REGIONS` override, best-effort failure handling, and unsupported storage;
- DDL job and subjob metadata copying;
- `SHOW DDL JOBS` comment formatting;
- global/session system-variable behavior.

Targeted commands:

```bash
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh pkg/ddl -run '^(TestPlanAutoSplitIndexRegionsTopN|TestPlanAutoSplitIndexRegionsSkipUnreliableStats|TestPreSplitIndexRegionsAutoGateAndManualOverride|TestAddIndexAutoSplitLoadsLeadingColumnTopNFromStorage)$' -count=1
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh pkg/executor -run '^(TestShowCommentsFromJob|TestShowCommentsFromSubJob)$' -count=1
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh pkg/sessionctx/variable -run '^TestSetTiDBDDLEnableAutoSplitHotRegion$' -count=1
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh pkg/statistics/handle/storage -run '^TestLoadStats$' -count=1
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh pkg/statistics/handle/syncload -run '^(TestConcurrentLoadHist|TestSyncLoadOnObjectWhichCanNotFoundInStorage)$' -count=1
```

RealTiKV functional test:

```bash
GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go ./tools/check/failpoint-go-test.sh tests/realtikvtest/addindextest3 -run '^TestAddIndexPresplitFunctional$' -count=1
```

The RealTiKV automatic pre-split scenario disables fast reorg and verifies the txn-reorg target keyspace. Existing manual `PRE_SPLIT_REGIONS` functional coverage verifies temporary-index target selection when fast reorg is enabled. This PR does not claim new normal-index pre-split coverage for fast reorg.

Manual test:

Created a non-partitioned skewed table with 1.2 million rows, enabled `tidb_ddl_enable_auto_split_hot_region`, ran `ALTER TABLE ... ADD INDEX`, and verified the result with:

```sql
SHOW TABLE t_skew INDEX idx_b REGIONS;
ADMIN CHECK TABLE t_skew;
```

The test verifies that the DDL completed and that `ADMIN CHECK TABLE` passed. The Region count alone does not establish that this feature pre-split the normal index keyspace in fast reorg, because fast-reorg import may use separate Region-management mechanisms.

Side effects

- [ ] Performance regression: Consumes more CPU
- [ ] Performance regression: Consumes more Memory
- [ ] Breaking backward compatibility

The feature is opt-in and disabled by default. When enabled, it adds bounded statistics reads and Region split/scatter requests before the add-index state transition. The target keyspace follows existing explicit `PRE_SPLIT_REGIONS` behavior.

Documentation

- [x] Affects user behaviors
- [ ] Contains syntax changes
- [x] Contains variable changes
- [x] Contains experimental features
- [ ] Changes MySQL compatibility

Documentation must state that txn reorg targets the normal index keyspace, while ingest and txn-merge reorg target the temporary index keyspace used by concurrent DML. This PR does not add normal-index pre-splitting for fast reorg.

### Release note

<!-- compatibility change, improvement, bugfix, and new feature need a release note -->

Please refer to [Release Notes Language Style Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html) to write a quality release note.

```release-note
Add an opt-in system variable `tidb_ddl_enable_auto_split_hot_region` that derives bounded add-index pre-split boundaries from leading-column TopN statistics for non-partitioned tables. The feature follows existing `PRE_SPLIT_REGIONS` target selection: txn reorg splits the normal index, while ingest and txn-merge reorg split the temporary index used by concurrent DML. The result is exposed in `SHOW DDL JOBS`.
```
````
