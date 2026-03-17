# TiDB session Test Case Map (pkg/session)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/session

### Tests
- `pkg/session/bench_test.go` - session: Tests basic.
- `pkg/session/bootstrap_test.go` - session: Tests MySQL DB tables.
- `pkg/session/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/session_test.go` - session: Tests get start mode.
- `pkg/session/tidb_test.go` - session: Tests do map handle nil.
- `pkg/session/upgrade_test.go` - session: Tests upgrade to ver functions check.

## pkg/session/cursor

### Tests
- `pkg/session/cursor/tracker_test.go` - session/cursor: Tests new cursor.

## pkg/session/sessmgr

### Tests
- `pkg/session/sessmgr/processinfo_test.go` - session/sessmgr: Tests process info shallow copy.

## pkg/session/syssession

### Tests
- `pkg/session/syssession/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/syssession/pool_test.go` - session/syssession: Tests new session pool.
- `pkg/session/syssession/session_integration_test.go` - session/syssession: Tests domain advanced session pool internal session registry.
- `pkg/session/syssession/session_test.go` - session/syssession: Tests new internal session.

## pkg/session/test

### Tests
- `pkg/session/test/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/session_test.go` - session/test: Tests schema checker SQL.
- `pkg/session/test/tidb_test.go` - session/test: Tests parse error warn.

## pkg/session/test/bootstraptest

### Tests
- `pkg/session/test/bootstraptest/boot_test.go` - session/test: Tests write DDL table version to MySQL TiDB.
- `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go` - session/test: Tests upgrade version83 and version84.
- `pkg/session/test/bootstraptest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/session/test/bootstraptest2

### Tests
- `pkg/session/test/bootstraptest2/boot_test.go` - session/test: Tests write DDL table version to MySQL TiDB when upgrading to 178.
- `pkg/session/test/bootstraptest2/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/session/test/clusteredindextest

### Tests
- `pkg/session/test/clusteredindextest/clustered_index_test.go` - session/test: Tests clustered insert ignore batch get key count.
- `pkg/session/test/clusteredindextest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/session/test/common

### Tests
- `pkg/session/test/common/common_test.go` - session/test: Tests misc.
- `pkg/session/test/common/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/session/test/meta

### Tests
- `pkg/session/test/meta/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/meta/session_test.go` - session/test: Tests init DDL tables.

## pkg/session/test/nontransactionaltest

### Tests
- `pkg/session/test/nontransactionaltest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/nontransactionaltest/nontransactional_test.go` - session/test: Tests non-transactional DML sharding.

## pkg/session/test/privileges

### Tests
- `pkg/session/test/privileges/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/privileges/privileges_test.go` - session/test: Tests skip with grant.

## pkg/session/test/resourcegrouptest

### Tests
- `pkg/session/test/resourcegrouptest/resource_group_test.go` - session/test: Tests resource group hint in txn.

## pkg/session/test/schematest

### Tests
- `pkg/session/test/schematest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/schematest/schema_test.go` - session/test: Tests prepare stmt commit when schema changed.

## pkg/session/test/temporarytabletest

### Tests
- `pkg/session/test/temporarytabletest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/temporarytabletest/temporary_table_test.go` - session/test: Tests local temporary table update.

## pkg/session/test/txn

### Tests
- `pkg/session/test/txn/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/txn/txn_test.go` - session/test: Tests autocommit.

## pkg/session/test/variable

### Tests
- `pkg/session/test/variable/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/variable/variable_test.go` - session/test: Tests forbid setting both TS variable.

## pkg/session/test/vars

### Tests
- `pkg/session/test/vars/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/session/test/vars/vars_test.go` - session/test: Tests KV vars.
