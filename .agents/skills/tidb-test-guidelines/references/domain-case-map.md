# TiDB domain Test Case Map (pkg/domain)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/domain

### Tests
- `pkg/domain/db_test.go` - Tests domain session.
- `pkg/domain/domain_test.go` - Tests domain info.
- `pkg/domain/domain_utils_test.go` - Tests error code.
- `pkg/domain/domainctx_test.go` - Tests domain ctx.
- `pkg/domain/extract_test.go` - Tests extract plan without history view.
- `pkg/domain/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/domain/plan_replayer_handle_test.go` - Tests plan replayer handle collect task.
- `pkg/domain/plan_replayer_slow_log_test.go` - Tests plan replayer internal query.
- `pkg/domain/plan_replayer_test.go` - Tests plan replayer with different GC.
- `pkg/domain/ru_stats_test.go` - Tests write RU statistics.
- `pkg/domain/schema_checker_test.go` - Tests schema checker simple.
- `pkg/domain/topn_slow_query_test.go` - Tests top-N slow query pushdown.

## pkg/domain/crossks

### Tests
- `pkg/domain/crossks/cross_ks_test.go` - Tests cross-keyspace manager in classical mode.

## pkg/domain/globalconfigsync

### Tests
- `pkg/domain/globalconfigsync/globalconfig_test.go` - Tests global config sync.

## pkg/domain/infosync

### Tests
- `pkg/domain/infosync/info_test.go` - Tests info sync.

## pkg/domain/serverinfo

### Tests
- `pkg/domain/serverinfo/syncer_test.go` - Tests server info syncer.
