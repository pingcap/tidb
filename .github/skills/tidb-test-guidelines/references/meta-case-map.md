# TiDB meta Test Case Map (pkg/meta)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/meta

### Tests
- `pkg/meta/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/meta/meta_test.go` - Tests placement policy.

## pkg/meta/autoid

### Tests
- `pkg/meta/autoid/autoid_test.go` - Tests signed auto ID.
- `pkg/meta/autoid/bench_test.go` - Tests allocator alloc.
- `pkg/meta/autoid/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/meta/autoid/memid_test.go` - Tests in-memory alloc.
- `pkg/meta/autoid/seq_autoid_test.go` - Tests sequence autoid.

## pkg/meta/metabuild

### Tests
- `pkg/meta/metabuild/context_test.go` - Tests meta build context.

## pkg/meta/metadef

### Tests
- `pkg/meta/metadef/db_test.go` - Tests is mem DB.
- `pkg/meta/metadef/system_test.go` - Tests is reserved ID.

## pkg/meta/model

### Tests
- `pkg/meta/model/bdr_test.go` - Tests action BDR map.
- `pkg/meta/model/column_test.go` - Tests default value.
- `pkg/meta/model/index_test.go` - Tests is index prefix covered.
- `pkg/meta/model/job_args_test.go` - Tests get or decode args V2.
- `pkg/meta/model/job_test.go` - Tests job start time.
- `pkg/meta/model/placement_test.go` - Tests placement settings string.
- `pkg/meta/model/table_test.go` - Tests move column info.
