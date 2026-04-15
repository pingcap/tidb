# TiDB importsdk Test Case Map (pkg/importsdk)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/importsdk

### Tests
- `pkg/importsdk/config_test.go` - Tests default SDK config.
- `pkg/importsdk/file_scanner_test.go` - Tests create data file meta.
- `pkg/importsdk/job_manager_test.go` - Tests submit job.
- `pkg/importsdk/model_test.go` - Tests job status.
- `pkg/importsdk/pattern_test.go` - Tests longest common prefix.
- `pkg/importsdk/sdk_test.go` - Tests cloud SDK.
- `pkg/importsdk/sql_generator_test.go` - Tests generate import SQL.
