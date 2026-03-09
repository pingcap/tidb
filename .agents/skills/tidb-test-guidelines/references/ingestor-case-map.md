# TiDB ingestor Test Case Map (pkg/ingestor)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/ingestor/ingestcli

### Tests
- `pkg/ingestor/ingestcli/client_test.go` - Tests JSON byte slice.
- `pkg/ingestor/ingestcli/ingest_err_test.go` - Tests ingest API error retryable.