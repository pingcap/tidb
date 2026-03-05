# TiDB objstore Test Case Map (pkg/objstore)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/objstore

### Tests
- `pkg/objstore/azblob_test.go` - Tests azblob.
- `pkg/objstore/batch_test.go` - Tests batched.
- `pkg/objstore/compress_test.go` - Tests with compress read write file.
- `pkg/objstore/gcs_test.go` - Tests GCS.
- `pkg/objstore/local_test.go` - Tests delete file.
- `pkg/objstore/locking_test.go` - Tests try lock remote.
- `pkg/objstore/memstore_test.go` - Tests mem store basic.
- `pkg/objstore/parse_test.go` - Tests create storage.
- `pkg/objstore/storage_test.go` - Tests default HTTP transport.

## pkg/objstore/objectio

### Tests
- `pkg/objstore/objectio/writer_test.go` - Tests external file writer.

## pkg/objstore/ossstore

### Tests
- `pkg/objstore/ossstore/client_test.go` - Tests client permission.
- `pkg/objstore/ossstore/credential_test.go` - Tests credential refresher.
- `pkg/objstore/ossstore/store_test.go` - Tests store.

## pkg/objstore/recording

### Tests
- `pkg/objstore/recording/recording_test.go` - Tests requests recording.

## pkg/objstore/s3like

### Tests
- `pkg/objstore/s3like/permission_test.go` - Tests check permissions.

## pkg/objstore/s3store

### Tests
- `pkg/objstore/s3store/client_test.go` - Tests client permission.
- `pkg/objstore/s3store/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/objstore/s3store/retry_test.go` - Tests S3 TiDB retrier never exhaust tokens.
- `pkg/objstore/s3store/s3_flags_test.go` - Tests S3 profile flag.
- `pkg/objstore/s3store/s3_test.go` - Tests apply options.

## pkg/objstore/storeapi

### Tests
- `pkg/objstore/storeapi/storage_test.go` - Tests prefix.
