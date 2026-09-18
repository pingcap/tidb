// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import "context"

// BucketPrefix is a tiny compatibility wrapper used by tests ported from
// object-store code paths.
type BucketPrefix struct {
	Bucket string
	Prefix string
}

// NewBucketPrefix creates a BucketPrefix compatibility wrapper.
func NewBucketPrefix(bucket string, prefix string) BucketPrefix {
	return BucketPrefix{
		Bucket: bucket,
		Prefix: prefix,
	}
}

// WithContentMD5 exposes the compatibility option for external tests.
var WithContentMD5 = withContentMD5

// MultipartWriter is a compatibility wrapper for tests ported from object-store code paths.
func (rs *S3Storage) MultipartWriter(ctx context.Context, name string) (ExternalFileWriter, error) {
	return rs.createUploader(ctx, name)
}
