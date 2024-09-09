// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

func init() {
	variable.ValidateCloudStorageURI = ValidateCloudStorageURI
}

// ValidateCloudStorageURI makes validation for tidb_cloud_storage_uri.
func ValidateCloudStorageURI(ctx context.Context, uri string) error {
	b, err := ParseBackend(uri, nil)
	if err != nil {
		return err
	}
	_, err = New(ctx, b, &ExternalStorageOptions{
		CheckPermissions: []Permission{
			ListObjects,
			GetObject,
			AccessBuckets,
		},
	})
	return err
}

// activeUploadWorkerCnt is the active upload worker count, it only works for GCS.
// For S3, we cannot get it.
var activeUploadWorkerCnt atomic.Int64

// GetActiveUploadWorkerCount returns the active upload worker count.
func GetActiveUploadWorkerCount() int64 {
	return activeUploadWorkerCnt.Load()
}
