// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"

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
