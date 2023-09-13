// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	vCtx, cancel := context.WithDeadline(
		ctx, time.Now().Add(5*time.Second))
	_, err = New(vCtx, b, &ExternalStorageOptions{
		CheckPermissions: []Permission{
			ListObjects,
			GetObject,
			AccessBuckets,
		},
	})
	cancel()
	if strings.Contains(err.Error(), "context deadline exceeded") {
		return errors.New("validate cloud storage uri timeout")
	}
	return err
}
