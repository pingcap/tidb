// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
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

// UnmarshalDir iterates over a prefix, then "unmarshal" the content of each file it met with the unmarshal function.
// Returning an iterator that yields the unmarshaled content.
// The "unmarshal" function should put the result of unmarshalling to the `target` argument.
func UnmarshalDir[T any](ctx context.Context, walkOpt *WalkOption, s ExternalStorage, unmarshal func(target *T, name string, content []byte) error) iter.TryNextor[*T] {
	ch := make(chan *T)
	errCh := make(chan error, 1)
	reader := func() {
		defer close(ch)
		err := s.WalkDir(ctx, walkOpt, func(path string, size int64) error {
			metaBytes, err := s.ReadFile(ctx, path)
			if err != nil {
				return errors.Annotatef(err, "failed during reading file %s", path)
			}
			var meta T
			if err := unmarshal(&meta, path, metaBytes); err != nil {
				return errors.Annotatef(err, "failed to parse subcompaction meta of file %s", path)
			}
			select {
			case ch <- &meta:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		if err != nil {
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
		}
	}
	go reader()
	return iter.Func(func(ctx context.Context) iter.IterResult[*T] {
		select {
		case <-ctx.Done():
			return iter.Throw[*T](ctx.Err())
		case err := <-errCh:
			return iter.Throw[*T](err)
		case meta, ok := <-ch:
			if !ok {
				return iter.Done[*T]()
			}
			return iter.Emit(meta)
		}
	})
}
