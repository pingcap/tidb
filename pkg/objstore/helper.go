// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	// To make goleak happy.
	httpCli := http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	storage, err := New(ctx, b, &storeapi.Options{
		HTTPClient: &httpCli,
		CheckPermissions: []storeapi.Permission{
			storeapi.ListObjects,
			storeapi.GetObject,
			storeapi.AccessBuckets,
		},
	})
	if err != nil {
		return err
	}
	storage.Close()
	return nil
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
func UnmarshalDir[T any](ctx context.Context, walkOpt *storeapi.WalkOption, s storeapi.Storage, unmarshal func(target *T, name string, content []byte) error) iter.TryNextor[*T] {
	ch := make(chan *T)
	errCh := make(chan error, 1)
	reader := func() {
		defer close(ch)
		pool := util.NewWorkerPool(128, "metadata")
		eg, ectx := errgroup.WithContext(ctx)
		err := s.WalkDir(ectx, walkOpt, func(path string, size int64) error {
			pool.ApplyOnErrorGroup(eg, func() error {
				metaBytes, err := s.ReadFile(ectx, path)
				if err != nil {
					log.Error("failed to read file", zap.String("file", path))
					return errors.Annotatef(err, "during reading meta file %s from storage", path)
				}

				var meta T
				if err := unmarshal(&meta, path, metaBytes); err != nil {
					return errors.Annotatef(err, "failed to unmarshal file %s", path)
				}
				select {
				case ch <- &meta:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			})
			return nil
		})
		if err == nil {
			err = eg.Wait()
		}
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
