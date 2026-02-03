// Copyright 2020 PingCAP, Inc.
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
	"io"
	"net/http"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/ossstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/s3store"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// TombstoneSize is the tombstone size.
	TombstoneSize int64 = -1
)

// Create creates Storage.
//
// Please consider using `New` in the future.
func Create(ctx context.Context, backend *backuppb.StorageBackend, sendCreds bool) (storeapi.Storage, error) {
	return New(ctx, backend, &storeapi.Options{
		SendCredentials: sendCreds,
		HTTPClient:      nil,
	})
}

// NewWithDefaultOpt creates Storage with default options.
func NewWithDefaultOpt(ctx context.Context, backend *backuppb.StorageBackend) (storeapi.Storage, error) {
	return New(ctx, backend, nil)
}

// NewFromURL creates an Storage from URL.
func NewFromURL(ctx context.Context, uri string) (storeapi.Storage, error) {
	if len(uri) == 0 {
		return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "empty store is not allowed")
	}
	u, err := ParseRawURL(uri)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if u.Scheme == "memstore" {
		return NewMemStorage(), nil
	}
	b, err := parseBackend(u, uri, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewWithDefaultOpt(ctx, b)
}

// New creates an Storage with options.
func New(ctx context.Context, backend *backuppb.StorageBackend, opts *storeapi.Options) (storeapi.Storage, error) {
	if opts == nil {
		opts = &storeapi.Options{}
	}
	switch backend := backend.Backend.(type) {
	case *backuppb.StorageBackend_Local:
		if backend.Local == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "local config not found")
		}
		return NewLocalStorage(backend.Local.Path)
	case *backuppb.StorageBackend_Hdfs:
		if backend.Hdfs == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "hdfs config not found")
		}
		return NewHDFSStorage(backend.Hdfs.Remote), nil
	case *backuppb.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "s3 config not found")
		}
		if backend.S3.Provider == s3like.KS3SDKProvider {
			return s3store.NewKS3Storage(ctx, backend.S3, opts)
		} else if backend.S3.Provider == s3like.OSSProvider {
			return ossstore.NewOSSStorage(ctx, backend.S3, opts)
		}
		return s3store.NewS3Storage(ctx, backend.S3, opts)
	case *backuppb.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backuppb.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GCS config not found")
		}
		return NewGCSStorage(ctx, backend.Gcs, opts)
	case *backuppb.StorageBackend_AzureBlobStorage:
		return newAzureBlobStorage(ctx, backend.AzureBlobStorage, opts)
	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "storage %T is not supported yet", backend)
	}
}

// GetDefaultHTTPClient Different from `http.DefaultTransport`, set the
// `MaxIdleConns` and `MaxIdleConnsPerHost` to the actual request concurrency to
// reuse tcp connection as much as possible.
func GetDefaultHTTPClient(concurrency uint) *http.Client {
	transport, _ := CloneDefaultHTTPTransport()
	transport.MaxIdleConns = int(concurrency)
	transport.MaxIdleConnsPerHost = int(concurrency)
	return &http.Client{
		Transport: transport,
	}
}

// CloneDefaultHTTPTransport close config.
func CloneDefaultHTTPTransport() (*http.Transport, bool) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	return transport.Clone(), ok
}

// ReadDataInRange reads data from storage in range [start, start+len(p)).
func ReadDataInRange(
	ctx context.Context,
	storage storeapi.Storage,
	name string,
	start int64,
	p []byte,
) (n int, err error) {
	// Sanity check: reject obviously invalid offsets
	if start < 0 {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid negative start offset: %d", start)
	}
	end := start + int64(len(p))
	// Detect overflow: if end wrapped around to negative, overflow occurred
	if end < start {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"range calculation overflow: start=%d, len=%d", start, len(p))
	}
	rd, err := storage.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &start,
		EndOffset:   &end,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		err := rd.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("failed to close reader", zap.Error(err))
		}
	}()
	return io.ReadFull(rd, p)
}
