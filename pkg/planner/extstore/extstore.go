// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	globalExtStorage   storeapi.Storage
	globalExtStorageMu sync.Mutex
)

// GetGlobalExtStorage returns the global external storage instance.
// If the storage is not initialized, it will be created automatically.
func GetGlobalExtStorage(ctx context.Context) (storeapi.Storage, error) {
	globalExtStorageMu.Lock()
	defer globalExtStorageMu.Unlock()

	if globalExtStorage == nil {
		storage, err := createGlobalExtStorage(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		globalExtStorage = storage
	}
	return globalExtStorage, nil
}

func createGlobalExtStorage(ctx context.Context) (storeapi.Storage, error) {
	uri := vardef.CloudStorageURI.Load()
	if uri == "" {
		logutil.BgLogger().Warn("cloud storage uri is empty, using default local storage",
			zap.String("category", "extstore"))
		uri = fmt.Sprintf("file://%s", os.TempDir())
	}
	keyspaceName := keyspace.GetKeyspaceNameBySettings()

	storage, err := NewExtStorage(ctx, uri, keyspaceName)
	if err != nil {
		logutil.BgLogger().Warn("failed to create global ext storage",
			zap.String("category", "extstore"),
			zap.String("uri", uri),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	logutil.BgLogger().Info("initialized global ext storage",
		zap.String("category", "extstore"),
		zap.String("uri", uri),
		zap.String("storage", storage.URI()))
	return storage, nil
}

// SetGlobalExtStorageForTest sets the global external storage instance for testing.
// This function should only be used in tests.
func SetGlobalExtStorageForTest(storage storeapi.Storage) {
	globalExtStorageMu.Lock()
	defer globalExtStorageMu.Unlock()
	globalExtStorage = storage
}

// NewExtStorage creates a new external storage instance.
func NewExtStorage(ctx context.Context, rawURL, namespace string) (storeapi.Storage, error) {
	u, err := objstore.ParseRawURL(rawURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if namespace != "" {
		u.Path = filepath.Join(u.Path, namespace)
	}

	backend, err := objstore.ParseBackendFromURL(u, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	storage, err := objstore.New(ctx, backend, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return storage, nil
}
