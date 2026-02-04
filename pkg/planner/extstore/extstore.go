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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/afero"
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
	keyspaceName := keyspace.GetKeyspaceNameBySettings()
	uri := vardef.CloudStorageURI.Load()

	// When classic kernel or cloud storage URI is not set, use local directory.
	if kerneltype.IsClassic() || uri == "" {
		localPath := getLocalPathDirName()
		logutil.BgLogger().Warn("using default local storage",
			zap.String("category", "extstore"),
			zap.String("localPath", localPath),
			zap.String("keyspaceName", keyspaceName))
		uri = fmt.Sprintf("file://%s", localPath)
	}

	storage, err := NewExtStorage(ctx, uri, keyspaceName)
	if err != nil {
		// Use ast.RedactURL to hide sensitive info (AK/SK) in logs
		logutil.BgLogger().Warn("failed to create global ext storage",
			zap.String("category", "extstore"),
			zap.String("uri", ast.RedactURL(uri)),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	// Only log the storage.URI() which typically doesn't contain credentials
	logutil.BgLogger().Info("initialized global ext storage",
		zap.String("category", "extstore"),
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

func getLocalPathDirName(vfs ...afero.Fs) string {
	var fs afero.Fs
	fs = afero.NewOsFs()
	if vfs != nil {
		fs = vfs[0]
	}
	tidbLogDir := filepath.Dir(config.GetGlobalConfig().Log.File.Filename)
	tidbLogDir = filepath.Clean(tidbLogDir)
	if canWriteToFile(fs, tidbLogDir) {
		logutil.BgLogger().Info("use log dir as local path", zap.String("dir", tidbLogDir))
		return tidbLogDir
	}
	tempDir := config.GetGlobalConfig().TempDir
	logutil.BgLogger().Info("use temp dir as local path", zap.String("dir", tempDir))
	return tempDir
}

func canWriteToFile(vfs afero.Fs, path string) bool {
	now := time.Now()
	timeStr := now.Format("20060102150405")
	filename := fmt.Sprintf("test_%s.txt", timeStr)
	path = filepath.Join(path, filename)
	if !canWriteToFileInternal(vfs, path) {
		logutil.BgLogger().Warn("cannot write to file", zap.String("path", path))
		return false
	}
	return true
}

func canWriteToFileInternal(vfs afero.Fs, path string) bool {
	// Open the file in write mode
	file, err := vfs.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return false
	}
	defer func() {
		err = file.Close()
		intest.Assert(err == nil, "failed to close file")
		if err == nil {
			err = vfs.Remove(path)
			intest.Assert(err == nil, "failed to delete file")
		}
	}()
	// Try to write a single byte to the file
	_, err = file.Write([]byte{0})
	return err == nil
}
