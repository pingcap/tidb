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
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type extStorage struct {
	opts     *storeapi.Options
	backend  *backuppb.StorageBackend
	basePath string

	mu      sync.Mutex
	storage storeapi.Storage
}

func (w *extStorage) getStorage(ctx context.Context) (storeapi.Storage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		return w.storage, nil
	}

	var err error
	w.storage, err = objstore.New(ctx, w.backend, w.opts)
	if err != nil {
		logutil.BgLogger().Warn("failed to initialize external storage, will retry on next request", zap.Error(err))
		return nil, err
	}
	return w.storage, nil
}

func (w *extStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.WriteFile(ctx, name, data)
}

func (w *extStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.ReadFile(ctx, name)
}

func (w *extStorage) FileExists(ctx context.Context, name string) (bool, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return false, err
	}
	return s.FileExists(ctx, name)
}

func (w *extStorage) Open(ctx context.Context, path string, option *storeapi.ReaderOption) (objectio.Reader, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.Open(ctx, path, option)
}

func (w *extStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.WalkDir(ctx, opt, fn)
}

func (w *extStorage) Create(ctx context.Context, path string, option *storeapi.WriterOption) (objectio.Writer, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.Create(ctx, path, option)
}

func (w *extStorage) DeleteFile(ctx context.Context, name string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.DeleteFile(ctx, name)
}

func (w *extStorage) DeleteFiles(ctx context.Context, names []string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.DeleteFiles(ctx, names)
}

func (w *extStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.Rename(ctx, oldFileName, newFileName)
}

func (w *extStorage) URI() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		return w.storage.URI()
	}
	return ""
}

func (w *extStorage) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		w.storage.Close()
		w.storage = nil
	}
}

// GetBasePath returns the base path of the storage.
func (w *extStorage) GetBasePath() string {
	return w.basePath
}

type fileWriter struct {
	ctx    context.Context
	writer objectio.Writer
}

// NewFileWriter creates a new io.WriteCloser from storeapi.FileWriter.
func NewFileWriter(ctx context.Context, writer objectio.Writer) io.WriteCloser {
	return &fileWriter{ctx: ctx, writer: writer}
}

func (w *fileWriter) Write(p []byte) (int, error) {
	return w.writer.Write(w.ctx, p)
}

func (w *fileWriter) Close() error {
	return w.writer.Close(w.ctx)
}

// NewExtStorage creates a new external storage instance.
func NewExtStorage(rawURL, namespace string, opts *storeapi.Options) (storeapi.Storage, error) {
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

	return &extStorage{
		backend:  backend,
		opts:     opts,
		basePath: u.Path,
	}, nil
}

var (
	globalExtStorage   storeapi.Storage
	globalExtStorageMu sync.Mutex
)

// SetGlobalExtStorage sets the global external storage instance.
func SetGlobalExtStorage(storage storeapi.Storage) {
	globalExtStorageMu.Lock()
	defer globalExtStorageMu.Unlock()
	globalExtStorage = storage
}

// GetGlobalExtStorage returns the global external storage instance.
// If the storage is nil, it will create a new storage based on the cloud_storage_uri system variable.
// If cloud_storage_uri is empty, it will create a default local storage using a temporary directory.
func GetGlobalExtStorage() storeapi.Storage {
	globalExtStorageMu.Lock()
	defer globalExtStorageMu.Unlock()

	if globalExtStorage != nil {
		return globalExtStorage
	}

	// Try to create storage from cloud_storage_uri system variable
	cloudStorageURI := vardef.CloudStorageURI.Load()
	if cloudStorageURI != "" {
		storage, err := NewExtStorage(cloudStorageURI, "", nil)
		if err != nil {
			logutil.BgLogger().Warn("failed to create global extstore from cloud_storage_uri, using default local storage",
				zap.String("category", "extstore"),
				zap.String("cloud_storage_uri", cloudStorageURI),
				zap.Error(err))
		} else {
			globalExtStorage = storage
			return globalExtStorage
		}
	}

	// Create default local storage using temporary directory
	tempDir := os.TempDir()
	defaultPath := filepath.Join(tempDir, "tidb_extstore")
	storage, err := NewExtStorage("file://"+defaultPath, "", nil)
	if err != nil {
		logutil.BgLogger().Error("failed to create default local extstore",
			zap.String("category", "extstore"),
			zap.String("path", defaultPath),
			zap.Error(err))
		// Return nil if we can't create storage, but this should rarely happen
		return nil
	}
	globalExtStorage = storage
	return globalExtStorage
}
