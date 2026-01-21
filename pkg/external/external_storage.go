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

package external

import (
	"context"
	"io"
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func init() {
	if tempDir := config.GetGlobalConfig().TempDir; tempDir != "" {
		_ = CreateExternalStorage(tempDir, "", nil)
	}
}

var (
	globalMu              sync.RWMutex
	globalExternalStorage *externalStorage
)

type externalStorage struct {
	opts     *storeapi.Options
	backend  *backuppb.StorageBackend
	basePath string

	mu      sync.Mutex
	storage storeapi.Storage
}

func (w *externalStorage) getStorage(ctx context.Context) (storeapi.Storage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		return w.storage, nil
	}

	var err error
	w.storage, err = objstore.New(ctx, w.backend, w.opts)
	if err != nil {
		logutil.BgLogger().Error("failed to initialize external storage, will retry on next request", zap.Error(err))
		return nil, err
	}
	return w.storage, nil
}

func (w *externalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.WriteFile(ctx, name, data)
}

func (w *externalStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.ReadFile(ctx, name)
}

func (w *externalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return false, err
	}
	return s.FileExists(ctx, name)
}

func (w *externalStorage) Open(ctx context.Context, path string, option *storeapi.ReaderOption) (objectio.Reader, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.Open(ctx, path, option)
}

func (w *externalStorage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.WalkDir(ctx, opt, fn)
}

func (w *externalStorage) Create(ctx context.Context, path string, option *storeapi.WriterOption) (objectio.Writer, error) {
	s, err := w.getStorage(ctx)
	if err != nil {
		return nil, err
	}
	return s.Create(ctx, path, option)
}

func (w *externalStorage) DeleteFile(ctx context.Context, name string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.DeleteFile(ctx, name)
}

func (w *externalStorage) DeleteFiles(ctx context.Context, names []string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.DeleteFiles(ctx, names)
}

func (w *externalStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	s, err := w.getStorage(ctx)
	if err != nil {
		return err
	}
	return s.Rename(ctx, oldFileName, newFileName)
}

func (w *externalStorage) URI() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		return w.storage.URI()
	}
	return ""
}

func (w *externalStorage) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.storage != nil {
		w.storage.Close()
		w.storage = nil
	}
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

// CreateExternalStorage creates a global external storage.
func CreateExternalStorage(rawURL, namespace string, opts *storeapi.Options) error {
	u, err := objstore.ParseRawURL(rawURL)
	if err != nil {
		return errors.Trace(err)
	}

	if namespace != "" {
		u.Path = filepath.Join(u.Path, namespace)
	}

	backend, err := objstore.ParseBackendFromURL(u, nil)
	if err != nil {
		return errors.Trace(err)
	}

	globalMu.Lock()
	defer globalMu.Unlock()
	globalExternalStorage = &externalStorage{
		backend:  backend,
		opts:     opts,
		basePath: u.Path,
	}
	return nil
}

// GetExternalStorage returns the global external storage.
func GetExternalStorage() storeapi.Storage {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalExternalStorage
}

// GetStorageBasePath returns the base path of the global external storage.
func GetStorageBasePath() string {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if globalExternalStorage == nil {
		return ""
	}
	return globalExternalStorage.basePath
}
