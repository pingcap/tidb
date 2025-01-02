// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func init() {
	// initialize for ut
	url := config.GetGlobalConfig().TempDir
	CreateExternalStorage(url, "", nil)
}

var globalExternalStorage *externalStorageWrapper

type externalStorageWrapper struct {
	// use to initialize external storage
	opts    *storage.ExternalStorageOptions
	backend *backuppb.StorageBackend

	mu       sync.Mutex
	storage  storage.ExternalStorage
	basePath string
}

// WriteFile will call `storage.WriteFile`.
func (w *externalStorageWrapper) WriteFile(ctx context.Context, name string, data []byte) error {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return err
	}
	return w.storage.WriteFile(ctx, name, data)
}

// ReadFile will call `storage.ReadFile`.
func (w *externalStorageWrapper) ReadFile(ctx context.Context, name string) ([]byte, error) {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return nil, err
	}
	return w.storage.ReadFile(ctx, name)
}

// FileExists will call `storage.FileExists`.
func (w *externalStorageWrapper) FileExists(ctx context.Context, name string) (bool, error) {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return false, err
	}
	return w.storage.FileExists(ctx, name)
}

// DeleteFile will call `storage.DeleteFile`.
func (w *externalStorageWrapper) DeleteFile(ctx context.Context, name string) error {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return err
	}
	return w.storage.DeleteFile(ctx, name)
}

// Open will call `storage.Open`.
func (w *externalStorageWrapper) Open(ctx context.Context, path string, option *storage.ReaderOption) (storage.ExternalFileReader, error) {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return nil, err
	}
	return w.storage.Open(ctx, path, nil)
}

// DeleteFile will call `storage.DeleteFiles`.
func (w *externalStorageWrapper) DeleteFiles(ctx context.Context, names []string) error {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return err
	}
	return w.storage.DeleteFiles(ctx, names)
}

// WaklDir will call `storage.WalkDir`.
func (w *externalStorageWrapper) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(path string, size int64) error) error {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return err
	}
	return w.storage.WalkDir(ctx, opt, fn)
}

// Create will call `storage.Create`.
func (w *externalStorageWrapper) Create(ctx context.Context, path string, option *storage.WriterOption) (storage.ExternalFileWriter, error) {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return nil, err
	}
	return w.storage.Create(ctx, path, option)
}

// Rename will call `storage.Rename`.
func (w *externalStorageWrapper) Rename(ctx context.Context, oldFileName, newFileName string) error {
	err := w.ensureStorageInitialized(ctx)
	if err != nil {
		return err
	}
	return w.storage.Rename(ctx, oldFileName, newFileName)
}

// URI will call `storage.URI`.
func (w *externalStorageWrapper) URI() string {
	if w.storage != nil {
		return w.storage.URI()
	}
	return ""
}

// Close will call `storage.Close` and set `w.storage` to nil if `w.storage` is initialized.
func (w *externalStorageWrapper) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.storage == nil {
		return
	}

	w.storage.Close()
	w.storage = nil
}

func (w *externalStorageWrapper) ensureStorageInitialized(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.storage != nil {
		return nil
	}

	var err error
	w.storage, err = storage.New(ctx, w.backend, w.opts)
	if err != nil {
		w.storage = nil
		logutil.BgLogger().Error("failed to initialize external storage", zap.Error(err))
	}
	return err
}

type fileWriterWrapper struct {
	ctx    context.Context
	writer storage.ExternalFileWriter
}

// NewExternalFileWriterWrap return a `storage.ExternalFileWriter` wrap.
func NewExternalFileWriterWrap(ctx context.Context, writer storage.ExternalFileWriter) io.WriteCloser {
	return &fileWriterWrapper{
		ctx:    ctx,
		writer: writer,
	}
}

// Write will call `storage.ExternalFileWriter.Write`.
func (w *fileWriterWrapper) Write(p []byte) (int, error) {
	return w.writer.Write(w.ctx, p)
}

// Close will call `storage.ExternalFileWriter.Close`.
func (w *fileWriterWrapper) Close() error {
	return w.writer.Close(w.ctx)
}

// CreateExternalStorage create a `externalStorage` using rawURL without initialization.
// The rawURL specifies the path to the data storage and parameters,
// see https://docs.pingcap.com/tidb/dev/external-storage-uri for more details.
// If namespace is set, data storage path will be isolated based on namespace.
func CreateExternalStorage(rawURL, namespace string, opts *storage.ExternalStorageOptions) error {
	url, err := storage.ParseRawURL(rawURL)
	if err != nil {
		return errors.AddStack(err)
	}

	if len(namespace) != 0 {
		url.Path = filepath.Join(url.Path, namespace)
	}

	backend, err := storage.ParseBackendFromURL(url, nil)
	if err != nil {
		return errors.AddStack(err)
	}

	globalExternalStorage = &externalStorageWrapper{
		backend: backend,
		opts:    opts,
		mu:      sync.Mutex{},
		// lazy init
		storage:  nil,
		basePath: url.Path,
	}
	return nil
}

// GetExternalStorage return a initialized `externalStorage`
func GetExternalStorage() storage.ExternalStorage {
	return globalExternalStorage
}

// GetBasePath return base path using by `externalStorage`
func GetBasePath() string {
	return globalExternalStorage.basePath
}
