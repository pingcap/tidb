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

	"github.com/pingcap/tidb/pkg/objstore/objectio"
)

type noopStorage struct{}

// DeleteFile delete the file in storage
func (*noopStorage) DeleteFile(_ context.Context, _ string) error {
	return nil
}

// DeleteFiles deletes the files in storage
func (*noopStorage) DeleteFiles(_ context.Context, _ []string) error {
	return nil
}

// WriteFile file to storage.
func (*noopStorage) WriteFile(_ context.Context, _ string, _ []byte) error {
	return nil
}

// ReadFile storage file.
func (*noopStorage) ReadFile(_ context.Context, _ string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists.
func (*noopStorage) FileExists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// Open a Reader by file path.
func (*noopStorage) Open(_ context.Context, _ string, _ *ReaderOption) (objectio.Reader, error) {
	return noopReader{}, nil
}

// WalkDir traverse all the files in a dir.
func (*noopStorage) WalkDir(_ context.Context, _ *WalkOption, _ func(string, int64) error) error {
	return nil
}

func (*noopStorage) URI() string {
	return "noop:///"
}

// Create implements Storage interface.
func (*noopStorage) Create(_ context.Context, _ string, _ *WriterOption) (objectio.Writer, error) {
	return &NoopWriter{}, nil
}

// Rename implements Storage interface.
func (*noopStorage) Rename(_ context.Context, _, _ string) error {
	return nil
}

// Close implements Storage interface.
func (*noopStorage) Close() {}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}

type noopReader struct{}

func (noopReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}

func (noopReader) Close() error {
	return nil
}

func (noopReader) Seek(offset int64, _ int) (int64, error) {
	return offset, nil
}

func (noopReader) GetFileSize() (int64, error) {
	return 0, nil
}

// NoopWriter is a writer that does nothing.
type NoopWriter struct{}

// Write implements objectio.Writer interface.
func (NoopWriter) Write(_ context.Context, p []byte) (int, error) {
	return len(p), nil
}

// Close implements objectio.Writer interface.
func (NoopWriter) Close(_ context.Context) error {
	return nil
}
