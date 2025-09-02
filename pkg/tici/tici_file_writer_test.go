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

package tici

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// mockExternalFileWriter implements storage.ExternalFileWriter for testing.
type mockExternalFileWriter struct {
	writes [][]byte
	closed bool
	fail   bool
}

func (m *mockExternalFileWriter) Write(ctx context.Context, data []byte) (int, error) {
	if m.fail {
		return 0, errors.New("write failed")
	}
	m.writes = append(m.writes, append([]byte{}, data...))
	return len(data), nil
}
func (m *mockExternalFileWriter) Close(ctx context.Context) error {
	if m.fail {
		return errors.New("close failed")
	}
	m.closed = true
	return nil
}

// mockExternalStorage implements storage.ExternalStorage for testing.
type mockExternalStorage struct {
	writer *mockExternalFileWriter
	fail   bool
}

func (m *mockExternalStorage) Create(ctx context.Context, name string, opt *storage.WriterOption) (storage.ExternalFileWriter, error) {
	if m.fail {
		return nil, errors.New("create failed")
	}
	return m.writer, nil
}

// Only implement Create for this test
func (m *mockExternalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	return nil
}
func (m *mockExternalStorage) Open(ctx context.Context, name string, opts *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return nil, nil
}
func (m *mockExternalStorage) Delete(ctx context.Context, name string) error { return nil }
func (m *mockExternalStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return false, nil
}
func (m *mockExternalStorage) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockExternalStorage) URI() string { return "" }
func (m *mockExternalStorage) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(string, int64) error) error {
	return nil
}
func (m *mockExternalStorage) OpenWithRange(ctx context.Context, name string, offset, length int64) (storage.ExternalFileReader, error) {
	return nil, nil
}
func (m *mockExternalStorage) Rename(ctx context.Context, oldName, newName string) error { return nil }
func (m *mockExternalStorage) CreateWithFlag(ctx context.Context, name string, opt *storage.WriterOption) (storage.ExternalFileWriter, error) {
	return m.Create(ctx, name, opt)
}

// Implement DeleteFile to satisfy storage.ExternalStorage interface
func (m *mockExternalStorage) DeleteFile(ctx context.Context, name string) error {
	return nil
}

// Implement ReadFile to satisfy storage.ExternalStorage interface
func (m *mockExternalStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return nil, nil
}

// Add Close method to satisfy storage.ExternalStorage interface
func (m *mockExternalStorage) Close() {
	// No-op for mock
}

// Implement DeleteFiles to satisfy storage.ExternalStorage interface
func (m *mockExternalStorage) DeleteFiles(ctx context.Context, names []string) error {
	return nil
}

func TestNewTICIFileWriterAndWriteRow(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	assert.NotNil(t, tfw)

	key := []byte("key")
	val := []byte("value")
	err = tfw.WriteRow(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(writer.writes))
	assert.Equal(t, tfw.totalCnt, uint64(1))
	assert.Equal(t, tfw.totalSize, uint64(len(key)+len(val)))
}

func TestWriteRow_AllocFail(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	// Simulate buffer allocation failure by replacing kvBuffer with a buffer with zero memory limit
	tfw.kvBuffer = membuf.NewPool(membuf.WithBlockNum(0), membuf.WithBlockSize(1)).NewBuffer(membuf.WithBufferMemoryLimit(0))
	err = tfw.WriteRow(ctx, []byte("k"), []byte("v"))
	assert.Error(t, err)
}

func TestWriteRow_WriteFail(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{fail: true}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	err = tfw.WriteRow(ctx, []byte("k"), []byte("v"))
	assert.Error(t, err)
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	err = tfw.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, writer.closed)

	// Double close
	err = tfw.Close(ctx)
	assert.Error(t, err)
}

func TestClose_Fail(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{fail: true}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	err = tfw.Close(ctx)
	assert.Error(t, err)
}

func TestWriteHeader(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)

	tbl := []byte("table")
	commitTS := uint64(time.Now().UnixNano())

	err = tfw.WriteHeader(ctx, tbl, commitTS)
	assert.NoError(t, err)
	assert.True(t, tfw.headerWritten)
	assert.Equal(t, 1, len(writer.writes))

	// Write header again should fail
	err = tfw.WriteHeader(ctx, tbl, commitTS)
	assert.Error(t, err)
}

func TestWriteHeader_NilWriter(t *testing.T) {
	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	tfw := &FileWriter{
		dataWriter:    nil,
		headerWritten: false,
		logger:        logger,
	}
	err := tfw.WriteHeader(ctx, []byte("t"), 1)
	assert.Error(t, err)
}

func TestWriteHeader_WriteFail(t *testing.T) {
	ctx := context.Background()
	writer := &mockExternalFileWriter{fail: true}
	store := &mockExternalStorage{writer: writer}
	logger := zaptest.NewLogger(t)

	tfw, err := NewTICIFileWriter(ctx, store, "testfile", 5*1024*1024, logger)
	assert.NoError(t, err)
	err = tfw.WriteHeader(ctx, []byte("t"), 1)
	assert.Error(t, err)
}

func TestEncodeKVForTICI(t *testing.T) {
	key := []byte("abc")
	val := []byte("defg")
	buf := make([]byte, lengthBytes*2+len(key)+len(val))
	encodeKVForTICI(buf, key, val)
	// Check keyLen and valLen
	keyLen := int(bytesToUint64(buf[:lengthBytes]))
	valLen := int(bytesToUint64(buf[lengthBytes : lengthBytes*2]))
	assert.Equal(t, len(key), keyLen)
	assert.Equal(t, len(val), valLen)
	assert.Equal(t, key, buf[lengthBytes*2:lengthBytes*2+keyLen])
	assert.Equal(t, val, buf[lengthBytes*2+keyLen:])
}

// Helper to decode uint64 from bytes
func bytesToUint64(b []byte) uint64 {
	var u uint64
	for i := 0; i < 8; i++ {
		u = u<<8 | uint64(b[i])
	}
	return u
}
