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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestLocalDirectIO(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	for _, partSize := range []int64{0, 1, 4097, 16384} {
		for _, size := range []int{0, 1, 4095, 4096, 4097, 65537} {
			for _, chunkSize := range []int{1, 3000, 65537} {
				t.Run(fmt.Sprintf("part%d/size%d/chunk%d", partSize, size, chunkSize), func(t *testing.T) {
					w, err := store.Create(ctx, "nested/data", &storeapi.WriterOption{DirectIO: true, PartSize: partSize})
					require.NoError(t, err)
					data := make([]byte, size)
					for i := range data {
						data[i] = byte(i%251 + 1)
					}
					expected := bytes.Clone(data)
					for offset := 0; offset < len(data); offset += chunkSize {
						chunk := data[offset:min(offset+chunkSize, len(data))]
						n, err := w.Write(ctx, chunk)
						require.NoError(t, err)
						require.Equal(t, len(chunk), n)
						// The writer must own any buffered bytes after Write returns.
						clear(chunk)
					}
					require.NoError(t, w.Close(ctx))
					if direct, ok := w.(*localDirectWriter); ok {
						_, err := direct.file.Stat()
						require.ErrorIs(t, err, os.ErrClosed)
					}
					got, err := store.ReadFile(ctx, "nested/data")
					require.NoError(t, err)
					require.Equal(t, expected, got)
				})
			}
		}
	}

	for _, option := range []*storeapi.WriterOption{nil, {}, {PartSize: 8192}} {
		w, err := store.Create(ctx, "buffered", option)
		require.NoError(t, err)
		require.IsType(t, &flushStorageWriter{}, w)
		_, err = w.Write(ctx, bytes.Repeat([]byte("x"), 17))
		require.NoError(t, err)
		require.NoError(t, w.Close(ctx))
	}

	for _, compression := range []compressedio.CompressType{compressedio.Gzip, compressedio.Snappy, compressedio.Zstd} {
		t.Run(compression.FileSuffix(), func(t *testing.T) {
			compressed := WithCompression(store, compression, compressedio.DecompressConfig{})
			w, err := compressed.Create(ctx, "compressed", &storeapi.WriterOption{DirectIO: true, PartSize: 4097})
			require.NoError(t, err)
			data := bytes.Repeat([]byte("direct I/O with compression\n"), 10000)
			n, err := w.Write(ctx, data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.NoError(t, w.Close(ctx))
			got, err := compressed.ReadFile(ctx, "compressed")
			require.NoError(t, err)
			require.Equal(t, data, got)
		})
	}
}

func TestLocalDirectIOFallback(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	for _, openErr := range []error{syscall.EINVAL, syscall.EOPNOTSUPP, errors.New("Failed to set F_NOCACHE: operation not supported")} {
		t.Run(openErr.Error(), func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{}))
			const secret = "secret-access-key=do-not-log"
			const failpointName = "github.com/pingcap/tidb/pkg/objstore/openLocalDirectIO"
			require.NoError(t, failpoint.EnableCall(failpointName, func(openFile *func(string, int, os.FileMode) (*os.File, error)) {
				*openFile = func(name string, _ int, _ os.FileMode) (*os.File, error) {
					if errno, ok := openErr.(syscall.Errno); ok {
						return nil, &os.PathError{Op: "open", Path: name, Err: errno}
					}
					return nil, openErr
				}
			}))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(failpointName)) })

			name := "nested/" + secret
			require.NoError(t, store.WriteFile(ctx, name, bytes.Repeat([]byte("old data"), 10000)))
			w, err := store.Create(ctx, name, &storeapi.WriterOption{DirectIO: true, PartSize: 4097})
			require.NoError(t, err)
			require.IsType(t, &flushStorageWriter{}, w)
			data := bytes.Repeat([]byte("buffered fallback\n"), 1000)
			for offset := 0; offset < len(data); offset += 3000 {
				chunk := data[offset:min(offset+3000, len(data))]
				n, err := w.Write(ctx, chunk)
				require.NoError(t, err)
				require.Equal(t, len(chunk), n)
			}
			require.NoError(t, w.Close(ctx))
			got, err := store.ReadFile(ctx, name)
			require.NoError(t, err)
			require.Equal(t, data, got)
			require.Equal(t, 1, logs.Len())
			require.Contains(t, logs.All()[0].Message, "falling back to buffered I/O")
			require.NotContains(t, fmt.Sprint(logs.All()), secret)

			for _, compression := range []compressedio.CompressType{compressedio.Gzip, compressedio.Snappy, compressedio.Zstd} {
				compressed := WithCompression(store, compression, compressedio.DecompressConfig{})
				w, err := compressed.Create(ctx, "compressed", &storeapi.WriterOption{DirectIO: true})
				require.NoError(t, err)
				n, err := w.Write(ctx, data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)
				require.NoError(t, w.Close(ctx))
				got, err := compressed.ReadFile(ctx, "compressed")
				require.NoError(t, err)
				require.Equal(t, data, got)
			}

			// The buffered retry must still propagate real file-creation errors.
			w, err = store.Create(ctx, "nested", &storeapi.WriterOption{DirectIO: true})
			require.Error(t, err)
			require.Nil(t, w)
		})
	}
}

func TestLocalDirectIOErrors(t *testing.T) {
	ctx := context.Background()
	filename := filepath.Join(t.TempDir(), "data")
	for _, stage := range []string{"write", "close", "truncate", "buffered-close"} {
		t.Run(stage, func(t *testing.T) {
			writer, err := newLocalDirectWriter(filename, 4096)
			require.NoError(t, err)
			w, ok := writer.(*localDirectWriter)
			if !ok {
				require.NoError(t, writer.Close(ctx))
				t.Skip("filesystem does not support direct I/O")
			}
			require.NoError(t, w.file.Close())
			// A real read-only descriptor makes writes or truncation fail while
			// still allowing us to verify that the writer releases it on error.
			w.file, err = os.Open(filename)
			require.NoError(t, err)
			switch stage {
			case "write":
				n, err := w.Write(ctx, make([]byte, 4097))
				require.Equal(t, 4096, n)
				require.Error(t, err)
			case "close":
				_, err = w.Write(ctx, []byte("tail"))
				require.NoError(t, err)
				require.Error(t, w.Close(ctx))
			case "truncate":
				require.Error(t, w.Close(ctx))
			case "buffered-close":
				buffered := objectio.NewBufferedWriter(w, 4096, compressedio.NoCompression, nil)
				_, err = buffered.Write(ctx, make([]byte, 4096))
				require.NoError(t, err)
				require.Error(t, buffered.Close(ctx))
			}
			_, err = w.file.Stat()
			require.ErrorIs(t, err, os.ErrClosed)
			require.Error(t, w.err)
			require.Same(t, w.err, w.Close(ctx))
			n, err := w.Write(ctx, []byte("retry"))
			require.Zero(t, n)
			require.Same(t, w.err, err)
		})
	}

	_, err := newLocalDirectWriter(filename, int64(^uint64(0)>>1))
	require.ErrorContains(t, err, "part size is too large")
	_, err = newLocalDirectWriter(filepath.Join(filename, "missing"), 4096)
	require.Error(t, err)

	store, err := NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	w, err := store.Create(ctx, "data", &storeapi.WriterOption{DirectIO: true, PartSize: int64(^uint64(0) >> 1)})
	require.Error(t, err)
	require.True(t, w == nil, "failed Create must return a nil writer interface")
}

func TestDeleteFile(t *testing.T) {
	dir := t.TempDir()
	sb, err := ParseBackend("file://"+filepath.ToSlash(dir), &BackendOptions{})
	require.NoError(t, err)
	store, err := Create(context.TODO(), sb, true)
	require.NoError(t, err)

	name := "test_delete"
	ret, err := store.FileExists(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, false, ret)

	_, err = store.Create(context.Background(), name, nil)
	require.NoError(t, err)

	ret, err = store.FileExists(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, true, ret)

	err = store.DeleteFile(context.Background(), name)
	require.NoError(t, err)

	ret, err = store.FileExists(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, false, ret)
}

func TestWalkDirWithSoftLinkFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		// skip the test on windows. typically windows users don't have symlink permission.
		return
	}

	dir1 := t.TempDir()
	name1 := "test.warehouse.0.sql"
	path1 := filepath.Join(dir1, name1)
	f1, err := os.Create(path1)
	require.NoError(t, err)
	data := "/* whatever pragmas */;" +
		"INSERT INTO `namespaced`.`table` (columns, more, columns) VALUES (1,-2, 3),\n(4,5., 6);" +
		"INSERT `namespaced`.`table` (x,y,z) VALUES (7,8,9);" +
		"insert another_table values (10,11e1,12, '(13)', '(', 14, ')');"
	_, err = f1.Write([]byte(data))
	require.NoError(t, err)
	err = f1.Close()
	require.NoError(t, err)

	dir2 := t.TempDir()
	name2 := "test.warehouse.1.sql"
	f2, err := os.Create(filepath.Join(dir2, name2))
	require.NoError(t, err)
	_, err = f2.Write([]byte(data))
	require.NoError(t, err)
	err = f2.Close()
	require.NoError(t, err)

	err = os.Symlink(path1, filepath.Join(dir2, name1))
	require.NoError(t, err)

	sb, err := ParseBackend("file://"+filepath.ToSlash(dir2), &BackendOptions{})
	require.NoError(t, err)

	store, err := Create(context.TODO(), sb, true)
	require.NoError(t, err)

	i := 0
	names := []string{name1, name2}
	err = store.WalkDir(context.TODO(), &storeapi.WalkOption{}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		require.Equal(t, int64(len(data)), size)
		i++

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)

	names = []string{name2}
	i = 0
	err = store.WalkDir(context.TODO(), &storeapi.WalkOption{ObjPrefix: "test.warehouse.1"}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		require.Equal(t, int64(len(data)), size)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, i)

	// test file not exists
	exists, err := store.FileExists(context.TODO(), "/123/456")
	require.NoError(t, err)
	require.False(t, exists)

	// test walk nonexistent directory
	err = store.WalkDir(context.TODO(), &storeapi.WalkOption{SubDir: "123/456"}, func(path string, size int64) error {
		return errors.New("find file")
	})
	require.NoError(t, err)
	// write file to a nonexistent directory
	err = store.WriteFile(context.TODO(), "/123/456/789.txt", []byte(data))
	require.NoError(t, err)
	exists, err = store.FileExists(context.TODO(), "/123/456")
	require.NoError(t, err)
	require.True(t, exists)

	// test walk existent directory
	err = store.WalkDir(context.TODO(), &storeapi.WalkOption{SubDir: "123/456"}, func(path string, size int64) error {
		if path == "123/456/789.txt" {
			return nil
		}
		return errors.Errorf("find other file: %s", path)
	})
	require.NoError(t, err)
}

func TestWalkDirSkipSubDir(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.WriteFile(path.Join(tempDir, "test1.txt"), []byte("test1"), 0o644))
	require.NoError(t, os.MkdirAll(path.Join(tempDir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "sub", "test2.txt"), []byte("test2"), 0o644))

	sb, err := ParseBackend(tempDir, &BackendOptions{})
	require.NoError(t, err)
	store, err := Create(context.TODO(), sb, true)
	require.NoError(t, err)
	names := []string{"sub/test2.txt", "test1.txt"}
	i := 0
	require.NoError(t, store.WalkDir(context.Background(), &storeapi.WalkOption{}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		i++
		return nil
	}))

	names = []string{"test1.txt"}
	i = 0
	require.NoError(t, store.WalkDir(context.Background(), &storeapi.WalkOption{SkipSubDir: true}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		i++
		return nil
	}))
}

func TestWalkDirStartAfter(t *testing.T) {
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(path.Join(tempDir, "meta"), 0o755))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "meta", "0001.meta"), []byte("1"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "meta", "0002.meta"), []byte("2"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "meta", "0003.meta"), []byte("3"), 0o644))

	sb, err := ParseBackend(tempDir, &BackendOptions{})
	require.NoError(t, err)
	store, err := Create(context.TODO(), sb, true)
	require.NoError(t, err)

	var files []string
	require.NoError(t, store.WalkDir(context.Background(), &storeapi.WalkOption{
		SubDir:     "meta",
		StartAfter: "meta/0001.meta",
	}, func(path string, size int64) error {
		files = append(files, path)
		return nil
	}))
	require.Equal(t, []string{"meta/0002.meta", "meta/0003.meta"}, files)
}

func TestLocalURI(t *testing.T) {
	ctx := context.Background()

	url := "file:///tmp/folder"
	sb, err := ParseBackend(url, &BackendOptions{})
	require.NoError(t, err)

	store, err := Create(ctx, sb, true)
	require.NoError(t, err)

	obtained := store.URI()
	require.Equal(t, url, obtained)
}

func TestLocalFileReadRange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sb, err := ParseBackend("file://"+filepath.ToSlash(dir), &BackendOptions{})
	require.NoError(t, err)
	store, err := Create(ctx, sb, true)
	require.NoError(t, err)

	name := "test_read_range"

	w, err := store.Create(ctx, name, nil)
	require.NoError(t, err)
	_, err = w.Write(ctx, []byte("0123456789"))
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))

	checkContent := func(r objectio.Reader, expected string) {
		buf := make([]byte, 10)
		n, _ := r.Read(buf)
		require.Equal(t, expected, string(buf[:n]))
		n, err = r.Read(buf)
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, io.EOF)
	}

	// [2, 6]
	start, end := int64(2), int64(6)
	r, err := store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &start,
		EndOffset:   &end,
	})
	require.NoError(t, err)
	checkContent(r, "2345")

	// full range
	r, err = store.Open(ctx, name, nil)
	require.NoError(t, err)
	checkContent(r, "0123456789")

	// [5, ...)
	start = 5
	r, err = store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &start,
	})
	require.NoError(t, err)
	checkContent(r, "56789")

	// [..., 5)
	end = 5
	r, err = store.Open(ctx, name, &storeapi.ReaderOption{
		EndOffset: &end,
	})
	require.NoError(t, err)
	checkContent(r, "01234")

	// test read into smaller buffer
	smallBuf := make([]byte, 2)
	start, end = int64(2), int64(6)
	r, err = store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &start,
		EndOffset:   &end,
	})
	require.NoError(t, err)
	n, _ := r.Read(smallBuf)
	require.Equal(t, "23", string(smallBuf[:n]))
}

func TestWalkBrokenSymLink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	err := os.Symlink(filepath.Join(dir, "non-existing-file"), filepath.Join(dir, "file-that-should-be-ignored"))
	require.NoError(t, err)

	sb, err := ParseBackend("file://"+filepath.ToSlash(dir), nil)
	require.NoError(t, err)
	store, err := New(ctx, sb, nil)
	require.NoError(t, err)

	files := map[string]int64{}
	err = store.WalkDir(ctx, nil, func(path string, size int64) error {
		files[path] = size
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"file-that-should-be-ignored": 0}, files)
}
