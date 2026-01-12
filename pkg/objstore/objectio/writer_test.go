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

package objectio_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/stretchr/testify/require"
)

func getStore(t *testing.T, uri string, changeStoreFn func(s objstore.Storage) objstore.Storage) objstore.Storage {
	t.Helper()
	backend, err := objstore.ParseBackend(uri, nil)
	require.NoError(t, err)
	ctx := context.Background()
	storage, err := objstore.Create(ctx, backend, true)
	require.NoError(t, err)
	return changeStoreFn(storage)
}

func writeFile(t *testing.T, storage objstore.Storage, fileName string, lines []string) {
	t.Helper()
	ctx := context.Background()
	writer, err := storage.Create(ctx, fileName, nil)
	require.NoError(t, err)
	for _, str := range lines {
		p := []byte(str)
		written, err2 := writer.Write(ctx, p)
		require.Nil(t, err2)
		require.Len(t, p, written)
	}
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestExternalFileWriter(t *testing.T) {
	dir := t.TempDir()

	type testcase struct {
		name    string
		content []string
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		storage := getStore(t, "local://"+filepath.ToSlash(dir), func(s objstore.Storage) objstore.Storage {
			return s
		})
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt"
		writeFile(t, storage, fileName, test.content)
		content, err := os.ReadFile(filepath.Join(dir, fileName))
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), string(content))
	}
	tests := []testcase{
		{
			name:    "short and sweet",
			content: []string{"hi"},
		},
		{
			name: "long text small chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
		},
		{
			name: "long text medium chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
		},
		{
			name: "long text large chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestCompressReaderWriter(t *testing.T) {
	dir := t.TempDir()

	type testcase struct {
		name         string
		content      []string
		compressType compressedio.CompressType
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		suffix := test.compressType.FileSuffix()
		fileName := strings.ReplaceAll(test.name, " ", "-") + suffix
		storage := getStore(t, "local://"+filepath.ToSlash(dir), func(s objstore.Storage) objstore.Storage {
			return objstore.WithCompression(s, test.compressType, compressedio.DecompressConfig{})
		})
		writeFile(t, storage, fileName, test.content)

		// make sure compressed file is written correctly
		file, err := os.Open(filepath.Join(dir, fileName))
		require.NoError(t, err)
		r, err := compressedio.NewReader(test.compressType, compressedio.DecompressConfig{}, file)
		require.NoError(t, err)
		var bf bytes.Buffer
		_, err = bf.ReadFrom(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), bf.String())

		// test withCompression Open
		ctx := context.Background()
		r, err = storage.Open(ctx, fileName, nil)
		require.NoError(t, err)
		content, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), string(content))

		require.Nil(t, file.Close())
	}
	compressTypeArr := []compressedio.CompressType{compressedio.Gzip, compressedio.Snappy, compressedio.Zstd}

	tests := []testcase{
		{
			name: "long text medium chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
		},
		{
			name: "long text large chunks",
			content: []string{
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
				"hello world",
			},
		},
	}
	for i := range tests {
		for _, compressType := range compressTypeArr {
			tests[i].compressType = compressType
			testFn(&tests[i], t)
		}
	}
}

func TestNewCompressReader(t *testing.T) {
	var buf bytes.Buffer
	var w io.WriteCloser
	var err error
	w, err = zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	compressedData := buf.Bytes()

	// default cfg(decode asynchronously)
	prevRoutineCnt := runtime.NumGoroutine()
	r, err := compressedio.NewReader(compressedio.Zstd, compressedio.DecompressConfig{}, bytes.NewReader(compressedData))
	currRoutineCnt := runtime.NumGoroutine()
	require.NoError(t, err)
	require.Greater(t, currRoutineCnt, prevRoutineCnt)
	allData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "data", string(allData))

	// sync decode
	prevRoutineCnt = runtime.NumGoroutine()
	config := compressedio.DecompressConfig{ZStdDecodeConcurrency: 1}
	r, err = compressedio.NewReader(compressedio.Zstd, config, bytes.NewReader(compressedData))
	require.NoError(t, err)
	currRoutineCnt = runtime.NumGoroutine()
	require.Equal(t, prevRoutineCnt, currRoutineCnt)
	allData, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "data", string(allData))
}
