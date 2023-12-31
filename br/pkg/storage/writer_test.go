// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

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
	"github.com/stretchr/testify/require"
)

func TestExternalFileWriter(t *testing.T) {
	dir := t.TempDir()

	type testcase struct {
		name    string
		content []string
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
		require.NoError(t, err)
		ctx := context.Background()
		storage, err := Create(ctx, backend, true)
		require.NoError(t, err)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt"
		writer, err := storage.Create(ctx, fileName, nil)
		require.NoError(t, err)
		for _, str := range test.content {
			p := []byte(str)
			written, err2 := writer.Write(ctx, p)
			require.Nil(t, err2)
			require.Len(t, p, written)
		}
		err = writer.Close(ctx)
		require.NoError(t, err)
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
		compressType CompressType
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
		require.NoError(t, err)
		ctx := context.Background()
		storage, err := Create(ctx, backend, true)
		require.NoError(t, err)
		storage = WithCompression(storage, test.compressType, DecompressConfig{})
		suffix := createSuffixString(test.compressType)
		fileName := strings.ReplaceAll(test.name, " ", "-") + suffix
		writer, err := storage.Create(ctx, fileName, nil)
		require.NoError(t, err)
		for _, str := range test.content {
			p := []byte(str)
			written, err2 := writer.Write(ctx, p)
			require.Nil(t, err2)
			require.Len(t, p, written)
		}
		err = writer.Close(ctx)
		require.NoError(t, err)

		// make sure compressed file is written correctly
		file, err := os.Open(filepath.Join(dir, fileName))
		require.NoError(t, err)
		r, err := newCompressReader(test.compressType, DecompressConfig{}, file)
		require.NoError(t, err)
		var bf bytes.Buffer
		_, err = bf.ReadFrom(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), bf.String())

		// test withCompression Open
		r, err = storage.Open(ctx, fileName, nil)
		require.NoError(t, err)
		content, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), string(content))

		require.Nil(t, file.Close())
	}
	compressTypeArr := []CompressType{Gzip, Snappy, Zstd}

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
	r, err := newCompressReader(Zstd, DecompressConfig{}, bytes.NewReader(compressedData))
	currRoutineCnt := runtime.NumGoroutine()
	require.NoError(t, err)
	require.Greater(t, currRoutineCnt, prevRoutineCnt)
	allData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "data", string(allData))

	// sync decode
	prevRoutineCnt = runtime.NumGoroutine()
	config := DecompressConfig{ZStdDecodeConcurrency: 1}
	r, err = newCompressReader(Zstd, config, bytes.NewReader(compressedData))
	require.NoError(t, err)
	currRoutineCnt = runtime.NumGoroutine()
	require.Equal(t, prevRoutineCnt, currRoutineCnt)
	allData, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "data", string(allData))
}
