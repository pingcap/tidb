// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		writer, err := storage.Create(ctx, fileName)
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
		storage = WithCompression(storage, Gzip)
		fileName := strings.ReplaceAll(test.name, " ", "-") + ".txt.gz"
		writer, err := storage.Create(ctx, fileName)
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
		r, err := newCompressReader(test.compressType, file)
		require.NoError(t, err)
		var bf bytes.Buffer
		_, err = bf.ReadFrom(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), bf.String())
		require.Nil(t, r.Close())

		// test withCompression Open
		r, err = storage.Open(ctx, fileName)
		require.NoError(t, err)
		content, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, strings.Join(test.content, ""), string(content))

		require.Nil(t, file.Close())
	}
	compressTypeArr := []CompressType{Gzip}
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
