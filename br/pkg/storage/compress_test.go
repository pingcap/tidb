// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithCompressReadWriteFile(t *testing.T) {
	dir := t.TempDir()
	backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
	require.NoError(t, err)
	ctx := context.Background()
	storage, err := Create(ctx, backend, true)
	require.NoError(t, err)
	storage = WithCompression(storage, Gzip)
	name := "with compress test"
	content := "hello,world!"
	fileName := strings.ReplaceAll(name, " ", "-") + ".txt.gz"
	err = storage.WriteFile(ctx, fileName, []byte(content))
	require.NoError(t, err)

	// make sure compressed file is written correctly
	file, err := os.Open(filepath.Join(dir, fileName))
	require.NoError(t, err)
	uncompressedFile, err := newCompressReader(Gzip, file)
	require.NoError(t, err)
	newContent, err := io.ReadAll(uncompressedFile)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))

	// test withCompression ReadFile
	newContent, err = storage.ReadFile(ctx, fileName)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))
}
