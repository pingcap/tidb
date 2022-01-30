// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
)

func (r *testStorageSuite) TestWithCompressReadWriteFile(c *C) {
	dir := c.MkDir()
	backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
	c.Assert(err, IsNil)
	ctx := context.Background()
	storage, err := Create(ctx, backend, true)
	c.Assert(err, IsNil)
	storage = WithCompression(storage, Gzip)
	name := "with compress test"
	content := "hello,world!"
	fileName := strings.ReplaceAll(name, " ", "-") + ".txt.gz"
	err = storage.WriteFile(ctx, fileName, []byte(content))
	c.Assert(err, IsNil)

	// make sure compressed file is written correctly
	file, err := os.Open(filepath.Join(dir, fileName))
	c.Assert(err, IsNil)
	uncompressedFile, err := newCompressReader(Gzip, file)
	c.Assert(err, IsNil)
	newContent, err := io.ReadAll(uncompressedFile)
	c.Assert(err, IsNil)
	c.Assert(string(newContent), Equals, content)

	// test withCompression ReadFile
	newContent, err = storage.ReadFile(ctx, fileName)
	c.Assert(err, IsNil)
	c.Assert(string(newContent), Equals, content)
}
