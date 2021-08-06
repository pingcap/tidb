// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"

	. "github.com/pingcap/check"
)

type testLocalSuite struct{}

var _ = Suite(&testLocalSuite{})

func (r *testStorageSuite) TestWalkDirWithSoftLinkFile(c *C) {
	if runtime.GOOS == "windows" {
		// skip the test on windows. typically windows users don't have symlink permission.
		return
	}

	dir1 := c.MkDir()
	name1 := "test.warehouse.0.sql"
	path1 := filepath.Join(dir1, name1)
	f1, err := os.Create(path1)
	c.Assert(err, IsNil)

	data := "/* whatever pragmas */;" +
		"INSERT INTO `namespaced`.`table` (columns, more, columns) VALUES (1,-2, 3),\n(4,5., 6);" +
		"INSERT `namespaced`.`table` (x,y,z) VALUES (7,8,9);" +
		"insert another_table values (10,11e1,12, '(13)', '(', 14, ')');"
	_, err = f1.Write([]byte(data))
	c.Assert(err, IsNil)
	err = f1.Close()
	c.Assert(err, IsNil)

	dir2 := c.MkDir()
	name2 := "test.warehouse.1.sql"
	f2, err := os.Create(filepath.Join(dir2, name2))
	c.Assert(err, IsNil)
	_, err = f2.Write([]byte(data))
	c.Assert(err, IsNil)
	err = f2.Close()
	c.Assert(err, IsNil)

	err = os.Symlink(path1, filepath.Join(dir2, name1))
	c.Assert(err, IsNil)

	sb, err := ParseBackend("file://"+filepath.ToSlash(dir2), &BackendOptions{})
	c.Assert(err, IsNil)

	store, err := Create(context.TODO(), sb, true)
	c.Assert(err, IsNil)

	i := 0
	names := []string{name1, name2}
	err = store.WalkDir(context.TODO(), &WalkOption{}, func(path string, size int64) error {
		c.Assert(path, Equals, names[i])
		c.Assert(size, Equals, int64(len(data)))
		i++

		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, 2)
}
