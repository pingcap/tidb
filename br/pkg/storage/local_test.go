// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	_, err = store.Create(context.Background(), name)
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
	err = store.WalkDir(context.TODO(), &WalkOption{}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		require.Equal(t, int64(len(data)), size)
		i++

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)

	names = []string{name2}
	i = 0
	err = store.WalkDir(context.TODO(), &WalkOption{ObjPrefix: "test.warehouse.1"}, func(path string, size int64) error {
		require.Equal(t, names[i], path)
		require.Equal(t, int64(len(data)), size)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, i)
}
