// Copyright 2020 PingCAP, Inc.
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

package common_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestGetStorageSize(t *testing.T) {
	// only ensure we can get storage size.
	d := t.TempDir()
	size, err := common.GetStorageSize(d)
	require.NoError(t, err)
	require.Greater(t, size.Capacity, uint64(0))
	require.Greater(t, size.Available, uint64(0))
}

func TestCountFilesAndSize(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		dir := t.TempDir()
		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 0, count)
		require.Equal(t, 0, size)
	})

	t.Run("single file", func(t *testing.T) {
		dir := t.TempDir()
		content := []byte("hello world")
		err := os.WriteFile(filepath.Join(dir, "file.txt"), content, 0644)
		require.NoError(t, err)

		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 1, count)
		require.Equal(t, len(content), size)
	})

	t.Run("multiple files", func(t *testing.T) {
		dir := t.TempDir()
		content1 := []byte("hello")
		content2 := []byte("lightning")
		content3 := []byte("world")

		err := os.WriteFile(filepath.Join(dir, "file1.txt"), content1, 0644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(dir, "file2.txt"), content2, 0644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(dir, "file3.txt"), content3, 0644)
		require.NoError(t, err)

		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 3, count)
		require.Equal(t, len(content1)+len(content2)+len(content3), size)
	})

	t.Run("nested directories", func(t *testing.T) {
		dir := t.TempDir()
		subdir := filepath.Join(dir, "subdir")
		err := os.Mkdir(subdir, 0755)
		require.NoError(t, err)

		content1 := []byte("top level")
		content2 := []byte("nested file")

		err = os.WriteFile(filepath.Join(dir, "file1.txt"), content1, 0644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(subdir, "file2.txt"), content2, 0644)
		require.NoError(t, err)

		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 2, count)
		require.Equal(t, len(content1)+len(content2), size)
	})

	t.Run("non-existent directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "doesnotexist")
		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 0, count)
		require.Equal(t, 0, size)
	})

	t.Run("permission denied", func(t *testing.T) {
		if os.Geteuid() == 0 {
			// Skip this test when running as root
			t.Skip("Skipping permission test when running as root")
		}

		dir := t.TempDir()
		subdir := filepath.Join(dir, "noaccess")
		err := os.Mkdir(subdir, 0755)
		require.NoError(t, err)

		// Create a file in the directory before changing permissions
		err = os.WriteFile(filepath.Join(dir, "accessible.txt"), []byte("can access"), 0644)
		require.NoError(t, err)

		// Make subdirectory inaccessible
		err = os.Chmod(subdir, 0)
		require.NoError(t, err)
		defer os.Chmod(subdir, 0755) // Restore permissions for cleanup

		// This should still count the accessible file but skip the inaccessible directory
		count, size := common.CountFilesAndSize(dir)
		require.Equal(t, 1, count)
		require.Equal(t, len("can access"), size)
	})
}
