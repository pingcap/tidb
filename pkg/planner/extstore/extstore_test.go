// Copyright 2026 PingCAP, Inc.
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

package extstore

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestExtStorage(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	s, err := NewExtStorage(ctx, "file://"+tempDir, "test_namespace")
	require.NoError(t, err)
	require.NotNil(t, s)

	uri := s.URI()
	require.Contains(t, uri, tempDir)
	require.Contains(t, uri, "test_namespace")

	// Test WriteFile and ReadFile
	fileName := "test_file.txt"
	fileContent := []byte("hello world")
	err = s.WriteFile(ctx, fileName, fileContent)
	require.NoError(t, err)

	readContent, err := s.ReadFile(ctx, fileName)
	require.NoError(t, err)
	require.Equal(t, fileContent, readContent)

	// Test FileExists
	exists, err := s.FileExists(ctx, fileName)
	require.NoError(t, err)
	require.True(t, exists)

	// Test WalkDir
	var foundFile bool
	err = s.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		if path == fileName {
			foundFile = true
			require.Equal(t, int64(len(fileContent)), size)
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, foundFile)

	// Test DeleteFile
	err = s.DeleteFile(ctx, fileName)
	require.NoError(t, err)

	exists, err = s.FileExists(ctx, fileName)
	require.NoError(t, err)
	require.False(t, exists)

	// Test Create, Write, Close
	writer, err := s.Create(ctx, "test_writer.txt", nil)
	require.NoError(t, err)
	_, err = writer.Write(ctx, []byte("test writer"))
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	readContent, err = s.ReadFile(ctx, "test_writer.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("test writer"), readContent)

	// Test Open
	reader, err := s.Open(ctx, "test_writer.txt", nil)
	require.NoError(t, err)
	buf := make([]byte, 11)
	_, err = reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "test writer", string(buf))
	err = reader.Close()
	require.NoError(t, err)

	// Test Rename
	err = s.Rename(ctx, "test_writer.txt", "test_writer_renamed.txt")
	require.NoError(t, err)
	exists, err = s.FileExists(ctx, "test_writer.txt")
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = s.FileExists(ctx, "test_writer_renamed.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// Test DeleteFiles
	err = s.WriteFile(ctx, "file1", []byte("1"))
	require.NoError(t, err)
	err = s.WriteFile(ctx, "file2", []byte("2"))
	require.NoError(t, err)
	err = s.DeleteFiles(ctx, []string{"file1", "file2"})
	require.NoError(t, err)
	exists, err = s.FileExists(ctx, "file1")
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = s.FileExists(ctx, "file2")
	require.NoError(t, err)
	require.False(t, exists)

	// Test URI
	uri = s.URI()
	require.Contains(t, uri, tempDir)
	require.Contains(t, uri, "test_namespace")

	// Test Close
	s.Close()
}

func TestGetLocalPathDirNameWithWritePerm(t *testing.T) {
	origLogFile := config.GetGlobalConfig().Log.File.Filename
	origTempDir := config.GetGlobalConfig().TempDir
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = origLogFile
		conf.TempDir = origTempDir
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = filepath.Join("/var/log/tidb", "tidb.log")
		conf.TempDir = filepath.Join("/tmp", "tidb")
	})

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/var/log/tidb", 0o755))
	basePathFsMem := afero.NewBasePathFs(fs, "/")

	path := getLocalPathDirName(basePathFsMem)
	require.Equal(t, "/var/log/tidb", path)
}

func TestGetLocalPathDirNameWithoutWritePerm(t *testing.T) {
	origLogFile := config.GetGlobalConfig().Log.File.Filename
	origTempDir := config.GetGlobalConfig().TempDir
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = origLogFile
		conf.TempDir = origTempDir
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = filepath.Join("/var/log/tidb", "tidb.log")
		conf.TempDir = filepath.Join("/tmp", "tidb")
	})

	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/var/log/tidb", 0o755))
	basePathFsMem := afero.NewReadOnlyFs(fs)

	path := getLocalPathDirName(basePathFsMem)
	require.Equal(t, config.GetGlobalConfig().TempDir, path)
}

func TestGetGlobalExtStorageWithWritePerm(t *testing.T) {
	ctx := context.Background()

	origLogFile := config.GetGlobalConfig().Log.File.Filename
	origTempDir := config.GetGlobalConfig().TempDir
	origCloudStorageURI := vardef.CloudStorageURI.Load()
	defer func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Log.File.Filename = origLogFile
			conf.TempDir = origTempDir
		})
		vardef.CloudStorageURI.Store(origCloudStorageURI)
		SetGlobalExtStorageForTest(nil)
	}()

	// Use writable temp dir for log
	tempDir := t.TempDir()
	logDir := filepath.Join(tempDir, "log")
	require.NoError(t, os.MkdirAll(logDir, 0o755))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = filepath.Join(logDir, "tidb.log")
		conf.TempDir = filepath.Join(tempDir, "tmp")
	})
	vardef.CloudStorageURI.Store("")
	SetGlobalExtStorageForTest(nil)

	s, err := GetGlobalExtStorage(ctx)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()

	uri := s.URI()
	require.Contains(t, uri, logDir, "storage URI should use log dir when writable")
}

func TestGetGlobalExtStorageWithoutWritePerm(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod read-only dir not reliable on Windows")
	}

	ctx := context.Background()

	origLogFile := config.GetGlobalConfig().Log.File.Filename
	origTempDir := config.GetGlobalConfig().TempDir
	origCloudStorageURI := vardef.CloudStorageURI.Load()
	defer func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Log.File.Filename = origLogFile
			conf.TempDir = origTempDir
		})
		vardef.CloudStorageURI.Store(origCloudStorageURI)
		SetGlobalExtStorageForTest(nil)
	}()

	tempDir := t.TempDir()
	readOnlyDir := filepath.Join(tempDir, "readonly")
	require.NoError(t, os.MkdirAll(readOnlyDir, 0o755))
	require.NoError(t, os.Chmod(readOnlyDir, 0o555))
	defer func() { _ = os.Chmod(readOnlyDir, 0o755) }()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = filepath.Join(readOnlyDir, "tidb.log")
		conf.TempDir = filepath.Join(tempDir, "tmp")
	})
	vardef.CloudStorageURI.Store("")
	SetGlobalExtStorageForTest(nil)

	s, err := GetGlobalExtStorage(ctx)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()

	uri := s.URI()
	expectedFallback := config.GetGlobalConfig().TempDir
	require.Contains(t, uri, expectedFallback, "storage URI should use temp dir when log dir not writable")
}
