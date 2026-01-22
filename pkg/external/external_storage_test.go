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

package external

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
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

func TestExternalStorage(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	err := CreateExternalStorage(tempDir, "test_namespace", nil)
	require.NoError(t, err)

	s := GetExternalStorage()
	require.NotNil(t, s)

	basePath := GetStorageBasePath()
	require.Equal(t, filepath.Join(tempDir, "test_namespace"), basePath)

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
	// The URI() method on the wrapper calls the underlying storage's URI().
	// For local storage, it should be the path.
	// The underlying storage is created lazily. Let's trigger it.
	_, err = s.FileExists(ctx, "anyfile")
	require.NoError(t, err)
	uri := s.URI()
	require.Contains(t, uri, tempDir)
	require.Contains(t, uri, "test_namespace")

	// Test Close
	es, ok := s.(*externalStorage)
	require.True(t, ok)
	es.Close()
	require.Nil(t, es.storage)
}
