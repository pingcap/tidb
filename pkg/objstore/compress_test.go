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

package objstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/stretchr/testify/require"
)

func TestWithCompressReadWriteFile(t *testing.T) {
	dir := t.TempDir()
	backend, err := ParseBackend("local://"+filepath.ToSlash(dir), nil)
	require.NoError(t, err)
	ctx := context.Background()
	storage, err := Create(ctx, backend, true)
	require.NoError(t, err)
	storage = WithCompression(storage, objectio.Gzip, objectio.DecompressConfig{})
	name := "with compress test"
	content := "hello,world!"
	fileName := strings.ReplaceAll(name, " ", "-") + ".txt.gz"
	err = storage.WriteFile(ctx, fileName, []byte(content))
	require.NoError(t, err)

	// make sure compressed file is written correctly
	file, err := os.Open(filepath.Join(dir, fileName))
	require.NoError(t, err)
	uncompressedFile, err := objectio.newCompressReader(objectio.Gzip, objectio.DecompressConfig{}, file)
	require.NoError(t, err)
	newContent, err := io.ReadAll(uncompressedFile)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))

	// test withCompression ReadFile
	newContent, err = storage.ReadFile(ctx, fileName)
	require.NoError(t, err)
	require.Equal(t, content, string(newContent))
}
