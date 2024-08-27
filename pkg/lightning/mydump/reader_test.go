// Copyright 2019 PingCAP, Inc.
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

package mydump_test

import (
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
	. "github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestExportStatementNoTrailingNewLine(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	_, err = file.Write([]byte("CREATE DATABASE whatever;"))
	require.NoError(t, err)
	stat, err := file.Stat()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	require.NoError(t, err)
	require.Equal(t, []byte("CREATE DATABASE whatever;"), data)
}

func TestExportStatementWithComment(t *testing.T) {
	exportStatmentShouldBe(t, `
		/* whatever blabla
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
		 */;
		CREATE DATABASE whatever;
`, "CREATE DATABASE whatever;")
}

func TestExportStatementWithCommentNoTrailingNewLine(t *testing.T) {
	exportStatmentShouldBe(t, `
		/* whatever blabla
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
		 */;
		CREATE DATABASE whatever;`, "CREATE DATABASE whatever;")
}

func exportStatmentShouldBe(t *testing.T, stmt string, expected string) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	require.NoError(t, err)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte(stmt))
	require.NoError(t, err)
	stat, err := file.Stat()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	require.NoError(t, err)
	require.Equal(t, []byte(expected), data)
}

func TestExportStatementGBK(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	require.NoError(t, err)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("CREATE TABLE a (b int(11) COMMENT '"))
	require.NoError(t, err)
	// "D7 DC B0 B8 C0 FD" is the GBK encoding of "总案例".
	_, err = file.Write([]byte{0xD7, 0xDC, 0xB0, 0xB8, 0xC0, 0xFD})
	require.NoError(t, err)
	_, err = file.Write([]byte("');\n"))
	require.NoError(t, err)
	stat, err := file.Stat()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	require.NoError(t, err)
	require.Equal(t, []byte("CREATE TABLE a (b int(11) COMMENT '总案例');"), data)
}

func TestExportStatementGibberishError(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	require.NoError(t, err)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("\x9e\x02\xdc\xfbZ/=n\xf3\xf2N8\xc1\xf2\xe9\xaa\xd0\x85\xc5}\x97\x07\xae6\x97\x99\x9c\x08\xcb\xe8;"))
	require.NoError(t, err)
	stat, err := file.Stat()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	require.Len(t, data, 0)
	require.Regexp(t, `failed to decode \w* as auto: invalid schema encoding`, err.Error())
}

type AlwaysErrorReadSeekCloser struct{}

func (AlwaysErrorReadSeekCloser) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func (AlwaysErrorReadSeekCloser) Seek(int64, int) (int64, error) {
	return 0, errors.New("seek error")
}

func (AlwaysErrorReadSeekCloser) Close() error {
	return nil
}

func (AlwaysErrorReadSeekCloser) GetFileSize() (int64, error) {
	return 0, errors.New("get file size error")
}

func TestExportStatementHandleNonEOFError(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	ctx := context.TODO()

	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().
		Open(ctx, "no-perm-file", nil).
		Return(AlwaysErrorReadSeekCloser{}, nil)

	f := FileInfo{FileMeta: SourceFileMeta{Path: "no-perm-file", FileSize: 1}}
	_, err := ExportStatement(ctx, mockStorage, f, "auto")
	require.Contains(t, err.Error(), "read error")
}

func TestExportStatementCompressed(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	gzipFile := gzip.NewWriter(file)
	_, err = gzipFile.Write([]byte("CREATE DATABASE whatever;"))
	require.NoError(t, err)
	err = gzipFile.Close()
	require.NoError(t, err)
	stat, err := file.Stat()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size(), Compression: CompressionGZ}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	require.NoError(t, err)
	require.Equal(t, []byte("CREATE DATABASE whatever;"), data)
}
