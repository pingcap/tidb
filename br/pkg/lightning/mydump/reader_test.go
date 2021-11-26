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
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var _ = Suite(&testMydumpReaderSuite{})

type testMydumpReaderSuite struct{}

func (s *testMydumpReaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpReaderSuite) TearDownSuite(c *C) {}

func (s *testMydumpReaderSuite) TestExportStatementNoTrailingNewLine(c *C) {
	dir := c.MkDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	_, err = file.Write([]byte("CREATE DATABASE whatever;"))
	c.Assert(err, IsNil)
	stat, err := file.Stat()
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE DATABASE whatever;"))
}

func (s *testMydumpReaderSuite) TestExportStatementWithComment(c *C) {
	s.exportStatmentShouldBe(c, `
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

func (s *testMydumpReaderSuite) TestExportStatementWithCommentNoTrailingNewLine(c *C) {
	s.exportStatmentShouldBe(c, `
		/* whatever blabla
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
		 */;
		CREATE DATABASE whatever;`, "CREATE DATABASE whatever;")
}

func (s *testMydumpReaderSuite) exportStatmentShouldBe(c *C, stmt string, expected string) {
	dir := c.MkDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte(stmt))
	c.Assert(err, IsNil)
	stat, err := file.Stat()
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte(expected))
}

func (s *testMydumpReaderSuite) TestExportStatementGBK(c *C) {
	dir := c.MkDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("CREATE TABLE a (b int(11) COMMENT '"))
	c.Assert(err, IsNil)
	// "D7 DC B0 B8 C0 FD" is the GBK encoding of "总案例".
	_, err = file.Write([]byte{0xD7, 0xDC, 0xB0, 0xB8, 0xC0, 0xFD})
	c.Assert(err, IsNil)
	_, err = file.Write([]byte("');\n"))
	c.Assert(err, IsNil)
	stat, err := file.Stat()
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE TABLE a (b int(11) COMMENT '总案例');"))
}

func (s *testMydumpReaderSuite) TestExportStatementGibberishError(c *C) {
	dir := c.MkDir()
	file, err := os.Create(filepath.Join(dir, "tidb_lightning_test_reader"))
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("\x9e\x02\xdc\xfbZ/=n\xf3\xf2N8\xc1\xf2\xe9\xaa\xd0\x85\xc5}\x97\x07\xae6\x97\x99\x9c\x08\xcb\xe8;"))
	c.Assert(err, IsNil)
	stat, err := file.Stat()
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	f := FileInfo{FileMeta: SourceFileMeta{Path: stat.Name(), FileSize: stat.Size()}}
	data, err := ExportStatement(context.TODO(), store, f, "auto")
	c.Assert(data, HasLen, 0)
	c.Assert(err, ErrorMatches, `failed to decode \w* as auto: invalid schema encoding`)
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

func (s *testMydumpReaderSuite) TestExportStatementHandleNonEOFError(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()

	ctx := context.TODO()

	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().
		Open(ctx, "no-perm-file").
		Return(AlwaysErrorReadSeekCloser{}, nil)

	f := FileInfo{FileMeta: SourceFileMeta{Path: "no-perm-file", FileSize: 1}}
	_, err := ExportStatement(ctx, mockStorage, f, "auto")
	c.Assert(err, ErrorMatches, "read error")
}
