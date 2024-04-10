// Copyright 2023 PingCAP, Inc.
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

package importintotest

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/testkit"
)

func (s *mockGCSSuite) getCompressedData(compression mydump.Compression, data []byte) []byte {
	var buf bytes.Buffer
	var w io.WriteCloser
	switch compression {
	case mydump.CompressionGZ:
		w = gzip.NewWriter(&buf)
	case mydump.CompressionZStd:
		var err error
		w, err = zstd.NewWriter(&buf)
		s.NoError(err)
	case mydump.CompressionSnappy:
		w = snappy.NewBufferedWriter(&buf)
	default:
		panic(fmt.Sprintf("unknown compression type: %d", compression))
	}
	_, err := w.Write(data)
	s.NoError(err)
	s.NoError(w.Close())
	compressedData := buf.Bytes()
	s.NotEqual(data, compressedData)
	return compressedData
}

func (s *mockGCSSuite) TestGzipAndMixedCompression() {
	s.prepareAndUseDB("gzip")
	s.tk.MustExec("CREATE TABLE gzip.t (i INT PRIMARY KEY, s varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gzip", Name: "compress.001.csv.gz"},
		Content:     s.getCompressedData(mydump.CompressionGZ, []byte("1,test1\n2,test2")),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gzip", Name: "compress.001.csv.gzip"},
		Content:     s.getCompressedData(mydump.CompressionGZ, []byte("3,test3\n4,test4")),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gzip", Name: "compress.002.csv"},
		Content:     []byte("5,test5\n6,test6\n7,test7\n8,test8\n9,test9"),
	})

	sql := fmt.Sprintf(`IMPORT INTO gzip.t FROM 'gs://gzip/compress.*?endpoint=%s'
		WITH thread=1;`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM gzip.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4",
		"5 test5", "6 test6", "7 test7", "8 test8", "9 test9",
	))

	// with ignore N rows
	s.tk.MustExec("truncate table gzip.t")
	sql = fmt.Sprintf(`IMPORT INTO gzip.t FROM 'gs://gzip/compress.*?endpoint=%s'
		WITH skip_rows=1, thread=1;`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM gzip.t;").Check(testkit.Rows(
		"2 test2", "4 test4", "6 test6", "7 test7", "8 test8", "9 test9",
	))
}

func (s *mockGCSSuite) TestZStd() {
	s.prepareAndUseDB("zstd")
	s.tk.MustExec("CREATE TABLE zstd.t (i INT PRIMARY KEY, s varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "zstd", Name: "t.01.csv.zst"},
		Content:     s.getCompressedData(mydump.CompressionZStd, []byte("1,test1\n2,test2")),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "zstd", Name: "t.02.csv.zstd"},
		Content:     s.getCompressedData(mydump.CompressionZStd, []byte("3,test3\n4,test4")),
	})

	sql := fmt.Sprintf(`IMPORT INTO zstd.t FROM 'gs://zstd/t.*?endpoint=%s'
		WITH thread=1;`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM zstd.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4",
	))
}

func (s *mockGCSSuite) TestSnappy() {
	s.prepareAndUseDB("snappy")
	s.tk.MustExec("CREATE TABLE snappy.t (i INT PRIMARY KEY, s varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "snappy", Name: "t.01.csv.snappy"},
		Content:     s.getCompressedData(mydump.CompressionSnappy, []byte("1,test1\n2,test2")),
	})

	sql := fmt.Sprintf(`IMPORT INTO snappy.t FROM 'gs://snappy/t.*?endpoint=%s'
		WITH thread=1;`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM snappy.t;").Check(testkit.Rows(
		"1 test1", "2 test2",
	))
}
