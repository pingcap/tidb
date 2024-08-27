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

package loadremotetest

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strconv"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestFilenameAsterisk() {
	s.tk.MustExec("DROP DATABASE IF EXISTS multi_load;")
	s.tk.MustExec("CREATE DATABASE multi_load;")
	s.tk.MustExec("CREATE TABLE multi_load.t (i INT PRIMARY KEY, s varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.001.tsv",
		},
		Content: []byte("1\ttest1\n" +
			"2\ttest2"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.002.tsv",
		},
		Content: []byte("3\ttest3\n" +
			"4\ttest4"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.003.tsv",
		},
		Content: []byte("5\ttest5\n" +
			"6\ttest6"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "not-me",
			Name:       "db.tbl.001.tsv",
		},
		Content: []byte("9\ttest9\n" +
			"10\ttest10"),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/db.tbl.*.tsv?endpoint=%s'
		INTO TABLE multi_load.t WITH thread=2;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.Equal(uint64(0), s.tk.Session().GetSessionVars().StmtCtx.LastInsertID)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4", "5 test5", "6 test6",
	))

	s.tk.MustExec("TRUNCATE TABLE multi_load.t;")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/db.tbl.*.tsv?endpoint=%s'
		INTO TABLE multi_load.t IGNORE 1 LINES WITH thread=20;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.Equal(uint64(0), s.tk.Session().GetSessionVars().StmtCtx.LastInsertID)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"2 test2", "4 test4", "6 test6",
	))

	// only `*` and `[]` is supported in pattern matching
	s.tk.MustExec("TRUNCATE TABLE multi_load.t;")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/db.tbl.00[13].tsv?endpoint=%s'
		INTO TABLE multi_load.t with thread=1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.Equal(uint64(0), s.tk.Session().GetSessionVars().StmtCtx.LastInsertID)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "5 test5", "6 test6",
	))
}

func (s *mockGCSSuite) TestLastInsertID() {
	s.tk.MustExec("DROP DATABASE IF EXISTS multi_load;")
	s.tk.MustExec("CREATE DATABASE multi_load;")
	s.tk.MustExec("CREATE TABLE multi_load.t (i INT auto_increment PRIMARY KEY, s varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "last-insert-id",
			Name:       "db.tbl.001.tsv",
		},
		Content: []byte("1\ttest1\n" +
			"2\ttest2"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "last-insert-id",
			Name:       "db.tbl.002.tsv",
		},
		Content: []byte("3\ttest3\n" +
			"4\ttest4"),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://last-insert-id/db.tbl.00*.tsv?endpoint=%s'
		INTO TABLE multi_load.t (@1, s) with thread=1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.Equal(uint64(1), s.tk.Session().GetSessionVars().StmtCtx.LastInsertID)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4",
	))

	// we don't test for auto_random, since the auto_id is not stable.
}

func (s *mockGCSSuite) TestMultiBatchWithIgnoreLines() {
	s.tk.MustExec("DROP DATABASE IF EXISTS multi_load;")
	s.tk.MustExec("CREATE DATABASE multi_load;")
	s.tk.MustExec("CREATE TABLE multi_load.t2 (i INT);")

	// [start, end] is both inclusive
	genData := func(start, end int) []byte {
		buf := make([][]byte, 0, end-start+1)
		for i := start; i <= end; i++ {
			buf = append(buf, []byte(strconv.Itoa(i)))
		}
		return bytes.Join(buf, []byte("\n"))
	}

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "multi-batch.001.tsv",
		},
		Content: genData(1, 10),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "multi-batch.002.tsv",
		},
		Content: genData(11, 20),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/multi-batch.*.tsv?endpoint=%s'
		INTO TABLE multi_load.t2 IGNORE 2 LINES WITH batch_size = 3, thread=1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM multi_load.t2;").Check(testkit.Rows(
		"3", "4", "5", "6", "7", "8", "9", "10",
		"13", "14", "15", "16", "17", "18", "19", "20",
	))
}

func (s *mockGCSSuite) TestMixedCompression() {
	s.tk.MustExec("DROP DATABASE IF EXISTS multi_load;")
	s.tk.MustExec("CREATE DATABASE multi_load;")
	s.tk.MustExec("CREATE TABLE multi_load.t (i INT PRIMARY KEY, s varchar(32));")

	// gzip content
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write([]byte(`1,test1
2,test2
3,test3
4,test4`))
	require.NoError(s.T(), err)
	err = w.Close()
	require.NoError(s.T(), err)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "compress.001.tsv.gz",
		},
		Content: buf.Bytes(),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "compress.002.tsv",
		},
		Content: []byte(`5,test5
6,test6
7,test7
8,test8
9,test9`),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/compress.*?endpoint=%s'
		INTO TABLE multi_load.t fields terminated by ',';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4",
		"5 test5", "6 test6", "7 test7", "8 test8", "9 test9",
	))

	// with ignore N rows
	s.tk.MustExec("truncate table multi_load.t")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/compress.*?endpoint=%s'
		INTO TABLE multi_load.t fields terminated by ',' ignore 3 lines;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"4 test4",
		"8 test8", "9 test9",
	))
}
