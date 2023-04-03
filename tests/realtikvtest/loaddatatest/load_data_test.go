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

package loaddatatest

import (
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/testkit"
)

func (s *mockGCSSuite) TestPhysicalMode() {
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
	s.tk.MustExec("drop database if exists load_data;")
	s.tk.MustExec("create database load_data;")
	s.tk.MustExec("use load_data;")
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/db.tbl.*.tsv?endpoint=%s'
		INTO TABLE load_data.t with import_mode='physical'`, gcsEndpoint)

	createTableSQLs := []string{
		"create table t (a bigint, b varchar(100));",
		"create table t (a bigint primary key, b varchar(100));",
		"create table t (a bigint primary key, b varchar(100), key(b, a));",

		"create table t (a bigint auto_increment primary key, b varchar(100));",
		"create table t (a bigint auto_random primary key, b varchar(100));",
	}

	for _, createTableSQL := range createTableSQLs {
		s.tk.MustExec("drop table if exists t;")
		s.tk.MustExec(createTableSQL)
		s.tk.MustExec(loadDataSQL)
		s.tk.MustQuery("SELECT * FROM load_data.t;").Check(testkit.Rows(
			"1 test1", "2 test2", "3 test3", "4 test4", "5 test5", "6 test6",
		))
	}
}
