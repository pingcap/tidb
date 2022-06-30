// Copyright 2021 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestDefaultValueIsBinaryString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tests := []struct {
		colTp  string
		defVal string
		result string
	}{
		{"char(10) charset gbk", "0xC4E3BAC3", "你好"},
		{"char(10) charset gbk", "'好'", "好"},
		{"varchar(10) charset gbk", "0xC4E3BAC3", "你好"},
		{"char(10) charset utf8mb4", "0xE4BDA0E5A5BD", "你好"},
		{"char(10) charset utf8mb4", "0b111001001011100010010110111001111001010110001100", "世界"},
		{"bit(48)", "0xE4BDA0E5A5BD", "你好"},
		{"enum('你好')", "0xE4BDA0E5A5BD", "你好"},
		{"set('你好')", "0xE4BDA0E5A5BD", "你好"},
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	for _, tt := range tests {
		tk.MustExec("drop table if exists t;")
		template := "create table t (a %s default %s);"
		tk.MustExec(fmt.Sprintf(template, tt.colTp, tt.defVal))
		tk.MustExec("insert into t values (default);")
		tk.MustQuery("select a from t;").Check(testkit.Rows(tt.result))
	}

	// Test invalid default value.
	tk.MustExec("drop table if exists t;")
	// 0xE4BDA0E5A5BD81 is an invalid utf-8 string.
	tk.MustGetErrMsg("create table t (a char(20) charset utf8mb4 default 0xE4BDA0E5A5BD81);",
		"[ddl:1067]Invalid default value for 'a'")
	tk.MustGetErrMsg("create table t (a blob default 0xE4BDA0E5A5BD81);",
		"[ddl:1101]BLOB/TEXT/JSON column 'a' can't have a default value")
}

// https://github.com/pingcap/tidb/issues/30740.
func TestDefaultValueInEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	// The value 0x91 should not cause panic.
	tk.MustExec("create table t(a enum('a', 0x91) charset gbk);")
	tk.MustExec("insert into t values (1), (2);")                 // Use 1-base index to locate the value.
	tk.MustQuery("select a from t;").Check(testkit.Rows("a", "")) // 0x91 is truncate.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a enum('a', 0x91)) charset gbk;") // Test for table charset.
	tk.MustExec("insert into t values (1), (2);")
	tk.MustQuery("select a from t;").Check(testkit.Rows("a", ""))
	tk.MustExec("drop table t;")
	tk.MustGetErrMsg("create table t(a set('a', 0x91, '') charset gbk);",
		"[types:1291]Column 'a' has duplicated value '' in SET")
	// Test valid utf-8 string value in enum. Note that the binary literal only can be decoded to utf-8.
	tk.MustExec("create table t (a enum('a', 0xE4BDA0E5A5BD) charset gbk);")
	tk.MustExec("insert into t values (1), (2);")
	tk.MustQuery("select a from t;").Check(testkit.Rows("a", "你好"))
}

func TestDDLStatementsBackFill(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	needReorg := false
	dom.DDL().SetHook(&ddl.TestDDLCallback{
		Do: dom,
		OnJobUpdatedExported: func(job *model.Job) {
			if job.SchemaState == model.StateWriteReorganization {
				needReorg = true
			}
		},
	})
	tk.MustExec("create table t (a int, b char(65));")
	tk.MustExec("insert into t values (1, '123');")
	testCases := []struct {
		ddlSQL            string
		expectedNeedReorg bool
	}{
		{"alter table t modify column a bigint;", false},
		{"alter table t modify column b char(255);", false},
		{"alter table t modify column a varchar(100);", true},
		{"create table t1 (a int, b int);", false},
		{"alter table t1 add index idx_a(a);", true},
		{"alter table t1 add primary key(b) nonclustered;", true},
		{"alter table t1 drop primary key;", false},
	}
	for _, tc := range testCases {
		needReorg = false
		tk.MustExec(tc.ddlSQL)
		require.Equal(t, tc.expectedNeedReorg, needReorg, tc)
	}
}

func TestDDLOnCachedTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tests := []struct {
		sql    string
		result string
	}{
		{"drop table t", "[ddl:8242]'Drop Table' is unsupported on cache tables."},
		{"create index t_id on t (id)", "[ddl:8242]'Create Index' is unsupported on cache tables."},
		{"alter table t drop index c", "[ddl:8242]'Alter Table' is unsupported on cache tables."},
		{"alter table t add column (d int)", "[ddl:8242]'Alter Table' is unsupported on cache tables."},
		{"truncate table t", "[ddl:8242]'Truncate Table' is unsupported on cache tables."},
		{"rename table t to t1", "[ddl:8242]'Rename Table' is unsupported on cache tables."},
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (id int, c int, index(c));")
	tk.MustExec("alter table t cache;")

	for _, tt := range tests {
		tk.MustGetErrMsg(tt.sql, tt.result)
	}

	tk.MustExec("alter table t nocache;")
	tk.MustExec("drop table if exists t;")
}
