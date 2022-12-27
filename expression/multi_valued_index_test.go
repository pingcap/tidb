// Copyright 2022 PingCAP, Inc.
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

package expression_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
)

func TestMultiValuedIndexDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")

	tk.MustExec("create table t(a json);")
	tk.MustGetErrCode("select cast(a as signed array) from t", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("select json_extract(cast(a as signed array), '$[0]') from t", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("select * from t where cast(a as signed array)", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("select cast('[1,2,3]' as unsigned array);", errno.ErrNotSupportedYet)

	tk.MustExec("drop table t")
	tk.MustGetErrCode("CREATE TABLE t(x INT, KEY k ((1 AND CAST(JSON_ARRAY(x) AS UNSIGNED ARRAY))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("CREATE TABLE t1 (f1 json, key mvi((cast(cast(f1 as unsigned array) as unsigned array))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("CREATE TABLE t1 (f1 json, key mvi((cast(f1->>'$[*]' as unsigned array))));", errno.ErrInvalidTypeForJSON)
	tk.MustGetErrCode("CREATE TABLE t1 (f1 json, key mvi((cast(f1->'$[*]' as year array))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("CREATE TABLE t1 (f1 json, key mvi((cast(f1->'$[*]' as json array))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("CREATE TABLE t1 (f1 json, key mvi((cast(f1->'$[*]' as char(10) charset gbk array))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("create table t(j json, gc json as ((concat(cast(j->'$[*]' as unsigned array),\"x\"))));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("create table t(j json, gc json as (cast(j->'$[*]' as unsigned array)));", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("create view v as select cast('[1,2,3]' as unsigned array);", errno.ErrNotSupportedYet)
	tk.MustExec("create table t(a json, index idx((cast(a as signed array))));")

	tk.MustExec("drop table t")
	tk.MustGetErrCode("create table t(a json, b int, index idx(b, (cast(a as signed array)), (cast(a as signed array))));", errno.ErrNotSupportedYet)
	tk.MustExec("create table t(a json, b int);")
	tk.MustGetErrCode("create index idx on t (b, (cast(a as signed array)), (cast(a as signed array)))", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("alter table t add index idx(b, (cast(a as signed array)), (cast(a as signed array)))", errno.ErrNotSupportedYet)
	tk.MustExec("create index idx1 on t (b, (cast(a as signed array)))")
	tk.MustExec("alter table t add index idx2(b, (cast(a as signed array)))")

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a json, b int, index idx3(b, (cast(a as signed array))));")
}

func TestMultiValuedIndexDML(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")

	mode := []string{`''`, `default`}

	for _, m := range mode {
		tk.MustExec(fmt.Sprintf("set @@sql_mode=%s", m))

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as unsigned array))));`)
		tk.MustExec(`insert into t values ('[1,2,3]');`)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrDataOutOfRangeFunctionalIndex)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as signed array))));`)
		tk.MustExec(`insert into t values ('[1,2,3]');`)
		tk.MustExec(`insert into t values ('[-1]');`)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as double array))));`)
		tk.MustExec(`insert into t values ('[1,2,3]');`)
		tk.MustExec(`insert into t values ('[-1]');`)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('[1.2]');`)
		tk.MustExec(`insert into t values ('[1.0]');`)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as decimal(10, 2) array))));`)
		tk.MustExec(`insert into t values ('[1,2,3]');`)
		tk.MustExec(`insert into t values ('[-1]');`)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('[1.2]');`)
		tk.MustExec(`insert into t values ('[1.0]');`)
		tk.MustExec(`insert into t values ('[1.1102]');`)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as char(1) array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('["1"]');`)
		tk.MustExec(`insert into t values ('["a"]');`)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrFunctionalIndexDataIsTooLong)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as char(2) array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('["1"]');`)
		tk.MustExec(`insert into t values ('["a"]');`)
		tk.MustExec(`insert into t values ('["汉字"]');`)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as binary(1) array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('["1"]');`)
		tk.MustExec(`insert into t values ('["a"]');`)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrFunctionalIndexDataIsTooLong)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as binary(2) array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values ('["1"]');`)
		tk.MustExec(`insert into t values ('["a"]');`)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrFunctionalIndexDataIsTooLong)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as date array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values (json_array(cast("2022-02-02" as date)));`)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as time array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values (json_array(cast("11:00:00" as time)));`)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)

		tk.MustExec(`drop table if exists t;`)
		tk.MustExec(`create table t(a json, index idx((cast(a as datetime array))));`)
		tk.MustGetErrCode(`insert into t values ('[1,2,3]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[-1]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["1"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["a"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('["汉字"]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.2]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values ('[1.0]');`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("11:00:00" as time)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustGetErrCode(`insert into t values (json_array(cast("2022-02-02" as date)));`, errno.ErrInvalidJSONValueForFuncIndex)
		tk.MustExec(`insert into t values (json_array(cast("2022-02-02 11:00:00" as datetime)));`)
		tk.MustGetErrCode(`insert into t values (json_array(cast('{"a":1}' as json)));`, errno.ErrInvalidJSONValueForFuncIndex)
	}
}
