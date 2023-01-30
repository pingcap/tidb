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

package ddl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
)

func TestMultiValuedIndexOnlineDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 32;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 100; i++ {
		sb.WriteString(fmt.Sprintf("(%d, '[%d, %d, %d]')", i, i+1, i+2, i+3))
		if i != 99 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())

	internalTK := testkit.NewTestKit(t, store)
	internalTK.MustExec("use test")

	hook := &callback.TestDDLCallback{Do: dom}
	n := 100
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		internalTK.MustExec(fmt.Sprintf("insert into t values (%d, '[%d, %d, %d]')", n, n, n+1, n+2))
		internalTK.MustExec(fmt.Sprintf("delete from t where pk = %d", n-4))
		internalTK.MustExec(fmt.Sprintf("update t set a = '[%d, %d, %d]' where pk = %d", n-3, n-2, n+1000, n-3))
		n++
	}
	o := dom.DDL().GetHook()
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table t add index idx((cast(a as signed array)))")
	tk.MustExec("admin check table t")
	dom.DDL().SetHook(o)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (pk int primary key, a json);")
	tk.MustExec("insert into t values (1, '[1,2,3]');")
	tk.MustExec("insert into t values (2, '[2,3,4]');")
	tk.MustExec("insert into t values (3, '[3,4,5]');")
	tk.MustExec("insert into t values (4, '[-4,5,6]');")
	tk.MustGetErrCode("alter table t add unique index idx((cast(a as signed array)));", errno.ErrDupEntry)
	tk.MustGetErrMsg("alter table t add index idx((cast(a as unsigned array)));", "[ddl:8202]Cannot decode index value, because [types:1690]constant -4 overflows bigint")
}
