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
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDDLStatementsBackFill(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	needReorg := false
	callback := &callback.TestDDLCallback{
		Do: dom,
	}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if job.SchemaState == model.StateWriteReorganization {
			needReorg = true
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	dom.DDL().SetHook(callback)
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
