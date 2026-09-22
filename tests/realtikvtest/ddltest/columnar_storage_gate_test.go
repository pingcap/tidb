// Copyright 2026 PingCAP, Inc.
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

package ddltest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestColumnarStorageGateNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("columnar DDL gate E2E is for next-gen")
	}

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.CSE.ColumnarStoreType = "columnar"
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t_like")
	tk.MustExec("create table t(a int)")

	tk.MustExec("set global tidb_columnar_storage_enabled = 'OFF'")
	tk.MustGetErrCode("alter table t set tiflash replica 1", errno.ErrUnsupportedDDLOperation)
	tk.MustContainErrMsg("alter table t set tiflash replica 1", "Columnar Storage is not enabled")
	require.Nil(t, external.GetTableByName(t, tk, "test", "t").Meta().TiFlashReplica)

	tk.MustExec("set global tidb_columnar_storage_enabled = 'ON'")
	tk.MustExec("alter table t set tiflash replica 1")
	require.Equal(t, uint64(1), external.GetTableByName(t, tk, "test", "t").Meta().TiFlashReplica.Count)

	tk.MustExec("set global tidb_columnar_storage_enabled = 'OFF'")
	tk.MustGetErrCode("create table t_like like t", errno.ErrUnsupportedDDLOperation)
	tk.MustContainErrMsg("create table t_like like t", "Columnar Storage is not enabled")
	tk.MustQuery("show tables like 't_like'").Check(testkit.Rows())

	tk.MustExec("alter table t set tiflash replica 0")
	require.Nil(t, external.GetTableByName(t, tk, "test", "t").Meta().TiFlashReplica)
}
