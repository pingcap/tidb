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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestStarterTTLJobIntervalSQL(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter deployment mode is only available in nextgen")
	}

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_default (created_at datetime) TTL = created_at + interval 1 day")
	require.Contains(t, tk.MustQuery("show create table t_default").Rows()[0][1].(string),
		"TTL_JOB_INTERVAL='"+model.StarterDefaultTTLJobInterval+"'")

	tk.MustGetErrCode(
		"create table t_create_invalid (created_at datetime) "+
			"TTL = created_at + interval 1 day TTL_JOB_INTERVAL = '1h'",
		errno.ErrUnsupportedDDLOperation,
	)

	tk.MustExec("create table t_alter (created_at datetime) TTL = created_at + interval 1 day")
	tk.MustGetErrCode(
		"alter table t_alter TTL_JOB_INTERVAL = '1h'",
		errno.ErrUnsupportedDDLOperation,
	)
	require.Contains(t, tk.MustQuery("show create table t_alter").Rows()[0][1].(string),
		"TTL_JOB_INTERVAL='"+model.StarterDefaultTTLJobInterval+"'")

	tk.MustExec("alter table t_alter TTL_JOB_INTERVAL = '15m'")
}
