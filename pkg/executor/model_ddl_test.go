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

package executor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestModelDDLFeatureGate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set global tidb_enable_model_ddl = off")
	tk.MustContainErrMsg(
		"create model m1 (input (a int) output (b int)) using onnx location 's3://models/m1.onnx' checksum 'deadbeef'",
		"tidb_enable_model_ddl",
	)
}

func TestModelDDLPrivilegesAndShowCreate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec("use test")

	rootTK.MustExec("set global tidb_enable_model_ddl = on")
	rootTK.MustExec("create user u1")

	userTK := testkit.NewTestKit(t, store)
	require.NoError(t, userTK.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))

	userTK.MustContainErrMsg(
		"create model test.m1 (input (a int) output (b int)) using onnx location 's3://models/m1.onnx' checksum 'deadbeef'",
		"MODEL_ADMIN",
	)

	rootTK.MustExec("grant MODEL_ADMIN on *.* to u1")
	userTK.MustExec("create model test.m1 (input (a int) output (b int)) using onnx location 's3://models/m1.onnx' checksum 'deadbeef'")

	rows := userTK.MustQuery("show create model test.m1").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "m1", rows[0][0])
	require.Contains(t, rows[0][1], "CREATE MODEL")

	userTK.MustExec("create model test.m2 (input (a int) output (b int)) using onnx location 's3://models/o''reilly.onnx' checksum 'ab''cd'")
	rows = userTK.MustQuery("show create model test.m2").Rows()
	require.Len(t, rows, 1)
	require.Contains(t, rows[0][1], "LOCATION 's3://models/o\\'reilly.onnx'")
	require.Contains(t, rows[0][1], "CHECKSUM 'ab\\'cd'")
}

func TestShowCreateModelSnapshot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	timeSafe := time.Now().Add(-48 * 60 * 60 * time.Second).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))

	tk.MustExec("set global tidb_enable_model_ddl = on")
	tk.MustExec("create model m1 (input (a int) output (b int)) using onnx location 's3://models/m1-v1.onnx' checksum 'c1'")

	tk.MustExec("begin")
	snapshotTS := tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string)
	tk.MustExec("commit")

	tk.MustExec("alter model m1 set location 's3://models/m1-v2.onnx' checksum 'c2'")

	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%s'", snapshotTS))
	rows := tk.MustQuery("show create model m1").Rows()
	require.Contains(t, rows[0][1], "m1-v1.onnx")

	tk.MustExec("set @@tidb_snapshot = ''")
	rows = tk.MustQuery("show create model m1").Rows()
	require.Contains(t, rows[0][1], "m1-v2.onnx")
}

func TestModelVersionsInfoSchemaPrivilegeMasking(t *testing.T) {
	store := testkit.CreateMockStore(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec("use test")
	rootTK.MustExec("set global tidb_enable_model_ddl = on")
	rootTK.MustExec("create model m1 (input (a int) output (b int)) using onnx location 's3://models/private/path.onnx' checksum 'secret'")
	rootTK.MustExec("create user u2")

	userTK := testkit.NewTestKit(t, store)
	require.NoError(t, userTK.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, nil, nil, nil))

	userTK.MustQuery("select location, checksum from information_schema.tidb_model_versions where model_name='m1'").
		Check(testkit.Rows("<nil> <nil>"))

	rootTK.MustExec("grant MODEL_ADMIN on *.* to u2")
	userTK.MustQuery("select location, checksum from information_schema.tidb_model_versions where model_name='m1'").
		Check(testkit.Rows("s3://models/private/path.onnx secret"))
}
