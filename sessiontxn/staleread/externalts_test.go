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

package staleread_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestReadWriteExternalTimestamp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_external_ts=19980613")
	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("19980613"))
	tk.MustExec("set global tidb_external_ts=20220930")
	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("20220930"))
}

func TestExternalTimestampRead(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id INT NOT NULL,d double,PRIMARY KEY (id))")
	tk.MustExec("insert into t values (0, 100)")
	tk.MustExec("insert into t values (1, 100)")
	tk.MustExec("insert into t values (2, 100)")
	tk.MustExec("insert into t values (3, 100)")
	tk.MustQuery("select * from t").Check(testkit.Rows("0 100", "1 100", "2 100", "3 100"))

	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("0"))
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit;")
	tk.MustExec("insert into t values (4, 100)")
	// as the `tidb_external_ts` is set an old value, the newest row (4, 100) cannot be read
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	tk.MustQuery("select * from t").Check(testkit.Rows("0 100", "1 100", "2 100", "3 100"))

	tk.MustExec("set tidb_enable_external_ts_read=OFF")
	tk.MustQuery("select * from t").Check(testkit.Rows("0 100", "1 100", "2 100", "3 100", "4 100"))
}

func TestExternalTimestampReadonly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("create table t (id INT NOT NULL,PRIMARY KEY (id))")

	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("0"))
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit;")

	// with tidb_enable_external_ts_read enabled, this session will be readonly
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	_, err := tk.Exec("insert into t values (0)")
	require.Error(t, err)

	tk.MustExec("set tidb_enable_external_ts_read=OFF")
	tk.MustExec("insert into t values (0)")

	// even when tidb_enable_external_ts_read is enabled, internal SQL will not be affected
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	tk.Session().GetSessionVars().InRestrictedSQL = true
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk.MustExecWithContext(ctx, "insert into t values (1)")
	tk.Session().GetSessionVars().InRestrictedSQL = false
}

func TestExternalTimestampReadWithTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("create table t (id INT NOT NULL,PRIMARY KEY (id))")

	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("0"))
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit;")

	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	tk.MustExec("set tidb_enable_external_ts_read=ON")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("start transaction")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("commit")

	tk.MustExec("set tidb_enable_external_ts_read=OFF")
	tk.MustExec("start transaction")
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))
	tk.MustExec("commit")

	tk.MustExec("start transaction")
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_external_ts_read=ON")
	// `tidb_enable_external_ts_read` doesn't affect existing transaction
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_external_ts_read=OFF")
	tk.MustExec("commit")
}

func TestExternalTimestampNotAffectPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("create table t (id INT NOT NULL,PRIMARY KEY (id))")

	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	tk.MustQuery("select @@tidb_external_ts").Check(testkit.Rows("0"))
	tk.MustExec("start transaction;set global tidb_external_ts=@@tidb_current_ts;commit;")

	tk.MustExec("insert into t values (1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("0", "1"))

	tk.MustExec("set tidb_enable_external_ts_read=on")
	tk.MustExec("prepare my_select from 'select * from t'")
	tk.MustQuery("execute my_select").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_external_ts_read=off")

	tk.MustQuery("execute my_select").Check(testkit.Rows("0", "1"))
}
