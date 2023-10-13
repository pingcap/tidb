// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/stretchr/testify/require"
)

func TestSessionRunInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")
	se := session.NewSession(tk.Session(), tk.Session(), nil)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (1, 10)")
		return nil
	}, session.TxnModeOptimistic))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	err := se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (2, 20)")
		return errors.New("mockErr")
	}, session.TxnModeOptimistic)
	require.EqualError(t, err, "mockErr")
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (3, 30)")
		return nil
	}, session.TxnModeOptimistic))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10", "3 30"))
}

func TestSessionResetTimeZone(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.time_zone='UTC'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")

	se := session.NewSession(tk.Session(), tk.Session(), nil)
	tk.MustQuery("select @@time_zone").Check(testkit.Rows("Asia/Shanghai"))
	require.NoError(t, se.ResetWithGlobalTimeZone(context.TODO()))
	tk.MustQuery("select @@time_zone").Check(testkit.Rows("UTC"))
}
