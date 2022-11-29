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

package ttl_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl"
	"github.com/stretchr/testify/require"
)

func TestSessionRunInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")
	se := ttl.NewSession(tk.Session(), tk.Session(), nil)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (1, 10)")
		return nil
	}))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (2, 20)")
		return errors.New("err")
	}))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10"))

	require.NoError(t, se.RunInTxn(context.TODO(), func() error {
		tk.MustExec("insert into t values (3, 30)")
		return nil
	}))
	tk2.MustQuery("select * from t order by id asc").Check(testkit.Rows("1 10", "3 30"))
}

func TestSessionResetTimeZone(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.time_zone='UTC'")
	tk.MustExec("set @@time_zone='Asia/Shanghai'")

	se := ttl.NewSession(tk.Session(), tk.Session(), nil)
	tk.MustQuery("select @@time_zone").Check(testkit.Rows("Asia/Shanghai"))
	require.NoError(t, se.ResetWithGlobalTimeZone(context.TODO()))
	tk.MustQuery("select @@time_zone").Check(testkit.Rows("UTC"))
}

func TestEvalTTLExpireTime(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t(a int, t datetime) ttl = `t` + interval 1 day")
	tk.MustExec("create table test.t2(a int, t datetime) ttl = `t` + interval 3 month")

	tb, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tb.Meta()
	ttlTbl, err := ttl.NewPhysicalTable(model.NewCIStr("test"), tblInfo, model.NewCIStr(""))
	require.NoError(t, err)

	tb2, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tblInfo2 := tb2.Meta()
	ttlTbl2, err := ttl.NewPhysicalTable(model.NewCIStr("test"), tblInfo2, model.NewCIStr(""))
	require.NoError(t, err)

	se := ttl.NewSession(tk.Session(), tk.Session(), nil)

	now := time.UnixMilli(0)
	tz1, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	tz2, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	se.GetSessionVars().TimeZone = tz1
	tm, err := se.EvalExpireTime(context.TODO(), ttlTbl, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 08:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz1.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz2
	tm, err = se.EvalExpireTime(context.TODO(), ttlTbl, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 01:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz2.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz1
	tm, err = se.EvalExpireTime(context.TODO(), ttlTbl2, now)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 08:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz1.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz2
	tm, err = se.EvalExpireTime(context.TODO(), ttlTbl2, now)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 01:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz2.String(), tm.Location().String())
}
