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

package ttl

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSessionRunInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")
	se := NewSession(tk.Session(), tk.Session(), nil)
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
