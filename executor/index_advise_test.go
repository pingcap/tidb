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

package executor_test

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIndexAdvise(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	_, err := tk.Exec("index advise infile '/tmp/nonexistence.sql'")
	require.EqualError(t, err, "Index Advise: don't support load file without local field")
	_, err = tk.Exec("index advise local infile ''")
	require.EqualError(t, err, "Index Advise: infile path is empty")
	_, err = tk.Exec("index advise local infile '/tmp/nonexistence.sql' lines terminated by ''")
	require.EqualError(t, err, "Index Advise: don't support advise index for SQL terminated by nil")

	path := "/tmp/index_advise.sql"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		err = fp.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()

	_, err = fp.WriteString("\n" +
		"select * from t;\n" +
		"\n" +
		"select * from t where a > 1;\n" +
		"select a from t where a > 1 and a < 100;\n" +
		"\n" +
		"\n" +
		"select a,b from t1,t2 where t1.a = t2.b;\n" +
		"\n")
	require.NoError(t, err)

	// TODO: Using "tastCase" to do more test when we finish the index advisor completely.
	tk.MustExec("index advise local infile '/tmp/index_advise.sql' max_minutes 3 max_idxnum per_table 4 per_db 5")
	ctx := tk.Session().(sessionctx.Context)
	ia, ok := ctx.Value(executor.IndexAdviseVarKey).(*executor.IndexAdviseInfo)
	defer ctx.SetValue(executor.IndexAdviseVarKey, nil)
	require.True(t, ok)
	require.Equal(t, uint64(3), ia.MaxMinutes)
	require.Equal(t, uint64(4), ia.MaxIndexNum.PerTable)
	require.Equal(t, uint64(5), ia.MaxIndexNum.PerDB)
}
