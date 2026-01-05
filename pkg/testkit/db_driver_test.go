// Copyright 2025 PingCAP, Inc.
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

package testkit_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMockDB(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	db := testkit.CreateMockDB(tk)
	defer db.Close()

	var err error
	_, err = db.Exec("use test")
	require.NoError(t, err)
	_, err = db.Exec("create table t (id int, v varchar(255))")
	require.NoError(t, err)
	_, err = db.Exec("insert into t values (1, 'a'), (2, 'b')")
	require.NoError(t, err)

	// Test Query
	rows, err := db.Query("select * from t order by id")
	require.NoError(t, err)
	defer rows.Close()

	var id int
	var v string
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &v))
	require.Equal(t, 1, id)
	require.Equal(t, "a", v)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &v))
	require.Equal(t, 2, id)
	require.Equal(t, "b", v)

	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	// Test Exec
	res, err := db.Exec("insert into t values (3, 'c')")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	// Verify with TestKit
	tk.MustExec("use test")
	tk.MustQuery("select * from t where id = 3").Check(testkit.Rows("3 c"))

	// Test Prepare
	stmt, err := db.Prepare("select v from t where id = ?")
	require.NoError(t, err)
	defer stmt.Close()

	row := stmt.QueryRow(2)
	require.NoError(t, row.Scan(&v))
	require.Equal(t, "b", v)
}
