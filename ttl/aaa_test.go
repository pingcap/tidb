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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestAAA(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	expire := types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Date(2018, 3, 8, 16, 1, 0, 315313000, time.UTC)), mysql.TypeTimestamp, 6))
	cases := []struct {
		name   string
		create string
		key1   []types.Datum
		key2   []types.Datum
	}{
		{
			"t1",
			"create table t1(a int, expire datetime)",
			[]types.Datum{types.NewIntDatum(1)},
			[]types.Datum{types.NewIntDatum(2)},
		},
		{
			"t2",
			"create table t2(id int primary key, expire datetime)",
			[]types.Datum{types.NewIntDatum(1)},
			[]types.Datum{types.NewIntDatum(2)},
		},
		{
			"t3",
			"create table t3 (a int, b varchar(255), expire datetime, primary key(a, b) clustered)",
			[]types.Datum{types.NewIntDatum(1), types.NewStringDatum("k1")},
			[]types.Datum{types.NewIntDatum(2), types.NewStringDatum("k2")},
		},
	}

	tbls := make([]*ttlTable, 0, len(cases))
	for _, c := range cases {
		tk.MustExec(c.create)
		tbl, err := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(c.name))
		require.NoError(t, err)
		tbls = append(tbls, &ttlTable{
			Schema:    model.NewCIStr("test"),
			TableInfo: tbl.Meta(),
		})
	}

	fmt.Println("=============")
	for i, tbl := range tbls {
		c := cases[i]

		querySQL, err := tbl.FormatQuerySQL(c.key1, c.key2, expire, 128)
		require.NoError(t, err)
		fmt.Println(querySQL)

		deleteSQL, err := tbl.FormatDeleteSQL([][]types.Datum{c.key1, c.key2}, expire)
		require.NoError(t, err)
		fmt.Println(deleteSQL)
	}
}
