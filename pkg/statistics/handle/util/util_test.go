// Copyright 2024 PingCAP, Inc.
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

package util_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIsSpecialGlobalIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(
		"create table t(" +
			"	a int," +
			"	b int," +
			"	c int," +
			"	d varchar(20)," +
			"	unique index b(b) global," +
			"	index c(c)," +
			"	unique index ub_s((b+1)) global," +
			"	unique index ud_s(d(3)) global," +
			"	index b_s((b+1))," +
			"	index d_s(d(3))" +
			") partition by hash(a) partitions 5")

	tblInfo := dom.MustGetTableInfo(t, "test", "t")
	cnt := 0
	for _, idx := range tblInfo.Indices {
		switch idx.Name.O {
		case "b", "c", "b_s", "d_s":
			cnt++
			require.False(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		case "ub_s", "ud_s":
			cnt++
			require.True(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		}
	}
	require.Equal(t, cnt, len(tblInfo.Indices))
}
