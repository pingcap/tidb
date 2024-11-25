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

package initstats

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestConcurrentlyInitStatsWithMemoryLimit(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
		conf.Performance.ConcurrentlyInitStats = true
	})
	handle.IsFullCacheFunc = func(cache statstypes.StatsCache, total uint64) bool {
		return true
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	tk.MustExec("analyze table t1")
	for i := 2; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("create table t%v (a int, b int, c int, primary key(a), key idx(b))", i))
		tk.MustExec(fmt.Sprintf("insert into t%v select * from t1", i))
		tk.MustExec(fmt.Sprintf("analyze table t%v", i))
	}
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	h.Clear()
	require.Equal(t, h.MemConsumed(), int64(0))
	require.NoError(t, h.InitStats(context.Background(), is))
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where a = 1", i)).CheckNotContain("pseudo")
	}
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where b = 1", i)).CheckNotContain("pseudo")
	}
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where c = 1", i)).CheckNotContain("pseudo")
	}
}
