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

package usage_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGCIndexUsage(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	const (
		tableCount = 10
		indexCount = 10
	)

	tk.MustExec("use test")
	for i := 0; i < tableCount; i++ {
		stmt := fmt.Sprintf(
			"CREATE TABLE t%d (", i,
		)
		for j := 0; j < indexCount; j++ {
			stmt += fmt.Sprintf("id%d int, key test_id%d (id%d)", j, j, j)
			if j != indexCount-1 {
				stmt += ","
			}
		}
		stmt += ")"
		tk.MustExec(stmt)
	}

	c := dom.StatsHandle().NewSessionIndexUsageCollector()
	db, ok := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema).SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	for _, tbl := range db.Tables {
		for _, idx := range tbl.Indices {
			c.Update(tbl.ID, idx.ID, indexusage.NewSample(1, 2, 3, 4))
		}
	}
	c.Flush()
	// Close it. It'll no longer receive any updates.
	dom.StatsHandle().StatsUsage.Close()

	verify := func(exist func(tblPos int, tbl *model.TableInfo, idxPos int, idx *model.IndexInfo) bool) {
		for tblPos, tbl := range db.Tables {
			for idxPos, idx := range tbl.Indices {
				info := dom.StatsHandle().GetIndexUsage(tbl.ID, idx.ID)
				if exist(tblPos, tbl, idxPos, idx) {
					require.Equal(t, uint64(1), info.QueryTotal)
					require.Equal(t, uint64(2), info.KvReqTotal)
					require.Equal(t, uint64(3), info.RowAccessTotal)
					require.Equal(t, [7]uint64{0, 0, 0, 0, 0, 1, 0}, info.PercentageAccess)
				} else {
					require.Equal(t, indexusage.Sample{}, info)
				}
			}
		}
	}

	// before GC
	verify(func(tblPos int, tbl *model.TableInfo, idxPos int, idx *model.IndexInfo) bool {
		return true
	})

	// drop index whose ID is greater or equal than 5
	for _, tbl := range db.Tables {
		for _, idx := range tbl.Indices {
			if idx.ID >= 5 {
				tk.MustExec(fmt.Sprintf("DROP INDEX %s ON %s", idx.Name.String(), tbl.Name.String()))
			}
		}
	}
	require.NoError(t, dom.StatsHandle().GCIndexUsage())
	verify(func(tblPos int, tbl *model.TableInfo, idxPos int, idx *model.IndexInfo) bool {
		return idx.ID < 5
	})

	// drop table whose tblPos is greater or equal than 5
	for tblPos, tbl := range db.Tables {
		if tblPos >= 5 {
			tk.MustExec(fmt.Sprintf("DROP TABLE %s", tbl.Name.String()))
		}
	}
	require.NoError(t, dom.StatsHandle().GCIndexUsage())
	verify(func(tblPos int, tbl *model.TableInfo, idxPos int, idx *model.IndexInfo) bool {
		return idx.ID < 5 && tblPos < 5
	})
}
