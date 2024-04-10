// Copyright 2023 PingCAP, Inc.
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

package storage_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestUpdateStatsMetaVersionForGC(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
		create table t (
			a int,
			b int,
			primary key(a),
			index idx(b)
		)
		partition by range (a) (
			partition p0 values less than (6),
			partition p1 values less than (11),
			partition p2 values less than (16),
			partition p3 values less than (21)
		)
	`)
	testKit.MustExec("insert into t values (1,2),(2,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(
		model.NewCIStr("test"), model.NewCIStr("t"),
	)
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		require.False(t, statsTbl.Pseudo)
	}
	err = h.Update(is)
	require.NoError(t, err)

	// Reset one partition stats.
	p0 := pi.Definitions[0]
	err = h.UpdateStatsMetaVersionForGC(p0.ID)
	require.NoError(t, err)

	// Get partition stats from stats_meta table.
	rows := testKit.MustQuery(
		"select version from mysql.stats_meta where table_id = ?",
		p0.ID,
	).Rows()
	require.Equal(t, 1, len(rows))
	// Parse version from stats_meta.
	version := rows[0][0].(string)

	// Check stats_meta_history again. The version should be the same.
	rows = testKit.MustQuery(
		"select count(*) from mysql.stats_meta_history where table_id = ? and version = ?",
		p0.ID,
		version,
	).Rows()
	require.Equal(t, 1, len(rows))
}
