// Copyright 2026 PingCAP, Inc.
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

package task

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/br/pkg/stream"
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestBuildRestoreNamePlanUsesPiTRLatestSourceName(t *testing.T) {
	t.Run("a dropped empty schema does not conflict with its replacement", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"c.u:y.copy"})
		require.NoError(t, err)
		a := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("a"), Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
		oldB := &metautil.Database{Info: &model.DBInfo{ID: 2, Name: ast.NewCIStr("b"), Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
		c := &metautil.Database{Info: &model.DBInfo{ID: 3, Name: ast.NewCIStr("c"), Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
		table := &metautil.Table{DB: a.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("t")}}
		other := &metautil.Table{DB: c.Info, Info: &model.TableInfo{ID: 30, Name: ast.NewCIStr("u")}}
		dbs, tables := []*metautil.Database{a, oldB, c}, []*metautil.Table{table, other}
		history := stream.NewTableHistoryManager()
		history.RecordDBIdToName(4, "b", (1<<60)+30)
		history.AddTableHistory(10, "t", 4, (1<<60)+40)
		tracker := brutils.NewPiTRIdTracker()
		tracker.TrackTableId(4, 10)
		tracker.TrackTableId(3, 30)
		mapping := stream.NewTableMappingManager()
		mapping.DBReplaceMap[4] = &stream.DBReplace{SourceDBInfo: &model.DBInfo{ID: 4, Name: ast.NewCIStr("b"), Charset: "latin1", Collate: "latin1_bin"}}
		deleteSchema := func(dbID int64, ts uint64) {
			entry := &kv.Entry{
				Key:   brutils.EncodeTxnMetaKey([]byte("DBs"), meta.DBkey(dbID), ts),
				Value: codec.EncodeUvarint([]byte{stream.WriteTypeDelete}, ts-1),
			}
			require.NoError(t, mapping.ParseMetaKvAndUpdateIdMapping(entry, consts.WriteCF, ts, history))
		}
		deleteSchema(2, (1<<60)+20)
		sources := buildPiTRRestoreNameSources(history, tracker, dbs, tables, mapping)
		plan, err := buildRestoreNamePlan(router, dbs, tables, sources)
		require.NoError(t, err)
		require.Equal(t, int64(4), plan.tables[0].TargetDB.ID)
		require.Equal(t, "latin1", plan.tables[0].TargetDB.Charset)
		require.Equal(t, "latin1_bin", plan.tables[0].TargetDB.Collate)
		require.Equal(t, "utf8mb4", oldB.Info.Charset)
		for _, dbPlan := range plan.databases {
			require.NotEqual(t, int64(2), dbPlan.Source.Info.ID)
		}
		// An expired original parent is also unnecessary once its table is
		// created directly in the latest destination. A live empty a is kept.
		deleteSchema(1, (1<<60)+50)
		sources = buildPiTRRestoreNameSources(history, tracker, dbs, tables, mapping)
		plan, err = buildRestoreNamePlan(router, dbs, tables, sources)
		require.NoError(t, err)
		require.Len(t, plan.databases, 2)
	})
	t.Run("an unrelated route preserves the latest source database settings", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"c.u:y.copy"})
		require.NoError(t, err)
		a := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("a"), Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
		b := &metautil.Database{Info: &model.DBInfo{ID: 2, Name: ast.NewCIStr("b"), Charset: "latin1", Collate: "latin1_bin"}}
		c := &metautil.Database{Info: &model.DBInfo{ID: 3, Name: ast.NewCIStr("c"), Charset: "utf8mb4", Collate: "utf8mb4_bin"}}
		table := &metautil.Table{DB: a.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("t")}}
		other := &metautil.Table{DB: c.Info, Info: &model.TableInfo{ID: 30, Name: ast.NewCIStr("u")}}
		dbs, tables := []*metautil.Database{a, b, c}, []*metautil.Table{table, other}
		history := stream.NewTableHistoryManager()
		history.AddTableHistory(10, "t", 2, 100)
		tracker := brutils.NewPiTRIdTracker()
		tracker.TrackTableId(2, 10)
		tracker.TrackTableId(3, 30)
		sources := buildPiTRRestoreNameSources(history, tracker, dbs, tables, nil)
		plan, err := buildRestoreNamePlan(router, dbs, tables, sources)
		require.NoError(t, err)
		require.Equal(t, "b", plan.tables[0].TargetDB.Name.O)
		require.Equal(t, "latin1", plan.tables[0].TargetDB.Charset)
		require.Equal(t, "latin1_bin", plan.tables[0].TargetDB.Collate)
		require.Equal(t, "a", table.DB.Name.O)
		require.Equal(t, "utf8mb4", table.DB.Charset)
		// A recreated schema can reuse the name of an older schema ID.
		history.RecordDBIdToName(4, "a", 200)
		history.AddTableHistory(10, "t", 4, 200)
		tracker.TrackTableId(4, 10)
		mapping := stream.NewTableMappingManager()
		mapping.DBReplaceMap[4] = &stream.DBReplace{SourceDBInfo: &model.DBInfo{ID: 4, Name: ast.NewCIStr("a"), Charset: "latin1", Collate: "latin1_bin"}}
		sources = buildPiTRRestoreNameSources(history, tracker, dbs, tables, mapping)
		plan, err = buildRestoreNamePlan(router, dbs, tables, sources)
		require.NoError(t, err)
		require.Equal(t, int64(4), plan.tables[0].TargetDB.ID)
		require.Equal(t, "latin1", plan.tables[0].TargetDB.Charset)
	})

	router, err := nameroute.Parse([]string{"latest_db.latest_table:target_db.target_table"})
	require.NoError(t, err)

	snapshotDB := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("snapshot_db")}}
	snapshotTable := &metautil.Table{
		DB: snapshotDB.Info,
		Info: &model.TableInfo{
			ID:   10,
			Name: ast.NewCIStr("snapshot_table"),
		},
	}
	snapshotDB.Tables = []*metautil.Table{snapshotTable}
	history := stream.NewTableHistoryManager()
	history.RecordDBIdToName(2, "latest_db", 100)
	history.AddTableHistory(10, "latest_table", 2, 100)
	tracker := brutils.NewPiTRIdTracker()
	tracker.TrackTableId(2, 10)

	mapping := stream.NewTableMappingManager()
	mapping.DBReplaceMap[2] = &stream.DBReplace{SourceDBInfo: &model.DBInfo{ID: 2, Name: ast.NewCIStr("latest_db")}}
	sources := buildPiTRRestoreNameSources(
		history, tracker, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, mapping)
	plan, err := buildRestoreNamePlan(
		router, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, sources)
	require.NoError(t, err)
	require.Len(t, plan.tables, 1)
	require.Equal(t, "target_db", plan.tables[0].TargetDB.Name.O)
	require.Equal(t, "target_table", plan.tables[0].TargetInfo.Name.O)
	require.Len(t, plan.databases, 2)
	require.Equal(t, "target_db", plan.databases[0].Target.Name.O)
	require.Equal(t, "snapshot_db", plan.databases[1].Target.Name.O)
	require.Equal(t, "snapshot_db", snapshotTable.DB.Name.O)
	require.Equal(t, "snapshot_table", snapshotTable.Info.Name.O)
}

func TestBuildPiTRRestoreNameSourcesFallsBackToSnapshotDB(t *testing.T) {
	snapshotDB := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("snapshot_db")}}
	snapshotTable := &metautil.Table{
		DB:   snapshotDB.Info,
		Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("snapshot_table")},
	}
	history := stream.NewTableHistoryManager()
	history.AddTableHistory(10, "latest_table", 1, 100)
	tracker := brutils.NewPiTRIdTracker()
	tracker.TrackTableId(1, 10)

	sources := buildPiTRRestoreNameSources(
		history, tracker, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, nil)
	require.Equal(t, nameroute.ObjectName{
		Schema: ast.NewCIStr("snapshot_db"),
		Table:  ast.NewCIStr("latest_table"),
	}, sources.tables[10])

	t.Run("includes a selected empty snapshot database", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"empty_src:empty_dst"})
		require.NoError(t, err)
		emptyDB := &metautil.Database{Info: &model.DBInfo{ID: 7, Name: ast.NewCIStr("empty_src")}}
		// No log history and not tracked: the selected snapshot schema must still
		// participate in routing.
		sources := buildPiTRRestoreNameSources(
			stream.NewTableHistoryManager(), brutils.NewPiTRIdTracker(),
			[]*metautil.Database{emptyDB}, nil, nil)
		require.Equal(t, ast.NewCIStr("empty_src"), sources.databases[7])

		plan, err := buildRestoreNamePlan(router, []*metautil.Database{emptyDB}, nil, sources)
		require.NoError(t, err)
		require.Len(t, plan.databases, 1)
		require.Equal(t, "empty_dst", plan.databases[0].Target.Name.O)
	})

	t.Run("existence precheck uses the latest routed target", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"a.new:b.copy"})
		require.NoError(t, err)
		history := stream.NewTableHistoryManager()
		history.RecordDBIdToName(1, "a", 100)
		history.AddTableHistory(10, "new", 1, 100)
		tracker := brutils.NewPiTRIdTracker()
		tracker.TrackTableId(1, 10)
		snapshotDB := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("a")}}
		snapshotTable := &metautil.Table{
			DB:   snapshotDB.Info,
			Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("old")},
		}
		sources := buildPiTRRestoreNameSources(
			history, tracker, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, nil)

		// The table was renamed a.old -> a.new, so the only target to check is
		// b.copy; the pre-rename a.old must not be checked as an identity target.
		targets := routeLatestSourceTables(router, sources)
		require.Len(t, targets, 1)
		require.Equal(t, "b", targets[0].DB.Name.O)
		require.Equal(t, "copy", targets[0].Info.Name.O)
		require.Nil(t, routeLatestSourceTables(router, nil))
	})
}

func TestApplyNameRoutesDistinguishesSchemaAndExactTableRules(t *testing.T) {
	router, err := nameroute.Parse([]string{
		"schema_source:schema_target",
		"table_source.original:table_target.copy",
	})
	require.NoError(t, err)

	mapping := stream.NewTableMappingManager()
	mapping.DBReplaceMap = map[stream.UpstreamID]*stream.DBReplace{
		1: {
			Name: "schema_source",
			DbID: -1,
			TableMap: map[stream.UpstreamID]*stream.TableReplace{
				11: {Name: "t", TableID: -11},
			},
		},
		2: {
			Name: "table_source",
			DbID: -2,
			TableMap: map[stream.UpstreamID]*stream.TableReplace{
				22: {Name: "original", TableID: -22},
			},
		},
		3: {
			Name:        "table_target",
			DbID:        -3,
			TableMap:    map[stream.UpstreamID]*stream.TableReplace{},
			FilteredOut: true,
		},
	}

	require.NoError(t, applyNameRoutesToTableMapping(router, stream.NewTableHistoryManager(), mapping, nil))

	require.Equal(t, "schema_target", mapping.DBReplaceMap[1].Name)
	require.Equal(t, "schema_target", mapping.DBReplaceMap[1].TableMap[11].TargetDBName)
	require.Equal(t, "table_source", mapping.DBReplaceMap[2].Name)
	require.Equal(t, "table_target", mapping.DBReplaceMap[2].TableMap[22].TargetDBName)
	require.Equal(t, int64(0), mapping.DBReplaceMap[2].TableMap[22].TargetDBID)
	require.Equal(t, "copy", mapping.DBReplaceMap[2].TableMap[22].Name)
	require.True(t, mapping.DBReplaceMap[1].SchemaRouted)
	require.False(t, mapping.DBReplaceMap[2].SchemaRouted)

	t.Run("schema rule restores its schema when all tables are overridden", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"a:x", "a.t1:y.t1"})
		require.NoError(t, err)
		db := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("a")}}
		table := &metautil.Table{DB: db.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("t1")}}
		db.Tables = []*metautil.Table{table}

		plan, err := buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
		require.NoError(t, err)
		require.Len(t, plan.tables, 1)
		require.Equal(t, "y", plan.tables[0].TargetDB.Name.O)
		require.Equal(t, "t1", plan.tables[0].TargetInfo.Name.O)

		// Both the schema rule target x and the overridden table target y exist.
		dbNames := make([]string, 0, len(plan.databases))
		for _, planDB := range plan.databases {
			dbNames = append(dbNames, planDB.Target.Name.O)
		}
		slices.Sort(dbNames)
		require.Equal(t, []string{"x", "y"}, dbNames)
	})

	t.Run("blocklist includes routed target databases", func(t *testing.T) {
		manager := stream.NewTableMappingManager()
		manager.DBReplaceMap = map[stream.UpstreamID]*stream.DBReplace{
			1: {
				DbID: 101,
				TableMap: map[stream.UpstreamID]*stream.TableReplace{
					11: {
						TableID:      301,
						TargetDBName: "target",
						TargetDBID:   201,
						PartitionMap: map[stream.UpstreamID]stream.DownstreamID{111: 401},
					},
					12: {TableID: 302},
					13: {TableID: 303, TargetDBID: 999, FilteredOut: true},
				},
			},
			2: {
				DbID: 102,
				TableMap: map[stream.UpstreamID]*stream.TableReplace{
					21: {TableID: 301, TargetDBName: "TARGET", TargetDBID: 201},
				},
			},
			3: {DbID: 103, FilteredOut: true},
		}

		tableIDs, dbIDs := collectLogRestoreBlocklistIDs(manager)
		require.Equal(t, []int64{301, 302, 401}, tableIDs)
		// 101 restores its own schema and 201 is the routed target; 102's schema
		// is not created because all of its tables are routed elsewhere.
		require.Equal(t, []int64{101, 201}, dbIDs)
	})
}

func TestApplyNameRoutesUsesSnapshotSourceNames(t *testing.T) {
	t.Run("unknown latest schema does not borrow a historical name", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"old.t:wrong.copy"})
		require.NoError(t, err)
		mapping := stream.NewTableMappingManager()
		mapping.DBReplaceMap[1] = stream.NewDBReplace("old", -1)
		mapping.DBReplaceMap[2] = stream.NewDBReplace("", -2)
		mapping.DBReplaceMap[1].TableMap[22] = stream.NewTableReplace("t", -3)
		mapping.DBReplaceMap[2].TableMap[22] = stream.NewTableReplace("t", -3)
		history := stream.NewTableHistoryManager()
		history.AddTableHistory(22, "t", 2, 20)
		require.NoError(t, applyNameRoutesToTableMapping(router, history, mapping, nil))
		require.Empty(t, mapping.DBReplaceMap[1].TableMap[22].TargetDBName)
		require.Empty(t, mapping.DBReplaceMap[2].TableMap[22].TargetDBName)
	})

	t.Run("seed snapshot-only source parents before table routing", func(t *testing.T) {
		router, err := nameroute.Parse([]string{"source.t:target.copy", "schema_source:schema_target"})
		require.NoError(t, err)
		mapping := stream.NewTableMappingManager()
		sources := &restoreNameSources{
			databases: map[int64]ast.CIStr{1: ast.NewCIStr("source"), 2: ast.NewCIStr("schema_source")},
			tables:    map[int64]nameroute.ObjectName{},
		}
		require.NoError(t, applyNameRoutesToTableMapping(router, stream.NewTableHistoryManager(), mapping, sources))
		require.Equal(t, "source", mapping.DBReplaceMap[1].Name)
		require.Equal(t, "source", mapping.DBReplaceMap[1].SourceName)
		require.Equal(t, "schema_target", mapping.DBReplaceMap[2].Name)
		require.Equal(t, "schema_source", mapping.DBReplaceMap[2].SourceName)
		require.True(t, mapping.DBReplaceMap[2].SchemaRouted)
	})

	router, err := nameroute.Parse([]string{"test.t:target.copy"})
	require.NoError(t, err)

	newMapping := func() *stream.TableMappingManager {
		mapping := stream.NewTableMappingManager()
		mapping.DBReplaceMap = map[stream.UpstreamID]*stream.DBReplace{
			130: {
				// The log scan saw only a table DDL, so the schema has no name.
				Name: "",
				DbID: -130,
				TableMap: map[stream.UpstreamID]*stream.TableReplace{
					135: {Name: "t", TableID: -135},
				},
			},
		}
		return mapping
	}

	snapshotDB := &metautil.Database{Info: &model.DBInfo{ID: 130, Name: ast.NewCIStr("test")}}
	snapshotTable := &metautil.Table{
		DB:   snapshotDB.Info,
		Info: &model.TableInfo{ID: 135, Name: ast.NewCIStr("t")},
	}
	history := stream.NewTableHistoryManager()
	sources := buildPiTRRestoreNameSources(history, brutils.NewPiTRIdTracker(),
		[]*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, nil)

	t.Run("log-only resolution drops the route", func(t *testing.T) {
		mapping := newMapping()
		require.NoError(t, applyNameRoutesToTableMapping(router, history, mapping, nil))
		tableReplace := mapping.DBReplaceMap[130].TableMap[135]
		require.Equal(t, "t", tableReplace.Name)
		require.Empty(t, tableReplace.TargetDBName)
	})

	t.Run("snapshot sources bind the route", func(t *testing.T) {
		mapping := newMapping()
		require.NoError(t, applyNameRoutesToTableMapping(router, history, mapping, sources))
		tableReplace := mapping.DBReplaceMap[130].TableMap[135]
		require.Equal(t, "copy", tableReplace.Name)
		require.Equal(t, "target", tableReplace.TargetDBName)

		// The snapshot phase resolves the target database ID afterwards;
		// MergeBaseDBReplace must keep the bound target table name and attach it.
		mapping.MergeBaseDBReplace(map[stream.UpstreamID]*stream.DBReplace{
			130: {
				Name: "target",
				DbID: 142,
				TableMap: map[stream.UpstreamID]*stream.TableReplace{
					135: {Name: "copy", TableID: 138, TargetDBName: "target", TargetDBID: 142},
				},
			},
		})
		merged := mapping.DBReplaceMap[130].TableMap[135]
		require.Equal(t, "copy", merged.Name)
		require.Equal(t, "target", merged.TargetDBName)
		require.Equal(t, stream.DownstreamID(142), merged.TargetDBID)
	})
}

func TestNameRouterCacheTracksRename(t *testing.T) {
	cfg := DefaultRestoreConfig(DefaultConfig())
	cfg.WithSysTable = false

	// DefaultRestoreConfig parses (and validates) flags, caching an empty router.
	router, err := cfg.getNameRouter()
	require.NoError(t, err)
	_, _, matched := router.Route(ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.False(t, matched)

	// A programmatic caller may assign Rename after parsing; routing must follow.
	cfg.Rename = []string{"test.t:target.copy"}
	router, err = cfg.getNameRouter()
	require.NoError(t, err)
	targetSchema, targetTable, matched := router.Route(ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.True(t, matched)
	require.Equal(t, "target", targetSchema.O)
	require.Equal(t, "copy", targetTable.O)
	require.True(t, cfg.hasNameRouting())

	// Changing the rules again must invalidate the cached router.
	cfg.Rename = []string{"test.t:other.other_copy"}
	router, err = cfg.getNameRouter()
	require.NoError(t, err)
	targetSchema, targetTable, matched = router.Route(ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.True(t, matched)
	require.Equal(t, "other", targetSchema.O)
	require.Equal(t, "other_copy", targetTable.O)
}

func TestBuildRestoreNamePlanMatchesLogOnlyTable(t *testing.T) {
	router, err := nameroute.Parse([]string{"log_db.created:target_db.copy"})
	require.NoError(t, err)
	history := stream.NewTableHistoryManager()
	history.RecordDBIdToName(2, "log_db", 100)
	history.AddTableHistory(20, "created", 2, 100)
	tracker := brutils.NewPiTRIdTracker()
	tracker.TrackTableId(2, 20)

	sources := buildPiTRRestoreNameSources(history, tracker, nil, nil, nil)
	plan, err := buildRestoreNamePlan(router, nil, nil, sources)
	require.NoError(t, err)
	require.Empty(t, plan.databases)
	require.Empty(t, plan.tables)
}

func TestBuildRestoreNamePlanChecksUnifiedPiTRTargets(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.t:target.t"})
	require.NoError(t, err)
	history := stream.NewTableHistoryManager()
	history.RecordDBIdToName(1, "source", 100)
	history.RecordDBIdToName(2, "target", 100)
	history.AddTableHistory(10, "t", 1, 100)
	history.AddTableHistory(20, "t", 2, 100)
	tracker := brutils.NewPiTRIdTracker()
	tracker.TrackTableId(1, 10)
	tracker.TrackTableId(2, 20)

	sources := buildPiTRRestoreNameSources(history, tracker, nil, nil, nil)
	_, err = buildRestoreNamePlan(router, nil, nil, sources)
	require.ErrorContains(t, err, "conflict at target")
}

func TestBuildRestoreNamePlanRejectsSnapshotDependencies(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.t:target.t"})
	require.NoError(t, err)
	db := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("source")}}
	table := &metautil.Table{DB: db.Info, Info: &model.TableInfo{
		ID:   10,
		Name: ast.NewCIStr("t"),
		ForeignKeys: []*model.FKInfo{{
			Name:      ast.NewCIStr("fk"),
			RefSchema: ast.NewCIStr("source"),
			RefTable:  ast.NewCIStr("t"),
		}},
	}}
	db.Tables = []*metautil.Table{table}

	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
	require.ErrorContains(t, err, "with foreign keys")
	table.Info.ForeignKeys = nil
	table.Info.View = &model.ViewInfo{}
	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
	require.ErrorContains(t, err, "does not support selected view")
}

func TestBuildRestoreNamePlanChecksSnapshotDependencies(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.unrelated:target.copy"})
	require.NoError(t, err)
	db := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("source")}}
	parent := &metautil.Table{DB: db.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("parent")}}
	child := &metautil.Table{DB: db.Info, Info: &model.TableInfo{
		ID:   11,
		Name: ast.NewCIStr("child"),
		ForeignKeys: []*model.FKInfo{{
			Name:      ast.NewCIStr("fk_parent"),
			RefSchema: ast.NewCIStr("source"),
			RefTable:  ast.NewCIStr("parent"),
		}},
	}}
	unrelated := &metautil.Table{DB: db.Info, Info: &model.TableInfo{ID: 12, Name: ast.NewCIStr("unrelated")}}
	db.Tables = []*metautil.Table{parent, child, unrelated}

	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, db.Tables, nil)
	require.NoError(t, err)

	// Renaming the child itself does not change the FK reference target.
	router, err = nameroute.Parse([]string{"source.child:target.child"})
	require.NoError(t, err)
	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, db.Tables, nil)
	require.NoError(t, err)

	// A route affecting the FK parent is unsafe even when the child itself is
	// identity-mapped, because this first-stage implementation preserves the FK
	// reference verbatim.
	router, err = nameroute.Parse([]string{"source.parent:target.parent"})
	require.NoError(t, err)
	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, db.Tables, nil)
	require.ErrorContains(t, err, "selected table source.child with foreign keys")
}

func TestHasRoutedForeignKey(t *testing.T) {
	router, err := nameroute.Parse([]string{"other.t:target.t"})
	require.NoError(t, err)
	table := &model.TableInfo{
		Name: ast.NewCIStr("child"),
		ForeignKeys: []*model.FKInfo{{
			RefSchema: ast.NewCIStr("source"),
			RefTable:  ast.NewCIStr("parent"),
		}},
	}
	require.False(t, hasRoutedForeignKey(router, nameroute.ObjectName{
		Schema: ast.NewCIStr("source"), Table: ast.NewCIStr("child"),
	}, table))

	router, err = nameroute.Parse([]string{"source.child:target.child"})
	require.NoError(t, err)
	require.False(t, hasRoutedForeignKey(router, nameroute.ObjectName{
		Schema: ast.NewCIStr("source"), Table: ast.NewCIStr("child"),
	}, table))
}

func TestBuildRestoreNamePlanChoosesTargetDBMetadataDeterministically(t *testing.T) {
	router, err := nameroute.Parse([]string{
		"b.t:target.b",
		"a.t:target.a",
	})
	require.NoError(t, err)
	dbA := &metautil.Database{Info: &model.DBInfo{
		ID: 2, Name: ast.NewCIStr("a"), Charset: "utf8mb4", Collate: "utf8mb4_bin",
		PlacementPolicyRef: &model.PolicyRefInfo{ID: 1, Name: ast.NewCIStr("primary")},
	}}
	dbB := &metautil.Database{Info: &model.DBInfo{
		ID: 1, Name: ast.NewCIStr("b"), Charset: "UTF8MB4", Collate: "UTF8MB4_BIN",
		PlacementPolicyRef: &model.PolicyRefInfo{ID: 2, Name: ast.NewCIStr("PRIMARY")},
	}}
	tableA := &metautil.Table{DB: dbA.Info, Info: &model.TableInfo{ID: 20, Name: ast.NewCIStr("t")}}
	tableB := &metautil.Table{DB: dbB.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("t")}}

	plan, err := buildRestoreNamePlan(router,
		[]*metautil.Database{dbB, dbA}, []*metautil.Table{tableB, tableA}, nil)
	require.NoError(t, err)
	require.Len(t, plan.databases, 1)
	require.Same(t, dbA, plan.databases[0].Source)
	require.Equal(t, "target", plan.databases[0].Target.Name.O)
}

func TestBuildRestoreNamePlanRejectsIncompatibleMergedDatabaseSettings(t *testing.T) {
	router, err := nameroute.Parse([]string{"a.t:target.a", "b.t:target.b"})
	require.NoError(t, err)
	base := &model.DBInfo{
		ID: 1, Name: ast.NewCIStr("a"), Charset: "utf8mb4", Collate: "utf8mb4_bin",
		PlacementPolicyRef: &model.PolicyRefInfo{ID: 1, Name: ast.NewCIStr("primary")},
	}
	tests := []struct {
		name   string
		mutate func(*model.DBInfo)
	}{
		{name: "charset", mutate: func(info *model.DBInfo) { info.Charset = "latin1" }},
		{name: "collation", mutate: func(info *model.DBInfo) { info.Collate = "utf8mb4_general_ci" }},
		{name: "placement policy", mutate: func(info *model.DBInfo) {
			info.PlacementPolicyRef = &model.PolicyRefInfo{ID: 2, Name: ast.NewCIStr("secondary")}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbA := &metautil.Database{Info: base.Clone()}
			dbBInfo := base.Clone()
			dbBInfo.ID = 2
			dbBInfo.Name = ast.NewCIStr("b")
			test.mutate(dbBInfo)
			dbB := &metautil.Database{Info: dbBInfo}
			tableA := &metautil.Table{DB: dbA.Info, Info: &model.TableInfo{ID: 10, Name: ast.NewCIStr("t")}}
			tableB := &metautil.Table{DB: dbB.Info, Info: &model.TableInfo{ID: 20, Name: ast.NewCIStr("t")}}

			_, err := buildRestoreNamePlan(router,
				[]*metautil.Database{dbA, dbB}, []*metautil.Table{tableA, tableB}, nil)
			require.ErrorContains(t, err, "source schemas a and b routed to target schema target")
			require.ErrorContains(t, err, "incompatible charset, collation, or placement policy")
		})
	}
}

func TestBuildRestoreNamePlanUsesPrecheckedTiFlashMetadata(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.t:target.copy"})
	require.NoError(t, err)
	db := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("source")}}
	table := &metautil.Table{DB: db.Info, Info: &model.TableInfo{
		ID:             10,
		Name:           ast.NewCIStr("t"),
		TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1, Available: true},
	}}
	require.NoError(t, PreCheckTableTiFlashReplica(t.Context(), nil, []*metautil.Table{table}, nil, true, "ON"))

	plan, err := buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
	require.NoError(t, err)
	require.Nil(t, plan.tables[0].TargetInfo.TiFlashReplica)
}

func TestRawRestoreRejectsInheritedRenameFlag(t *testing.T) {
	flags := pflag.NewFlagSet("raw", pflag.ContinueOnError)
	flags.StringArray(FlagRename, nil, "")
	require.NoError(t, flags.Set(FlagRename, "source:target"))
	err := (&RestoreRawConfig{}).ParseFromFlags(flags)
	require.ErrorContains(t, err, "not supported by raw or transactional KV restore")
}

func TestPiTRTargetTablesForExistenceCheck(t *testing.T) {
	t.Run("missing tracker cannot silently skip identity checks", func(t *testing.T) {
		_, err := pitrTargetTablesForExistenceCheck(&RestoreConfig{}, nil)
		require.ErrorContains(t, err, "PiTR table tracker is missing")
	})
	t.Run("empty selection has no log targets", func(t *testing.T) {
		cfg := &RestoreConfig{}
		cfg.PiTRTableTracker = brutils.NewPiTRIdTracker()
		targets, err := pitrTargetTablesForExistenceCheck(cfg, nil)
		require.NoError(t, err)
		require.Empty(t, targets)
	})
	t.Run("identity tables created after the snapshot are checked", func(t *testing.T) {
		cfg := &RestoreConfig{}
		cfg.PiTRTableTracker = brutils.NewPiTRIdTracker()
		cfg.PiTRTableTracker.TrackTableName("source", "table3")
		// Without --rename, runSnapshotRestore does not build nameSources.
		// table3 has no snapshot metadata, but must fail the same preflight as
		// snapshot tables when its name already exists downstream.
		targets, err := pitrTargetTablesForExistenceCheck(cfg, nil)
		require.NoError(t, err)
		require.Len(t, targets, 1)
		require.Equal(t, "source", targets[0].DB.Name.O)
		require.Equal(t, "table3", targets[0].Info.Name.O)
	})

	for _, testCase := range []struct {
		name       string
		rules      []string
		targetDB   string
		targetName string
	}{
		{name: "route latest name without checking a historical alias", rules: []string{"source.latest:target.copy"}, targetDB: "target", targetName: "copy"},
		{name: "an unrelated route retains identity targets", rules: []string{"other.t:target.copy"}, targetDB: "source", targetName: "latest"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := &RestoreConfig{}
			cfg.Rename = testCase.rules
			cfg.PiTRTableTracker = brutils.NewPiTRIdTracker()
			cfg.PiTRTableTracker.TrackTableName("source", "old")
			cfg.PiTRTableTracker.TrackTableName("source", "latest")
			sources := &restoreNameSources{tables: map[int64]nameroute.ObjectName{
				10: {Schema: ast.NewCIStr("source"), Table: ast.NewCIStr("latest")},
			}}
			targets, err := pitrTargetTablesForExistenceCheck(cfg, sources)
			require.NoError(t, err)
			require.Len(t, targets, 1)
			require.Equal(t, testCase.targetDB, targets[0].DB.Name.O)
			require.Equal(t, testCase.targetName, targets[0].Info.Name.O)
		})
	}
}

func TestShouldCheckRestoreTargetExistence(t *testing.T) {
	require.True(t, shouldCheckRestoreTargetExistence(true, false, false, false))
	require.True(t, shouldCheckRestoreTargetExistence(false, false, false, true))
	require.False(t, shouldCheckRestoreTargetExistence(false, false, false, false))
	require.False(t, shouldCheckRestoreTargetExistence(true, true, false, true))
	require.False(t, shouldCheckRestoreTargetExistence(true, false, true, true))
}

func TestValidateRenameRuleSchemas(t *testing.T) {
	unsupportedSchemas := []string{
		"mysql",
		"SYS",
		"workload_schema",
		"INFORMATION_SCHEMA",
		"performance_schema",
		"MeTrIcS_ScHeMa",
		"dm_heartbeat",
		"inspection_schema",
		"__TiDB_BR_Temporary_mysql",
		"__tidb_br_temporary_UserSchema",
		checkpoint.LogRestoreCheckpointDatabaseName + "_42",
		checkpoint.SnapshotRestoreCheckpointDatabaseName + "_42",
		checkpoint.CustomSSTRestoreCheckpointDatabaseName + "_42",
	}

	for _, schema := range unsupportedSchemas {
		for _, rule := range []string{
			fmt.Sprintf("%s.t:user_target.t", schema),
			fmt.Sprintf("user_source.t:%s.t", schema),
		} {
			t.Run(rule, func(t *testing.T) {
				router, err := nameroute.Parse([]string{rule})
				require.NoError(t, err)
				err = validateRenameRuleSchemas(router)
				require.ErrorContains(t, err, "does not support system, temporary system, or checkpoint schema")
			})
		}
	}

	router, err := nameroute.Parse([]string{"user_source.t:user_target.t"})
	require.NoError(t, err)
	require.NoError(t, validateRenameRuleSchemas(router))
}

func TestRestoreConfigNameRoutingValidationAndHash(t *testing.T) {
	t.Run("reject incompatible modes", func(t *testing.T) {
		testCases := []struct {
			name    string
			cfg     RestoreConfig
			cmdName string
			errText string
		}{
			{
				name:    "no schema",
				cfg:     RestoreConfig{Rename: []string{"source:target"}, NoSchema: true},
				errText: "--rename cannot be used with --no-schema",
			},
			{
				name: "system tables",
				cfg: RestoreConfig{
					Rename: []string{"source:target"},
					RestoreCommonConfig: RestoreCommonConfig{
						WithSysTable: true,
					},
				},
				errText: "--rename requires --with-sys-table=false",
			},
			{
				name:    "EBS",
				cfg:     RestoreConfig{Rename: []string{"source:target"}, FullBackupType: FullBackupTypeEBS},
				errText: "--rename is not supported by EBS restore",
			},
			{
				name:    "raw",
				cfg:     RestoreConfig{Rename: []string{"source:target"}},
				cmdName: RawRestoreCmd,
				errText: "--rename is not supported by Raw Restore",
			},
			{
				name:    "txn",
				cfg:     RestoreConfig{Rename: []string{"source:target"}},
				cmdName: TxnRestoreCmd,
				errText: "--rename is not supported by Txn Restore",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.cfg.validateNameRouting(tc.cmdName)
				require.ErrorContains(t, err, tc.errText)
			})
		}
	})

	t.Run("canonical config hash", func(t *testing.T) {
		first := &RestoreConfig{Rename: []string{"source_b:target_b", "source_a:target_a"}}
		reordered := &RestoreConfig{Rename: []string{"source_a:target_a", "source_b:target_b"}}
		changed := &RestoreConfig{Rename: []string{"source_a:target_a", "source_b:other_target"}}

		firstHash, err := first.Hash(FullRestoreCmd)
		require.NoError(t, err)
		reorderedHash, err := reordered.Hash(FullRestoreCmd)
		require.NoError(t, err)
		changedHash, err := changed.Hash(FullRestoreCmd)
		require.NoError(t, err)

		require.Equal(t, firstHash, reorderedHash)
		require.NotEqual(t, firstHash, changedHash)
	})

	t.Run("empty rename preserves legacy config hash", func(t *testing.T) {
		cfg := &RestoreConfig{
			UpstreamClusterID: 42,
			Config: Config{
				Storage:   "s3://bucket/prefix?access-key=secret",
				FilterStr: []string{"source.*"},
			},
			RestoreCommonConfig: RestoreCommonConfig{
				WithSysTable: true,
			},
			FastLoadSysTables: true,
			LoadStats:         true,
		}

		actual, err := cfg.Hash(FullRestoreCmd)
		require.NoError(t, err)

		// This is the immutable config serialized by BR before rename support.
		legacyConfig := struct {
			CmdName           string
			UpstreamClusterID uint64
			Storage           string
			ExplictFilter     bool
			FilterStr         []string
			WithSysTable      bool
			FastLoadSysTables bool
			LoadStats         bool
		}{
			CmdName:           FullRestoreCmd,
			UpstreamClusterID: cfg.UpstreamClusterID,
			Storage:           ast.RedactURL(cfg.Storage),
			FilterStr:         cfg.FilterStr,
			WithSysTable:      cfg.WithSysTable,
			FastLoadSysTables: cfg.FastLoadSysTables,
			LoadStats:         cfg.LoadStats,
		}
		legacyJSON, err := json.Marshal(legacyConfig)
		require.NoError(t, err)
		legacyHash := sha256.Sum256(legacyJSON)
		require.Equal(t, legacyHash[:], actual)
	})

	t.Run("partial restore covers filter and rename", func(t *testing.T) {
		require.False(t, (&RestoreConfig{}).isPartialRestore())
		require.True(t, (&RestoreConfig{Config: Config{ExplicitFilter: true}}).isPartialRestore())
		require.True(t, (&RestoreConfig{Rename: []string{"source:target"}}).isPartialRestore())
		require.True(t, (&RestoreConfig{
			Config: Config{ExplicitFilter: true},
			Rename: []string{"source:target"},
		}).isPartialRestore())
	})

	t.Run("route-only PiTR sets tables to restore mode", func(t *testing.T) {
		cfg := &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{Rename: []string{"source:target"}}}
		tables := []*metautil.Table{{
			DB:   &model.DBInfo{Name: ast.NewCIStr("source")},
			Info: &model.TableInfo{ID: 1, Name: ast.NewCIStr("t")},
		}}
		setTablesRestoreModeIfNeeded(tables, cfg, true, false)
		require.Equal(t, model.TableModeRestore, tables[0].Info.Mode)

		// A full (non-partial) restore keeps the table mode unchanged.
		tables[0].Info.Mode = model.TableModeNormal
		setTablesRestoreModeIfNeeded(tables, &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{}}, true, false)
		require.Equal(t, model.TableModeNormal, tables[0].Info.Mode)
	})
}
