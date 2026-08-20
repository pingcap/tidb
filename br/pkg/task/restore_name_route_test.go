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
	"testing"

	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/br/pkg/stream"
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestBuildRestoreNamePlanUsesPiTRLatestSourceName(t *testing.T) {
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

	sources := buildPiTRRestoreNameSources(
		history, tracker, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable})
	plan, err := buildRestoreNamePlan(
		router, []*metautil.Database{snapshotDB}, []*metautil.Table{snapshotTable}, sources)
	require.NoError(t, err)
	require.Len(t, plan.tables, 1)
	require.Equal(t, "target_db", plan.tables[0].TargetDB.Name.O)
	require.Equal(t, "target_table", plan.tables[0].TargetInfo.Name.O)
	require.Len(t, plan.databases, 1)
	require.Equal(t, "target_db", plan.databases[0].Target.Name.O)
	require.Equal(t, "snapshot_db", snapshotTable.DB.Name.O)
	require.Equal(t, "snapshot_table", snapshotTable.Info.Name.O)
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

	applyNameRoutesToTableMapping(router, stream.NewTableHistoryManager(), mapping)

	require.Equal(t, "schema_target", mapping.DBReplaceMap[1].Name)
	require.Equal(t, "schema_target", mapping.DBReplaceMap[1].TableMap[11].TargetDBName)
	require.Equal(t, "table_source", mapping.DBReplaceMap[2].Name)
	require.Equal(t, "table_target", mapping.DBReplaceMap[2].TableMap[22].TargetDBName)
	require.Equal(t, int64(0), mapping.DBReplaceMap[2].TableMap[22].TargetDBID)
	require.Equal(t, "copy", mapping.DBReplaceMap[2].TableMap[22].Name)

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
		require.Equal(t, []int64{101, 102, 201}, dbIDs)
	})
}

func TestBuildRestoreNamePlanMatchesLogOnlyTable(t *testing.T) {
	router, err := nameroute.Parse([]string{"log_db.created:target_db.copy"})
	require.NoError(t, err)
	history := stream.NewTableHistoryManager()
	history.RecordDBIdToName(2, "log_db", 100)
	history.AddTableHistory(20, "created", 2, 100)
	tracker := brutils.NewPiTRIdTracker()
	tracker.TrackTableId(2, 20)

	sources := buildPiTRRestoreNameSources(history, tracker, nil, nil)
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

	sources := buildPiTRRestoreNameSources(history, tracker, nil, nil)
	_, err = buildRestoreNamePlan(router, nil, nil, sources)
	require.ErrorContains(t, err, "conflict at target")
}

func TestBuildRestoreNamePlanRejectsSnapshotDependencies(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.t:target.t"})
	require.NoError(t, err)
	db := &metautil.Database{Info: &model.DBInfo{ID: 1, Name: ast.NewCIStr("source")}}
	table := &metautil.Table{DB: db.Info, Info: &model.TableInfo{
		ID:          10,
		Name:        ast.NewCIStr("t"),
		ForeignKeys: []*model.FKInfo{{Name: ast.NewCIStr("fk")}},
	}}
	db.Tables = []*metautil.Table{table}

	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
	require.ErrorContains(t, err, "with foreign keys")
	table.Info.ForeignKeys = nil
	table.Info.View = &model.ViewInfo{}
	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, []*metautil.Table{table}, nil)
	require.ErrorContains(t, err, "does not support selected view")
}

func TestBuildRestoreNamePlanRejectsUnroutedSnapshotDependencies(t *testing.T) {
	router, err := nameroute.Parse([]string{"source.parent:target.copy"})
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
	db.Tables = []*metautil.Table{parent, child}

	_, err = buildRestoreNamePlan(router, []*metautil.Database{db}, db.Tables, nil)
	require.ErrorContains(t, err, "selected table source.child with foreign keys")
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
	require.NoError(t, PreCheckTableTiFlashReplica(t.Context(), nil, []*metautil.Table{table}, nil, true))

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
}
