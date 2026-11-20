// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestShouldForceRestoreMaskingPolicySchema(t *testing.T) {
	tempSysDB := utils.TemporaryDBName(mysql.SystemDB).O

	require.True(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
		RestoreCommonConfig: RestoreCommonConfig{WithSysTable: true},
	}, tempSysDB))
	require.True(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
		RestoreCommonConfig: RestoreCommonConfig{WithSysTable: true},
		NoSchema:            true,
	}, tempSysDB))
	require.False(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
		RestoreCommonConfig: RestoreCommonConfig{WithSysTable: false},
	}, tempSysDB))
	require.False(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
		RestoreCommonConfig: RestoreCommonConfig{WithSysTable: true},
	}, "test"))
}

func TestSetTablesRestoreModeSkipsTemporaryMaskingPolicy(t *testing.T) {
	cfg := &SnapshotRestoreConfig{
		RestoreConfig: &RestoreConfig{
			Config: Config{
				ExplicitFilter: true,
			},
		},
	}
	tempSysDB := utils.TemporaryDBName(mysql.SystemDB).O
	tables := []*metautil.Table{
		{
			DB: &model.DBInfo{Name: pmodel.NewCIStr(tempSysDB)},
			Info: &model.TableInfo{
				Name: pmodel.NewCIStr("tidb_masking_policy"),
				Mode: model.TableModeNormal,
			},
		},
		{
			DB: &model.DBInfo{Name: pmodel.NewCIStr("test")},
			Info: &model.TableInfo{
				Name: pmodel.NewCIStr("t"),
				Mode: model.TableModeNormal,
			},
		},
		{
			DB: &model.DBInfo{Name: pmodel.NewCIStr("test")},
			Info: &model.TableInfo{
				Name:     pmodel.NewCIStr("seq"),
				Mode:     model.TableModeNormal,
				Sequence: &model.SequenceInfo{},
			},
		},
	}

	setTablesRestoreModeIfNeeded(tables, cfg, true, false)

	require.Equal(t, model.TableModeNormal, tables[0].Info.Mode)
	require.Equal(t, model.TableModeRestore, tables[1].Info.Mode)
	require.Equal(t, model.TableModeNormal, tables[2].Info.Mode)
}

func TestEnsureDBMapCoversTables(t *testing.T) {
	dbInfo := &model.DBInfo{ID: 42, Name: pmodel.NewCIStr("test")}
	tableMap := map[int64]*metautil.Table{
		100: {
			DB: dbInfo,
			Info: &model.TableInfo{
				ID:   100,
				Name: pmodel.NewCIStr("t"),
			},
		},
	}
	dbMap := map[int64]*metautil.Database{}

	ensureDBMapCoversTables(tableMap, dbMap)

	require.Contains(t, dbMap, int64(42))
	require.Equal(t, dbInfo, dbMap[42].Info)
}

func TestRestoreMaskingPolicyDDLRespectsSystemTableOption(t *testing.T) {
	tables := []*metautil.Table{{DB: &model.DBInfo{ID: 1, Name: pmodel.NewCIStr("test")}, Info: &model.TableInfo{ID: 2, Name: pmodel.NewCIStr("t")}}}
	jobs := []*model.Job{
		{ID: 1, Type: model.ActionCreateMaskingPolicy, TableID: 2, TableName: "t", SchemaName: "test", BinlogInfo: &model.HistoryInfo{SchemaVersion: 1}},
		{ID: 2, Type: model.ActionAlterMaskingPolicy, TableID: 2, TableName: "t", SchemaName: "test", BinlogInfo: &model.HistoryInfo{SchemaVersion: 2}},
		{ID: 3, Type: model.ActionDropMaskingPolicy, TableID: 2, TableName: "t", SchemaName: "test", BinlogInfo: &model.HistoryInfo{SchemaVersion: 3}},
		{ID: 4, Type: model.ActionAddColumn, TableID: 2, TableName: "t", SchemaName: "test", BinlogInfo: &model.HistoryInfo{SchemaVersion: 4}},
	}
	require.Len(t, filterDDLJobsForRestore(jobs, tables, true), 4)
	filtered := filterDDLJobsForRestore(jobs, tables, false)
	require.Len(t, filtered, 1)
	require.Equal(t, model.ActionAddColumn, filtered[0].Type)
}

func TestSnapshotMaskingPolicyTablesStartProtected(t *testing.T) {
	for _, withSys := range []bool{false, true} {
		cfg := &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{RestoreCommonConfig: RestoreCommonConfig{WithSysTable: withSys}}}
		tables := []*metautil.Table{
			{DB: &model.DBInfo{Name: pmodel.NewCIStr("test")}, Info: &model.TableInfo{Name: pmodel.NewCIStr("t")}},
			{DB: &model.DBInfo{Name: utils.TemporaryDBName(mysql.SystemDB)}, Info: &model.TableInfo{Name: pmodel.NewCIStr("tidb_masking_policy")}},
			{DB: &model.DBInfo{Name: pmodel.NewCIStr("test")}, Info: &model.TableInfo{Name: pmodel.NewCIStr("already_protected"), Mode: model.TableModeImport}},
		}
		setTablesRestoreModeIfNeeded(tables, cfg, false, false)
		if withSys {
			require.Equal(t, model.TableModeRestore, tables[0].Info.Mode)
			require.Contains(t, cfg.maskingPolicyRestoreTables, filter.Table{Schema: "test", Name: "t"})
		} else {
			require.Equal(t, model.TableModeNormal, tables[0].Info.Mode)
			require.Nil(t, cfg.maskingPolicyRestoreTables)
		}
		require.Equal(t, model.TableModeNormal, tables[1].Info.Mode)
		require.Equal(t, model.TableModeImport, tables[2].Info.Mode)
	}
}

func TestSnapshotMaskingPolicySelection(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		manifest   *metautil.MaskingPolicyManifest
		legacyRows uint64
		isPiTR     bool
		wantMode   model.TableMode
		wantStage  bool
	}{
		{"empty manifest", &metautil.MaskingPolicyManifest{TableIDs: []int64{}}, 0, false, model.TableModeNormal, false},
		{"unselected policies", &metautil.MaskingPolicyManifest{TableIDs: []int64{99}},
			1, false, model.TableModeNormal, false},
		{"selected policy", &metautil.MaskingPolicyManifest{TableIDs: []int64{42}}, 1, false, model.TableModeRestore, true},
		{"legacy empty", nil, 0, false, model.TableModeNormal, false},
		{"legacy nonempty", nil, 1, false, model.TableModeRestore, true},
		{"pitr future policies", &metautil.MaskingPolicyManifest{TableIDs: []int64{}}, 0, true, model.TableModeRestore, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{
				RestoreCommonConfig: RestoreCommonConfig{WithSysTable: true},
			}}
			tables := map[int64]*metautil.Table{
				42: {
					DB:   &model.DBInfo{Name: pmodel.NewCIStr("ordinary")},
					Info: &model.TableInfo{ID: 42, Name: pmodel.NewCIStr("t")},
				},
				43: {
					DB:       &model.DBInfo{Name: utils.TemporaryDBName(mysql.SystemDB)},
					Info:     &model.TableInfo{ID: 43, Name: pmodel.NewCIStr("tidb_masking_policy")},
					TotalKvs: testCase.legacyRows,
				},
			}
			configureSnapshotMaskingPolicies(cfg, tables, testCase.manifest, testCase.isPiTR)
			_, hasStage := tables[43]
			require.Equal(t, testCase.wantStage, hasStage)
			selected := utils.Values(tables)
			setTablesRestoreModeIfNeeded(selected, cfg, testCase.isPiTR, false)
			for _, tbl := range selected {
				if tbl.Info.ID == 42 {
					require.Equal(t, testCase.wantMode, tbl.Info.Mode)
				}
			}
		})
	}
}

func TestMaskingPolicySnapshotTargetIsolation(t *testing.T) {
	sourceDB := &metautil.Database{Info: &model.DBInfo{ID: 2, Name: utils.TemporaryDBName(mysql.SystemDB)}}
	source := &metautil.Table{
		DB: sourceDB.Info, Info: &model.TableInfo{ID: 43, Name: pmodel.NewCIStr("tidb_masking_policy")},
	}
	router, err := nameroute.Parse(nil)
	require.NoError(t, err)
	var targets []string
	for _, restoreID := range []uint64{123, 456, 123} {
		plan, err := buildRestoreNamePlan(router, []*metautil.Database{sourceDB}, []*metautil.Table{source}, nil)
		require.NoError(t, err)
		require.NoError(t, isolateMaskingPolicySnapshot(plan, &RestoreConfig{RestoreID: restoreID}))
		require.Equal(t, "mysql", plan.targetTables[0].DB.Name.L)
		targets = append(targets, plan.targetTables[0].Info.Name.L)
		require.Equal(t, "tidb_masking_policy", source.Info.Name.L)
	}
	require.NotEqual(t, targets[0], targets[1])
	require.Equal(t, targets[0], targets[2])
}
