// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestShouldForceRestoreMaskingPolicySchema(t *testing.T) {
	tempSysDB := utils.TemporaryDBName(mysql.SystemDB).O

	require.True(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
		RestoreCommonConfig: RestoreCommonConfig{WithSysTable: true},
	}, tempSysDB))
	require.False(t, shouldForceRestoreMaskingPolicySchema(&RestoreConfig{
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
