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

package mvservice

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestLatestServerInfosByInstanceKeepsNewestDDLID(t *testing.T) {
	allServers := map[string]*serverinfo.ServerInfo{
		"old-node": {
			StaticInfo: serverinfo.StaticInfo{
				ID:             "old-node",
				IP:             "127.0.0.1",
				Port:           4000,
				StartTimestamp: 100,
			},
		},
		"new-node": {
			StaticInfo: serverinfo.StaticInfo{
				ID:             "new-node",
				IP:             "127.0.0.1",
				Port:           4000,
				StartTimestamp: 200,
			},
		},
		"peer-node": {
			StaticInfo: serverinfo.StaticInfo{
				ID:             "peer-node",
				IP:             "127.0.0.2",
				Port:           4000,
				StartTimestamp: 150,
			},
		},
	}

	got := latestServerInfosByInstance(allServers)

	require.Len(t, got, 2)
	require.NotContains(t, got, "old-node")
	require.Equal(t, serverInfo{
		ID:             "new-node",
		IP:             "127.0.0.1",
		Port:           4000,
		StartTimestamp: 200,
	}, got["new-node"])
	require.Equal(t, serverInfo{
		ID:             "peer-node",
		IP:             "127.0.0.2",
		Port:           4000,
		StartTimestamp: 150,
	}, got["peer-node"])
}

func TestRegisterMVServiceBootstrapAndDDLHandler(t *testing.T) {
	called := atomic.Int32{}
	var (
		gotHandlerID notifier.HandlerID
		gotHandler   notifier.SchemaChangeHandler
	)
	svc := RegisterMVService(context.Background(), func(id notifier.HandlerID, handler notifier.SchemaChangeHandler) {
		gotHandlerID = id
		gotHandler = handler
	}, mockSessionPool{}, func() {
		called.Add(1)
	})
	require.NotNil(t, svc)
	require.True(t, svc.ddlDirty.Load())
	require.True(t, svc.notifier.isAwake())
	require.Equal(t, notifier.MVServiceHandlerID, gotHandlerID)
	require.NotNil(t, gotHandler)
	require.Equal(t, TaskBackpressureConfig{
		CPUThreshold: defaultBackpressureCPUThreshold,
		MemThreshold: defaultBackpressureMemThreshold,
		Delay:        defaultTaskBackpressureDelay,
	}, svc.GetTaskBackpressureConfig())
	require.NotNil(t, svc.refreshExecutor.backpressure.Load())
	require.NotNil(t, svc.purgeExecutor.backpressure.Load())

	createTable := notifier.NewCreateTableEvent(&meta.TableInfo{ID: 1})
	require.NoError(t, gotHandler(context.Background(), nil, createTable))
	require.Equal(t, int32(0), called.Load())

	createMVLogTable := notifier.NewCreateTableEvent(&meta.TableInfo{
		ID: 2,
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			BaseTableID: 1,
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, createMVLogTable))
	require.Equal(t, int32(1), called.Load())

	createMVTable := notifier.NewCreateTableEvent(&meta.TableInfo{
		ID: 3,
		MaterializedView: &meta.MaterializedViewInfo{
			BaseTableIDs: []int64{1},
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, createMVTable))
	require.Equal(t, int32(2), called.Load())

	mvLogEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvLogEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedViewLog))))
	require.NoError(t, gotHandler(context.Background(), nil, mvLogEvent))
	require.Equal(t, int32(2), called.Load())

	mvEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedView))))
	require.NoError(t, gotHandler(context.Background(), nil, mvEvent))
	require.Equal(t, int32(2), called.Load())

	alterMVRefreshEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, alterMVRefreshEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionAlterMaterializedViewRefresh))))
	require.NoError(t, gotHandler(context.Background(), nil, alterMVRefreshEvent))
	require.Equal(t, int32(3), called.Load())

	alterMLogPurgeEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, alterMLogPurgeEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionAlterMaterializedViewLogPurge))))
	require.NoError(t, gotHandler(context.Background(), nil, alterMLogPurgeEvent))
	require.Equal(t, int32(4), called.Load())

	outOfPlaceCutoverEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, outOfPlaceCutoverEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionMViewRefreshOutOfPlaceCutover))))
	require.NoError(t, gotHandler(context.Background(), nil, outOfPlaceCutoverEvent))
	require.Equal(t, int32(5), called.Load())

	dropTableEvent := notifier.NewDropTableEvent(&meta.TableInfo{ID: 4})
	require.NoError(t, gotHandler(context.Background(), nil, dropTableEvent))
	require.Equal(t, int32(5), called.Load())

	dropMVTableEvent := notifier.NewDropTableEvent(&meta.TableInfo{
		ID: 5,
		MaterializedView: &meta.MaterializedViewInfo{
			BaseTableIDs: []int64{1},
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, dropMVTableEvent))
	require.Equal(t, int32(6), called.Load())
}

func TestServerHelperLoadAllTiDBMLogPurge(t *testing.T) {
	nextPurgeSec1 := int64(600)
	nextPurgeSec2 := int64(720)

	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVLogPurge] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec1),
			types.NewIntDatum(201),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewIntDatum(202),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewIntDatum(0),
		}).ToRow(),
		// Invalid row with NULL NEXT_PURGE_UNIX_SECONDS should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(203),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).LoadAllTiDBMVLogPurge(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVLogPurge}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	l201 := got[int64(201)]
	require.NotNil(t, l201)
	expect201 := time.Unix(nextPurgeSec1, 0)
	require.Equal(t, expect201, l201.nextPurge)
	require.Equal(t, expect201.UnixMilli(), l201.orderTs)

	l202 := got[int64(202)]
	require.NotNil(t, l202)
	expect202 := time.Unix(nextPurgeSec2, 0)
	require.Equal(t, expect202, l202.nextPurge)
	require.Equal(t, expect202.UnixMilli(), l202.orderTs)
}

func TestServerHelperLoadAllTiDBMVRefresh(t *testing.T) {
	nextRefreshSec1 := int64(900)
	nextRefreshSec2 := int64(1200)

	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVRefresh] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec1),
			types.NewIntDatum(101),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(102),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(0),
		}).ToRow(),
		// Invalid row with NULL NEXT_REFRESH_UNIX_SECONDS should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(103),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).LoadAllTiDBMVRefresh(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVRefresh}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	m101 := got[int64(101)]
	require.NotNil(t, m101)
	expect101 := time.Unix(nextRefreshSec1, 0)
	require.Equal(t, expect101, m101.nextRefresh)
	require.Equal(t, expect101.UnixMilli(), m101.orderTs)

	m102 := got[int64(102)]
	require.NotNil(t, m102)
	expect102 := time.Unix(nextRefreshSec2, 0)
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
}

func TestServerHelperRefreshMVDeletedWhenMetaNotFound(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 100)
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVSuccess(t *testing.T) {
	se := newRecordingSessionContext()
	expectedNextRefresh := time.Unix(1700000000, 0).Add(time.Minute).Round(0)
	se.restrictedRows[testSQLFindMVNextTime] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(expectedNextRefresh.Unix()),
		}).ToRow(),
	}
	mviewTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mviewTable)
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVManualCancelNormalized(t *testing.T) {
	se := newRecordingSessionContext()
	se.restrictedErrs[testSQLRefreshMV] = errors.New("materialized view task canceled manually")
	mviewTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mviewTable)
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.ErrorIs(t, err, errMVTaskCanceledManually)
	require.True(t, nextRefresh.IsZero())
	require.Equal(t, []string{testSQLRefreshMV}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVUsesGlobalRefreshSessionVars(t *testing.T) {
	se := newRecordingSessionContext()
	expectedNextRefresh := time.Unix(1700000000, 0).Add(time.Minute).Round(0)
	se.restrictedRows[testSQLFindMVNextTime] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(expectedNextRefresh.Unix()),
		}).ToRow(),
	}
	mviewTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mviewTable)
	pool := recordingSessionPool{se: se}

	vars := se.GetSessionVars()
	mockGlobalAccessor := variable.NewMockGlobalAccessor4Tests()
	mockGlobalAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = mockGlobalAccessor
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainMemQuota, "536870912"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMaxTiFlashThreads, "8"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, "111"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, "222"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMaxBytesBeforeTiFlashExternalSort, "333"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiFlashMemQuotaQueryPerNode, "444"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiFlashQuerySpillRatio, "0.25"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiFlashFineGrainedShuffleStreamCount, "16"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiFlashFineGrainedShuffleBatchSize, "4096"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainImportThreads, "12"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainImportDiskQuota, "64gib"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainIsolationReadEngines, "tikv"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainMemQuota, "268435456"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainIsolationReadEngines, "tidb"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBIsolationReadEngines, "tidb"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxTiFlashThreads, "2"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, "101"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, "202"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalSort, "303"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashMemQuotaQueryPerNode, "404"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashQuerySpillRatio, "0.75"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashFineGrainedShuffleStreamCount, "4"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashFineGrainedShuffleBatchSize, "1024"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainImportThreads, "3"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainImportDiskQuota, "8gib"))

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
	require.Equal(t, []int64{536870912, 536870912}, se.restrictedMaintainQuota)
	require.Equal(t, []string{"tikv", "tikv"}, se.restrictedMaintainIsolationReadEngines)
	require.Equal(t, []int64{8, 8}, se.restrictedMaxThreads)
	require.Equal(t, []int64{111, 111}, se.restrictedMaxBytesBeforeExternalJoin)
	require.Equal(t, []int64{222, 222}, se.restrictedMaxBytesBeforeExternalAgg)
	require.Equal(t, []int64{333, 333}, se.restrictedMaxBytesBeforeExternalSort)
	require.Equal(t, []int64{444, 444}, se.restrictedMemQuotaQueryPerNode)
	require.Equal(t, []float64{0.25, 0.25}, se.restrictedQuerySpillRatio)
	require.Equal(t, []int64{16, 16}, se.restrictedStreamCount)
	require.Equal(t, []uint64{4096, 4096}, se.restrictedBatchSize)
	require.Equal(t, []int{12, 12}, se.restrictedImportThreads)
	require.Equal(t, []string{"64gib", "64gib"}, se.restrictedImportDiskQuota)

	refreshErr := errors.New("mock refresh failure")
	se.restrictedErrs[testSQLRefreshMV] = refreshErr
	_, err = (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.ErrorIs(t, err, refreshErr)

	require.Equal(t, int64(268435456), vars.MViewMaintainMemQuota)
	require.Equal(t, int64(2), vars.TiFlashMaxThreads)
	require.Equal(t, int64(101), vars.TiFlashMaxBytesBeforeExternalJoin)
	require.Equal(t, int64(202), vars.TiFlashMaxBytesBeforeExternalGroupBy)
	require.Equal(t, int64(303), vars.TiFlashMaxBytesBeforeExternalSort)
	require.Equal(t, int64(404), vars.TiFlashMaxQueryMemoryPerNode)
	require.Equal(t, float64(0.75), vars.TiFlashQuerySpillRatio)
	require.Equal(t, int64(4), vars.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(1024), vars.TiFlashFineGrainedShuffleBatchSize)
	require.Equal(t, 3, vars.MViewMaintainImportThreads)
	require.Equal(t, "8gib", vars.MViewMaintainImportDiskQuota)
	require.Equal(t, "tidb", variable.GetIsolationReadEnginesString(vars))
	require.Equal(t, "tidb", vars.MViewMaintainIsolationReadEngines)
}

func TestServerHelperRefreshMVBestEffortWhenGlobalSessionVarsUnavailable(t *testing.T) {
	se := newRecordingSessionContext()
	expectedNextRefresh := time.Unix(1700000000, 0).Add(time.Minute).Round(0)
	se.restrictedRows[testSQLFindMVNextTime] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(expectedNextRefresh.Unix()),
		}).ToRow(),
	}
	mviewTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mviewTable)
	pool := recordingSessionPool{se: se}

	vars := se.GetSessionVars()
	baseAccessor := variable.NewMockGlobalAccessor4Tests()
	baseAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = &faultyGlobalAccessor{
		MockGlobalAccessor: baseAccessor,
		getErrs: map[string]error{
			vardef.TiDBMaxTiFlashThreads:                 errors.New("mock global read failure"),
			vardef.TiDBMViewMaintainIsolationReadEngines: errors.New("mock global read failure"),
		},
		failAfter: map[string]int{
			vardef.TiDBMViewMaintainIsolationReadEngines: 1,
		},
		getVals: map[string]string{
			vardef.TiDBMViewMaintainMemQuota:                "536870912",
			vardef.TiDBMViewMaintainIsolationReadEngines:    "tidb",
			vardef.TiDBMaxBytesBeforeTiFlashExternalJoin:    "111",
			vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy: "222",
			vardef.TiDBMaxBytesBeforeTiFlashExternalSort:    "333",
			vardef.TiFlashMemQuotaQueryPerNode:              "444",
			vardef.TiFlashQuerySpillRatio:                   "not-a-float",
			vardef.TiFlashFineGrainedShuffleStreamCount:     "16",
			vardef.TiFlashFineGrainedShuffleBatchSize:       "4096",
			vardef.TiDBMViewMaintainImportThreads:           "12",
			vardef.TiDBMViewMaintainImportDiskQuota:         "bad-quota",
		},
	}
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainMemQuota, "268435456"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBIsolationReadEngines, "tikv"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxTiFlashThreads, "2"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, "101"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, "202"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMaxBytesBeforeTiFlashExternalSort, "303"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashMemQuotaQueryPerNode, "404"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashQuerySpillRatio, "0.75"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashFineGrainedShuffleStreamCount, "4"))
	require.NoError(t, vars.SetSystemVar(vardef.TiFlashFineGrainedShuffleBatchSize, "1024"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainImportThreads, "3"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainImportDiskQuota, "8gib"))

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
	require.Equal(t, []int64{536870912, 536870912}, se.restrictedMaintainQuota)
	require.Equal(t, []string{"tidb", "tidb"}, se.restrictedMaintainIsolationReadEngines)
	require.Equal(t, []int64{2, 2}, se.restrictedMaxThreads)
	require.Equal(t, []int64{111, 111}, se.restrictedMaxBytesBeforeExternalJoin)
	require.Equal(t, []int64{222, 222}, se.restrictedMaxBytesBeforeExternalAgg)
	require.Equal(t, []int64{333, 333}, se.restrictedMaxBytesBeforeExternalSort)
	require.Equal(t, []int64{444, 444}, se.restrictedMemQuotaQueryPerNode)
	require.Equal(t, []float64{0.75, 0.75}, se.restrictedQuerySpillRatio)
	require.Equal(t, []int64{16, 16}, se.restrictedStreamCount)
	require.Equal(t, []uint64{4096, 4096}, se.restrictedBatchSize)
	require.Equal(t, []int{12, 12}, se.restrictedImportThreads)
	require.Equal(t, []string{"8gib", "8gib"}, se.restrictedImportDiskQuota)
	require.Equal(t, int64(268435456), vars.MViewMaintainMemQuota)
	require.Equal(t, int64(2), vars.TiFlashMaxThreads)
	require.Equal(t, int64(101), vars.TiFlashMaxBytesBeforeExternalJoin)
	require.Equal(t, int64(202), vars.TiFlashMaxBytesBeforeExternalGroupBy)
	require.Equal(t, int64(303), vars.TiFlashMaxBytesBeforeExternalSort)
	require.Equal(t, int64(404), vars.TiFlashMaxQueryMemoryPerNode)
	require.Equal(t, float64(0.75), vars.TiFlashQuerySpillRatio)
	require.Equal(t, int64(4), vars.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(1024), vars.TiFlashFineGrainedShuffleBatchSize)
	require.Equal(t, 3, vars.MViewMaintainImportThreads)
	require.Equal(t, "8gib", vars.MViewMaintainImportDiskQuota)
	require.Equal(t, "tikv", variable.GetIsolationReadEnginesString(vars))
	require.Equal(t, variable.GetSysVar(vardef.TiDBMViewMaintainIsolationReadEngines).Value, vars.MViewMaintainIsolationReadEngines)
}

func TestServerHelperRefreshMVDeletedWhenNextTimeNotFound(t *testing.T) {
	se := newRecordingSessionContext()
	mviewTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mviewTable)
	se.restrictedRows[testSQLFindMVNextTime] = nil
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogDeletedWhenMetaNotFound(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 200)
	require.NoError(t, err)
	require.True(t, nextPurge.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogSuccess(t *testing.T) {
	se := newRecordingSessionContext()
	nextTimeRows := []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(time.Unix(1700000000, 0).Add(time.Minute).Unix()),
		}).ToRow(),
	}
	setupPurgeMVLogMetaForTest(t, se, nextTimeRows)

	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.True(t, nextPurge.After(time.Unix(1700000000, 0).Add(-2*time.Second)))
	require.Empty(t, se.executedSQL)
	require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 2)
	require.Len(t, se.executedRestrictedArg[0], 2)
	require.Equal(t, "mv_base", se.executedRestrictedArg[0][1])
}

func TestServerHelperPurgeMVLogManualCancelNormalized(t *testing.T) {
	se := newRecordingSessionContext()
	setupPurgeMVLogMetaForTest(t, se, nil)
	se.restrictedErrs[testSQLPurgeMVLog] = errors.New("materialized view task canceled manually")
	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.ErrorIs(t, err, errMVTaskCanceledManually)
	require.True(t, nextPurge.IsZero())
	require.Equal(t, []string{testSQLPurgeMVLog}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogUsesGlobalMaintainMemQuota(t *testing.T) {
	se := newRecordingSessionContext()
	nextTimeRows := []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(time.Unix(1700000000, 0).Add(time.Minute).Unix()),
		}).ToRow(),
	}
	setupPurgeMVLogMetaForTest(t, se, nextTimeRows)

	pool := recordingSessionPool{se: se}
	vars := se.GetSessionVars()
	mockGlobalAccessor := variable.NewMockGlobalAccessor4Tests()
	mockGlobalAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = mockGlobalAccessor
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainMemQuota, "536870912"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMViewMaintainIsolationReadEngines, "tikv"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeBatchSize, "4321"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeMinRate, "8765"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeRateBudgetRatio, "0.75"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeDeleteTiFlashThreads, "3"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainMemQuota, "268435456"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainIsolationReadEngines, "tidb"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBIsolationReadEngines, "tidb"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeBatchSize, "1234"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeMinRate, "2345"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeRateBudgetRatio, "0.25"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeDeleteTiFlashThreads, "5"))
	vars.EnableChunkRPC = true

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
	require.Equal(t, []int64{536870912, 536870912}, se.restrictedMaintainQuota)
	require.Equal(t, []string{"tikv", "tikv"}, se.restrictedMaintainIsolationReadEngines)
	require.Equal(t, []int{4321, 4321}, se.restrictedMLogPurgeBatchSize)
	require.Equal(t, []int{8765, 8765}, se.restrictedMLogPurgeMinRate)
	require.Equal(t, []float64{0.75, 0.75}, se.restrictedMLogPurgeRateBudgetRatio)
	require.Equal(t, []int64{3, 3}, se.restrictedMLogPurgeDeleteTiFlashThreads)

	purgeErr := errors.New("mock purge failure")
	se.restrictedErrs[testSQLPurgeMVLog] = purgeErr
	_, err = (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.ErrorIs(t, err, purgeErr)

	require.Equal(t, int64(268435456), vars.MViewMaintainMemQuota)
	require.Equal(t, "tidb", variable.GetIsolationReadEnginesString(vars))
	require.Equal(t, "tidb", vars.MViewMaintainIsolationReadEngines)
	require.Equal(t, 1234, vars.MLogPurgeBatchSize)
	require.Equal(t, 2345, vars.MLogPurgeMinRate)
	require.Equal(t, 0.25, vars.MLogPurgeRateBudgetRatio)
	require.Equal(t, int64(5), vars.MLogPurgeDeleteTiFlashThreads)
	require.True(t, vars.EnableChunkRPC)
}

func TestServerHelperPurgeMVLogBestEffortWhenGlobalMaintainMemQuotaUnavailable(t *testing.T) {
	se := newRecordingSessionContext()
	nextTimeRows := []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(time.Unix(1700000000, 0).Add(time.Minute).Unix()),
		}).ToRow(),
	}
	setupPurgeMVLogMetaForTest(t, se, nextTimeRows)

	pool := recordingSessionPool{se: se}
	vars := se.GetSessionVars()
	baseAccessor := variable.NewMockGlobalAccessor4Tests()
	baseAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = &faultyGlobalAccessor{
		MockGlobalAccessor: baseAccessor,
		getErrs: map[string]error{
			vardef.TiDBMViewMaintainMemQuota:             errors.New("mock global read failure"),
			vardef.TiDBMViewMaintainIsolationReadEngines: errors.New("mock global read failure"),
			vardef.TiDBMLogPurgeBatchSize:                errors.New("mock global read failure"),
			vardef.TiDBMLogPurgeMinRate:                  errors.New("mock global read failure"),
			vardef.TiDBMLogPurgeRateBudgetRatio:          errors.New("mock global read failure"),
			vardef.TiDBMLogPurgeDeleteTiFlashThreads:     errors.New("mock global read failure"),
		},
		failAfter: map[string]int{
			vardef.TiDBMViewMaintainIsolationReadEngines: 1,
		},
		getVals: map[string]string{
			vardef.TiDBMViewMaintainIsolationReadEngines: "tidb",
		},
	}
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewMaintainMemQuota, "268435456"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBIsolationReadEngines, "tikv"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeBatchSize, "3456"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeMinRate, "4567"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeRateBudgetRatio, "0.35"))
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMLogPurgeDeleteTiFlashThreads, "7"))

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
	require.Equal(t, []int64{268435456, 268435456}, se.restrictedMaintainQuota)
	require.Equal(t, []string{"tidb", "tidb"}, se.restrictedMaintainIsolationReadEngines)
	require.Equal(t, []int{3456, 3456}, se.restrictedMLogPurgeBatchSize)
	require.Equal(t, []int{4567, 4567}, se.restrictedMLogPurgeMinRate)
	require.Equal(t, []float64{0.35, 0.35}, se.restrictedMLogPurgeRateBudgetRatio)
	require.Equal(t, []int64{7, 7}, se.restrictedMLogPurgeDeleteTiFlashThreads)
	require.Equal(t, int64(268435456), vars.MViewMaintainMemQuota)
	require.Equal(t, "tikv", variable.GetIsolationReadEnginesString(vars))
	require.Equal(t, variable.GetSysVar(vardef.TiDBMViewMaintainIsolationReadEngines).Value, vars.MViewMaintainIsolationReadEngines)
	require.Equal(t, 3456, vars.MLogPurgeBatchSize)
	require.Equal(t, 4567, vars.MLogPurgeMinRate)
	require.Equal(t, 0.35, vars.MLogPurgeRateBudgetRatio)
	require.Equal(t, int64(7), vars.MLogPurgeDeleteTiFlashThreads)
}

func TestPurgeMVLogSkipWhenAutoPurge(t *testing.T) {
	testCases := []struct {
		name     string
		nextRows func(now time.Time) []chunk.Row
		expected func(now time.Time) time.Time
	}{
		{
			name: "next_time_null",
			nextRows: func(_ time.Time) []chunk.Row {
				return []chunk.Row{
					chunk.MutRowFromDatums([]types.Datum{
						types.NewDatum(nil),
					}).ToRow(),
				}
			},
			expected: func(_ time.Time) time.Time {
				return time.Time{}
			},
		},
		{
			name: "next_time_future",
			nextRows: func(now time.Time) []chunk.Row {
				return []chunk.Row{
					chunk.MutRowFromDatums([]types.Datum{
						types.NewIntDatum(now.Add(time.Minute).Unix()),
					}).ToRow(),
				}
			},
			expected: func(now time.Time) time.Time {
				return mvsUnix(now.Add(time.Minute).Unix(), 0)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1700000000, 0)
			se := newRecordingSessionContext()
			setupPurgeMVLogMetaForTest(t, se, tc.nextRows(now))
			pool := recordingSessionPool{se: se}

			nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
			require.NoError(t, err)
			require.Equal(t, tc.expected(now), nextPurge)
			require.Empty(t, se.executedSQL)
			require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
			require.Len(t, se.executedRestrictedArg, 2)
			require.Len(t, se.executedRestrictedArg[0], 2)
			require.Equal(t, "mv_base", se.executedRestrictedArg[0][1])
		})
	}
}

func TestServerHelperTryBackoffRefreshManualCancel(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		expectedNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			expectedNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				next := time.Unix(1700000000, 0).UTC().Add(time.Minute).Round(0)
				return &next, false, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(expectedNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(int64)
		require.True(t, ok)
		require.Equal(t, expectedNext.Unix(), gotNext)
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("keep_later_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		currentNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay + time.Minute).Round(0)
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return &currentNext, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(currentNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(int64)
		require.True(t, ok)
		require.Equal(t, currentNext.Unix(), gotNext)
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("no_next_schedule_clears_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		require.Nil(t, se.executedRestrictedArg[1][0])
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("locked_next_time_null_keeps_unscheduled", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay)
		resolverCalled := false

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				resolverCalled = true
				next := time.Unix(1700000000, 0).UTC().Add(time.Hour)
				return &next, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.False(t, resolverCalled)
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
	})

	t.Run("resolver_error_falls_back_to_cooldown", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, false, errors.New("mock resolver error")
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(cooldownNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(int64)
		require.True(t, ok)
		require.Equal(t, cooldownNext.Unix(), gotNext)
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("lock_conflict", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedErrs[testSQLLockMVNextTime] = storeerr.ErrLockAcquireFailAndNoWaitSet
		pool := recordingSessionPool{se: se}

		applied, appliedNext, err := (&serviceHelper{}).TryBackoffRefreshManualCancel(context.Background(), pool, 101, time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay))
		require.NoError(t, err)
		require.False(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime}, se.executedRestrictedSQL)
	})
}

func TestServerHelperTryBackoffPurgeManualCancel(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		expectedNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			expectedNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				next := time.Unix(1700000000, 0).UTC().Add(time.Minute).Round(0)
				return &next, false, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(expectedNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(int64)
		require.True(t, ok)
		require.Equal(t, expectedNext.Unix(), gotNext)
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})

	t.Run("keep_later_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		currentNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay + time.Minute).Round(0)
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return &currentNext, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(currentNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(int64)
		require.True(t, ok)
		require.Equal(t, currentNext.Unix(), gotNext)
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})

	t.Run("no_next_schedule_clears_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		require.Nil(t, se.executedRestrictedArg[1][0])
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})

	t.Run("locked_next_time_null_keeps_unscheduled", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := time.Unix(1700000000, 0).UTC().Add(manualCancelBackoffDelay)
		resolverCalled := false

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				resolverCalled = true
				next := time.Unix(1700000000, 0).UTC().Add(time.Hour)
				return &next, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.False(t, resolverCalled)
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
	})
}
