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
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
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
			types.NewIntDatum(123456789),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(102),
			types.NewIntDatum(223456789),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(0),
			types.NewIntDatum(323456789),
		}).ToRow(),
		// Invalid row with NULL NEXT_REFRESH_UNIX_SECONDS should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(103),
			types.NewIntDatum(423456789),
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
	require.Equal(t, uint64(123456789), m101.lastSuccessReadTSO)
	require.Equal(t, mvsUnixMilli(oracle.ExtractPhysical(123456789)), m101.lastSuccessTime)

	m102 := got[int64(102)]
	require.NotNil(t, m102)
	expect102 := time.Unix(nextRefreshSec2, 0)
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
	require.Equal(t, uint64(223456789), m102.lastSuccessReadTSO)
	require.Equal(t, mvsUnixMilli(oracle.ExtractPhysical(223456789)), m102.lastSuccessTime)
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

func TestServerHelperLoadAllTiDBMVLogAccumulationTasksSkipsDisabledByDefault(t *testing.T) {
	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVLogAccumulation] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(201)}).ToRow(),
		chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(202)}).ToRow(),
	}

	enabledBaseTable, enabledTable := buildMockMVBaseAndMVLogTables(101, 201, 101)
	enabledRows := uint64(2048)
	enabledTable.MaterializedViewLog.LogAccumulationAlertRows = &enabledRows

	disabledBaseTable, disabledTable := buildMockMVBaseAndMVLogTables(102, 202, 102)
	withMockInfoSchema(t, enabledBaseTable, enabledTable, disabledBaseTable, disabledTable)

	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).LoadAllTiDBMVLogAccumulationTasks(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVLogAccumulation}, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 1)
	require.Empty(t, se.executedRestrictedArg[0])
	require.Len(t, got, 1)
	require.Equal(t, &mlogAccumulationTask{
		schemaName: "test",
		mlogName:   "mlog1",
		alertRows:  enabledRows,
	}, got[int64(201)])
}

func TestServerHelperLoadTiDBMVLogAccumulationRowCounts(t *testing.T) {
	se := newRecordingSessionContext()
	se.GetSessionVars().SetDistSQLScanConcurrency(7)
	se.restrictedRows[testSQLCountMVLogRows] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(50)}).ToRow(),
	}

	pool := recordingSessionPool{se: se}
	tasks := map[int64]*mlogAccumulationTask{
		201: {
			schemaName: "test",
			mlogName:   "mlog1",
			alertRows:  50,
		},
	}

	got, err := (&serviceHelper{}).LoadTiDBMVLogAccumulationRowCounts(context.Background(), pool, tasks)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLCountMVLogRows}, se.executedRestrictedSQL)
	require.Equal(t, []any{"test", "mlog1", "test", "mlog1"}, se.executedRestrictedArg[0])
	require.Equal(t, []int{1}, se.restrictedDistSQLScanConcurrency)
	require.Equal(t, 7, se.GetSessionVars().DistSQLScanConcurrency())
	require.Equal(t, map[int64]uint64{201: 50}, got)
}

func TestServerHelperGetCurrentTSO(t *testing.T) {
	se := newRecordingSessionContext()
	se.Store = &mockCurrentVersionStore{
		Store:   &mock.Store{},
		version: kv.NewVersion(123456),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).GetCurrentTSO(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, uint64(123456), got)
}

func TestServerHelperGetCurrentTSOInvalidVersion(t *testing.T) {
	se := newRecordingSessionContext()
	se.Store = &mockCurrentVersionStore{
		Store:   &mock.Store{},
		version: kv.NewVersion(0),
	}
	pool := recordingSessionPool{se: se}

	_, err := (&serviceHelper{}).GetCurrentTSO(context.Background(), pool)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid version")
}

func TestServerHelperPurgeMVHistoryBeforeTSO(t *testing.T) {
	buildExpectedSQL := func(
		refreshExec, purgeExec int,
		refreshCountCapLimits, purgeCountCapLimits []uint64,
	) []string {
		sqls := make([]string, 0, refreshExec+purgeExec+len(refreshCountCapLimits)+len(purgeCountCapLimits)+4)
		sqls = append(sqls, testSQLMarkStaleMVRefreshHistOrphaned)
		for i := 0; i < refreshExec; i++ {
			sqls = append(sqls, testSQLDeleteMVRefreshHistBeforeTSO)
		}
		sqls = append(sqls, testSQLCountMVRefreshHist)
		for _, limit := range refreshCountCapLimits {
			sqls = append(sqls, testSQLDeleteMVRefreshHistByCount(limit))
		}
		sqls = append(sqls, testSQLMarkStaleMVLogPurgeHistOrphaned)
		for i := 0; i < purgeExec; i++ {
			sqls = append(sqls, testSQLDeleteMVLogPurgeHistBeforeTSO)
		}
		sqls = append(sqls, testSQLCountMVLogPurgeHist)
		for _, limit := range purgeCountCapLimits {
			sqls = append(sqls, testSQLDeleteMVLogPurgeHistByCount(limit))
		}
		return sqls
	}

	testCases := []struct {
		name                     string
		currentTSO               uint64
		mviewRetention           time.Duration
		mlogRetention            time.Duration
		refreshTimeAffectedRows  []uint64
		purgeTimeAffectedRows    []uint64
		refreshCountRows         []chunk.Row
		purgeCountRows           []chunk.Row
		refreshTimeErr           error
		refreshCountErr          error
		purgeTimeErr             error
		purgeCountErr            error
		expectRefreshExec        int
		expectPurgeExec          int
		expectRefreshCountCap    []uint64
		expectPurgeCountCap      []uint64
		refreshCountAffectedRows []uint64
		purgeCountAffectedRows   []uint64
		assertArgsAreEqualTSO    bool
		assertArgsNotEqual       bool
		expectErrContains        []string
	}{
		{
			name:                  "zero_retention",
			currentTSO:            987654321,
			mviewRetention:        0,
			mlogRetention:         0,
			expectRefreshExec:     1,
			expectPurgeExec:       1,
			assertArgsAreEqualTSO: true,
		},
		{
			name:               "separate_retention",
			currentTSO:         uint64((10 * time.Hour / time.Millisecond) << 18),
			mviewRetention:     2 * time.Hour,
			mlogRetention:      4 * time.Hour,
			expectRefreshExec:  1,
			expectPurgeExec:    1,
			assertArgsNotEqual: true,
		},
		{
			name:           "batch_delete",
			currentTSO:     987654321,
			mviewRetention: 0,
			mlogRetention:  0,
			refreshTimeAffectedRows: []uint64{
				uint64(historyGCDeleteBatchSize),
				uint64(historyGCDeleteBatchSize),
				1,
			},
			purgeTimeAffectedRows: []uint64{0},
			expectRefreshExec:     3,
			expectPurgeExec:       1,
		},
		{
			name:              "batch_delete_no_max_batches",
			currentTSO:        987654321,
			mviewRetention:    0,
			mlogRetention:     0,
			expectRefreshExec: 26,
			expectPurgeExec:   1,
		},
		{
			name:           "count_cap_delete_budget",
			currentTSO:     987654321,
			mviewRetention: 0,
			mlogRetention:  0,
			refreshCountRows: []chunk.Row{
				chunk.MutRowFromDatums([]types.Datum{
					types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords + historyGCDeleteBatchSize + 5)),
					types.NewUintDatum(500),
				}).ToRow(),
			},
			purgeCountRows: []chunk.Row{
				chunk.MutRowFromDatums([]types.Datum{
					types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords + 8)),
					types.NewUintDatum(900),
				}).ToRow(),
			},
			refreshTimeAffectedRows:  []uint64{0},
			purgeTimeAffectedRows:    []uint64{0},
			expectRefreshExec:        1,
			expectPurgeExec:          1,
			expectRefreshCountCap:    []uint64{uint64(historyGCDeleteBatchSize), 5},
			expectPurgeCountCap:      []uint64{8},
			refreshCountAffectedRows: []uint64{uint64(historyGCDeleteBatchSize), 5},
			purgeCountAffectedRows:   []uint64{8},
		},
		{
			name:                    "count_cap_under_limit",
			currentTSO:              987654321,
			mviewRetention:          0,
			mlogRetention:           0,
			refreshTimeAffectedRows: []uint64{0},
			purgeTimeAffectedRows:   []uint64{0},
			refreshCountRows: []chunk.Row{
				chunk.MutRowFromDatums([]types.Datum{
					types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords)),
					types.NewUintDatum(500),
				}).ToRow(),
			},
			purgeCountRows: []chunk.Row{
				chunk.MutRowFromDatums([]types.Datum{
					types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords - 1)),
					types.NewUintDatum(900),
				}).ToRow(),
			},
			expectRefreshExec: 1,
			expectPurgeExec:   1,
		},
		{
			name:                  "continue_after_refresh_retention_error",
			currentTSO:            987654321,
			mviewRetention:        0,
			mlogRetention:         0,
			refreshTimeErr:        errors.New("refresh retention gc failed"),
			purgeTimeAffectedRows: []uint64{0},
			expectRefreshExec:     1,
			expectPurgeExec:       1,
			expectErrContains:     []string{"purge mview refresh history by retention failed", "refresh retention gc failed"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			se := newRecordingSessionContext()
			se.restrictedAffectedRows[testSQLMarkStaleMVRefreshHistOrphaned] = []uint64{0}
			se.restrictedAffectedRows[testSQLMarkStaleMVLogPurgeHistOrphaned] = []uint64{0}
			if tc.name == "batch_delete_no_max_batches" {
				const extraBatches = 25
				refreshAffected := make([]uint64, extraBatches)
				for i := range refreshAffected {
					refreshAffected[i] = uint64(historyGCDeleteBatchSize)
				}
				se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = append(refreshAffected, 1)
				se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = []uint64{0}
			} else {
				if len(tc.refreshTimeAffectedRows) > 0 {
					se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = tc.refreshTimeAffectedRows
				}
				if len(tc.purgeTimeAffectedRows) > 0 {
					se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = tc.purgeTimeAffectedRows
				}
			}
			for i, limit := range tc.expectRefreshCountCap {
				se.restrictedAffectedRows[testSQLDeleteMVRefreshHistByCount(limit)] = []uint64{tc.refreshCountAffectedRows[i]}
			}
			for i, limit := range tc.expectPurgeCountCap {
				se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistByCount(limit)] = []uint64{tc.purgeCountAffectedRows[i]}
			}
			if tc.refreshCountRows != nil {
				se.restrictedRows[testSQLCountMVRefreshHist] = tc.refreshCountRows
			}
			if tc.purgeCountRows != nil {
				se.restrictedRows[testSQLCountMVLogPurgeHist] = tc.purgeCountRows
			}
			if tc.refreshTimeErr != nil {
				se.restrictedErrs[testSQLDeleteMVRefreshHistBeforeTSO] = tc.refreshTimeErr
			}
			if tc.refreshCountErr != nil {
				se.restrictedErrs[testSQLCountMVRefreshHist] = tc.refreshCountErr
			}
			if tc.purgeTimeErr != nil {
				se.restrictedErrs[testSQLDeleteMVLogPurgeHistBeforeTSO] = tc.purgeTimeErr
			}
			if tc.purgeCountErr != nil {
				se.restrictedErrs[testSQLCountMVLogPurgeHist] = tc.purgeCountErr
			}
			pool := recordingSessionPool{se: se}

			err := (&serviceHelper{}).PurgeMVHistoryBeforeTSO(
				context.Background(),
				pool,
				tc.currentTSO,
				tc.mviewRetention,
				tc.mlogRetention,
			)
			if len(tc.expectErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, msg := range tc.expectErrContains {
					require.ErrorContains(t, err, msg)
				}
			}
			require.Equal(t, buildExpectedSQL(
				tc.expectRefreshExec,
				tc.expectPurgeExec,
				tc.expectRefreshCountCap,
				tc.expectPurgeCountCap,
			), se.executedRestrictedSQL)
			require.Len(t, se.executedRestrictedArg, len(se.executedRestrictedSQL))

			refreshRetentionArgPos := 1
			purgeOrphanArgPos := tc.expectRefreshExec + len(tc.expectRefreshCountCap) + 2
			purgeTimeArgPos := purgeOrphanArgPos + 1
			if tc.assertArgsAreEqualTSO {
				require.Len(t, se.executedRestrictedArg[0], 1)
				require.Len(t, se.executedRestrictedArg[purgeOrphanArgPos], 1)
				require.Equal(t, se.executedRestrictedArg[0][0], se.executedRestrictedArg[purgeOrphanArgPos][0])
				require.Equal(t, []any{tc.currentTSO}, se.executedRestrictedArg[refreshRetentionArgPos])
				require.Equal(t, []any{tc.currentTSO}, se.executedRestrictedArg[purgeTimeArgPos])
			}
			if tc.assertArgsNotEqual {
				require.NotEqual(t, se.executedRestrictedArg[refreshRetentionArgPos][0], se.executedRestrictedArg[purgeTimeArgPos][0])
			}
			require.Empty(t, se.executedRestrictedArg[tc.expectRefreshExec+1])
			argBase := tc.expectRefreshExec + 2
			if len(tc.refreshCountRows) > 0 {
				minJobID := tc.refreshCountRows[0].GetUint64(1)
				for i := range tc.expectRefreshCountCap {
					require.Equal(t, []any{minJobID}, se.executedRestrictedArg[argBase+i])
				}
			}
			countPos := purgeTimeArgPos + tc.expectPurgeExec
			require.Empty(t, se.executedRestrictedArg[countPos])
			if len(tc.purgeCountRows) > 0 {
				minJobID := tc.purgeCountRows[0].GetUint64(1)
				for i := range tc.expectPurgeCountCap {
					require.Equal(t, []any{minJobID}, se.executedRestrictedArg[countPos+1+i])
				}
			}
		})
	}
}

func TestServerHelperPurgeMVHistoryBeforeTSOTreatsNullStatusAsDeletable(t *testing.T) {
	se := newRecordingSessionContext()
	se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = []uint64{0}
	se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = []uint64{0}
	se.restrictedRows[testSQLCountMVRefreshHist] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords)),
			types.NewUintDatum(500),
		}).ToRow(),
	}
	se.restrictedRows[testSQLCountMVLogPurgeHist] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords)),
			types.NewUintDatum(900),
		}).ToRow(),
	}

	err := (&serviceHelper{}).PurgeMVHistoryBeforeTSO(
		context.Background(),
		recordingSessionPool{se: se},
		987654321,
		0,
		0,
	)
	require.NoError(t, err)
	require.Contains(t, se.executedRestrictedSQL, testSQLDeleteMVRefreshHistBeforeTSO)
	require.Contains(t, se.executedRestrictedSQL, testSQLDeleteMVLogPurgeHistBeforeTSO)
	require.Contains(t, testSQLDeleteMVRefreshHistBeforeTSO, "REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running'")
	require.Contains(t, testSQLDeleteMVLogPurgeHistBeforeTSO, "PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running'")
	require.Contains(t, testSQLDeleteMVRefreshHistByCount(1), "REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running'")
	require.Contains(t, testSQLDeleteMVLogPurgeHistByCount(1), "PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running'")
}

func TestServerHelperPurgeMVHistoryBeforeTSOReconcilesStaleRunningHistRows(t *testing.T) {
	currentTSO := oracle.ComposeTS(int64(36*time.Hour/time.Millisecond), 0)
	expectedCutoffTime := oracle.GetTimeFromTS(oracle.ComposeTS(int64(12*time.Hour/time.Millisecond), 0))

	se := newRecordingSessionContext()
	se.restrictedAffectedRows[testSQLMarkStaleMVRefreshHistOrphaned] = []uint64{2}
	se.restrictedAffectedRows[testSQLMarkStaleMVLogPurgeHistOrphaned] = []uint64{1}
	se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = []uint64{0}
	se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = []uint64{0}
	se.restrictedRows[testSQLCountMVRefreshHist] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords)),
			types.NewUintDatum(500),
		}).ToRow(),
	}
	se.restrictedRows[testSQLCountMVLogPurgeHist] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(int64(defaultMVHistoryGCMaxRecords)),
			types.NewUintDatum(900),
		}).ToRow(),
	}

	err := (&serviceHelper{}).PurgeMVHistoryBeforeTSO(
		context.Background(),
		recordingSessionPool{se: se},
		currentTSO,
		0,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		testSQLMarkStaleMVRefreshHistOrphaned,
		testSQLDeleteMVRefreshHistBeforeTSO,
		testSQLCountMVRefreshHist,
		testSQLMarkStaleMVLogPurgeHistOrphaned,
		testSQLDeleteMVLogPurgeHistBeforeTSO,
		testSQLCountMVLogPurgeHist,
	}, se.executedRestrictedSQL)
	require.Equal(t, []any{expectedCutoffTime}, se.executedRestrictedArg[0])
	require.Equal(t, []any{currentTSO}, se.executedRestrictedArg[1])
	require.Equal(t, []any{expectedCutoffTime}, se.executedRestrictedArg[3])
	require.Equal(t, []any{currentTSO}, se.executedRestrictedArg[4])
}

func TestServerHelperPurgeMVHistoryBeforeTSOUsesStatusIndexHints(t *testing.T) {
	require.Contains(t, buildMarkRefreshHistoryRunningRowsOrphanedSQL(), "FORCE INDEX (idx_refresh_status_start_time)")
	require.Contains(t, buildMarkLogPurgeHistoryRunningRowsOrphanedSQL(), "FORCE INDEX (idx_purge_status_start_time)")
}

func TestServerHelperSyncMVRefreshAlertStates(t *testing.T) {
	now := time.Unix(1700000000, 0).Round(0)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}
	states := []refreshAlertTask{
		{
			mviewID:         101,
			schemaName:      "test",
			mviewName:       "mv_warn",
			lastSuccessTime: now.Add(-time.Minute),
			alertLevel:      mvRefreshAlertLevelWarning,
		},
		{
			mviewID:         102,
			schemaName:      "test",
			mviewName:       "mv_overdue",
			lastSuccessTime: now.Add(-2 * time.Minute),
			alertLevel:      mvRefreshAlertLevelOverdue,
		},
		{
			mviewID: 103,
		},
		{
			mviewID:            104,
			metadataUnresolved: true,
		},
		{
			mviewID: 0,
		},
		{
			mviewID:    105,
			schemaName: "test",
			mviewName:  "mv_no_success",
			alertLevel: mvRefreshAlertLevelWarning,
		},
	}

	err := (&serviceHelper{}).SyncMVRefreshAlertStates(context.Background(), pool, now, states)
	require.NoError(t, err)
	upsertSQL := buildUpsertMVRefreshAlertSQL(now, []refreshAlertTask{states[0], states[1], states[5]})
	require.Equal(t, []string{
		buildDeleteResolvedMVRefreshAlertSQL([]int64{103}),
		buildClearResolvedMVRefreshAlertLevelSQL(now, []int64{103}),
		upsertSQL,
	}, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 3)
	require.Contains(t, upsertSQL, "'mv_no_success', 'warning', NULL,")
	require.Empty(t, se.executedRestrictedArg[0])
	require.Empty(t, se.executedRestrictedArg[1])
	require.Empty(t, se.executedRestrictedArg[2])
}

func TestBuildResolvedMVRefreshAlertSQL(t *testing.T) {
	now := time.Unix(1700000000, 0).Round(0)
	ids := []int64{103, 104}

	require.Equal(t,
		"DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NULL",
		buildDeleteResolvedMVRefreshAlertSQL(ids),
	)
	require.Equal(t,
		"UPDATE mysql.tidb_mview_refresh_alert SET ALERT_LEVEL = NULL, UPDATE_TIME = '"+now.Format("2006-01-02 15:04:05")+"' WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NOT NULL AND ALERT_LEVEL IS NOT NULL",
		buildClearResolvedMVRefreshAlertLevelSQL(now, ids),
	)
	require.Equal(t,
		`UPDATE mysql.tidb_mview_refresh_alert AS a
JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
SET a.ALERT_LEVEL = NULL, a.UPDATE_TIME = NOW(6)
WHERE i.NEXT_REFRESH_UNIX_SECONDS IS NULL AND a.ALERT_LEVEL IS NOT NULL`,
		buildClearDisabledMVRefreshAlertLevelSQL(),
	)
}

func TestServerHelperCleanupStaleMVRefreshAlerts(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	err := (&serviceHelper{}).CleanupStaleMVRefreshAlerts(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{buildClearDisabledMVRefreshAlertLevelSQL(), buildCleanupStaleMVRefreshAlertSQL()}, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 2)
	require.Empty(t, se.executedRestrictedArg[0])
	require.Empty(t, se.executedRestrictedArg[1])
}
