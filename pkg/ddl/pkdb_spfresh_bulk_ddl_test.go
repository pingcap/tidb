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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	fieldtypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSPFreshReorgRejectsMultiSchemaBeforeAdmission(t *testing.T) {
	originalURI := spfreshBulkCloudStorageURI
	t.Cleanup(func() { spfreshBulkCloudStorageURI = originalURI })
	for _, bulk := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "bulk"}[bulk], func(t *testing.T) {
			spfreshBulkCloudStorageURI = func() string {
				if bulk {
					return "s3://spfresh-mvp/multi-schema"
				}
				return ""
			}
			tbl, index := newSPFreshBulkTestTable()
			job := newSPFreshBulkTestJob()
			job.MultiSchemaInfo = model.NewMultiSchemaInfo()
			job.ReorgMeta.UseCloudStorage = bulk
			// No worker or job context is needed: rejection precedes route selection or IO.
			require.ErrorIs(t, initSPFreshReorg(nil, nil, job, tbl, []*model.IndexInfo{index}), errSPFreshMultiSchemaChange)
			require.Empty(t, job.ReorgMeta.SPFreshBulkRunID)
		})
	}
}

func TestSPFreshReorgEmptyURIKeepsExistingRoute(t *testing.T) {
	for _, reorgType := range []model.ReorgType{model.ReorgTypeTxn, model.ReorgTypeTxnMerge, model.ReorgTypeIngest} {
		t.Run(reorgType.String(), func(t *testing.T) {
			originalReadURI := spfreshBulkCloudStorageURI
			t.Cleanup(func() { spfreshBulkCloudStorageURI = originalReadURI })
			reads := 0
			spfreshBulkCloudStorageURI = func() string {
				reads++
				if reads == 1 {
					return ""
				}
				return "s3://spfresh-mvp/unexpected-bulk-run"
			}
			tblInfo, indexInfo := newSPFreshBulkTestTable()
			job := newSPFreshBulkTestJob()
			job.ReorgMeta.ReorgTp = reorgType
			w := newSPFreshBulkTestWorker(job.ID)

			require.NoError(t, initSPFreshReorg(w, nil, job, tblInfo, []*model.IndexInfo{indexInfo}))
			require.NoError(t, initSPFreshReorg(w, nil, job, tblInfo, []*model.IndexInfo{indexInfo}))
			require.Equal(t, 1, reads)
			require.False(t, job.ReorgMeta.UseCloudStorage)
			require.Equal(t, model.ReorgTypeIngest, job.ReorgMeta.ReorgTp)
			require.Equal(t, model.BackfillStateInapplicable, indexInfo.BackfillState)
			require.Empty(t, w.jobContext(job.ID, job.ReorgMeta).cloudStorageURI)
		})
	}
}

func TestSPFreshReorgBulkRouteSurvivesConfigChange(t *testing.T) {
	originalReadURI := spfreshBulkCloudStorageURI
	originalResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		spfreshBulkCloudStorageURI = originalReadURI
		resolveSPFreshWorkerTargets = originalResolve
	})
	reads, resolutions := 0, 0
	spfreshBulkCloudStorageURI = func() string {
		reads++
		return "s3://spfresh-mvp/frozen-route"
	}
	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		resolutions++
		return spfreshWorkerTargets{keyspaceID: 1, workerAddrs: []string{"http://worker:8080"}}, nil
	}
	tblInfo, indexInfo := newSPFreshBulkTestTable()
	job := newSPFreshBulkTestJob()
	job.ReorgMeta.IsDistReorg = true
	job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
	w := newSPFreshBulkTestWorker(job.ID)
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	txn, err := store.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	jobCtx := &jobContext{metaMut: meta.NewMutator(txn)}
	indexInfos := []*model.IndexInfo{indexInfo}
	require.NoError(t, initSPFreshReorg(w, jobCtx, job, tblInfo, indexInfos))
	run := w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk
	require.NotNil(t, run)
	require.Equal(t, run.runID, job.ReorgMeta.SPFreshBulkRunID)

	spfreshBulkCloudStorageURI = func() string {
		reads++
		return ""
	}
	require.NoError(t, initSPFreshReorg(w, jobCtx, job, tblInfo, indexInfos))
	require.True(t, job.ReorgMeta.UseCloudStorage)
	require.Same(t, run, w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk)
	require.Equal(t, model.BackfillStateInapplicable, indexInfo.BackfillState)
	require.Equal(t, 1, reads)
	require.Equal(t, 1, resolutions)
}

func TestSPFreshBulkFirstTransactionRetryUsesNewSnapshot(t *testing.T) {
	originalURI, originalTargets := spfreshBulkCloudStorageURI, resolveSPFreshWorkerTargets
	t.Cleanup(func() { spfreshBulkCloudStorageURI, resolveSPFreshWorkerTargets = originalURI, originalTargets })
	spfreshBulkCloudStorageURI = func() string { return "s3://spfresh-mvp/retry" }
	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{keyspaceID: 7, workerAddrs: []string{"http://worker"}}, nil
	}
	tbl, index := newSPFreshBulkTestTable()
	job := newSPFreshBulkTestJob()
	job.RealStartTS = 100
	require.NoError(t, ensureSPFreshBuildAdmission(job, []*model.IndexInfo{index}))
	job.ReorgMeta.IsDistReorg = true
	w := newSPFreshBulkTestWorker(job.ID)
	selected, err := admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
	require.NoError(t, err)
	require.True(t, selected)
	state := w.jobContext(job.ID, job.ReorgMeta)
	firstRun := state.spfresh.bulk
	// A failed initial transaction reloads the unadmitted job with a new snapshot.
	job.RealStartTS, job.ReorgMeta.SPFreshBuildGeneration = 101, 0
	require.NoError(t, ensureSPFreshBuildAdmission(job, []*model.IndexInfo{index}))
	selected, err = admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
	require.NoError(t, err)
	require.True(t, selected)
	require.NotSame(t, firstRun, state.spfresh.bulk)
	require.Equal(t, job.RealStartTS, state.spfresh.bulk.sourceSnapshotTS)
	job.SchemaState = model.StateDeleteOnly
	job.RealStartTS++
	_, err = admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
	require.ErrorContains(t, err, "snapshot changed after schema progress")
}

func TestSPFreshAdmissionRetryRecapturesRoute(t *testing.T) {
	for _, initialBulk := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy_to_bulk", true: "bulk_to_legacy"}[initialBulk], func(t *testing.T) {
			originalURI, originalTargets := spfreshBulkCloudStorageURI, resolveSPFreshWorkerTargets
			t.Cleanup(func() { spfreshBulkCloudStorageURI, resolveSPFreshWorkerTargets = originalURI, originalTargets })
			uri := ""
			if initialBulk {
				uri = "s3://spfresh-mvp/retry"
			}
			spfreshBulkCloudStorageURI = func() string { return uri }
			resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
				return spfreshWorkerTargets{keyspaceID: 7, workerAddrs: []string{"http://worker"}}, nil
			}
			tbl, index := newSPFreshBulkTestTable()
			job := newSPFreshBulkTestJob()
			job.ReorgMeta.IsDistReorg = true
			w := newSPFreshBulkTestWorker(job.ID)
			selected, err := admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
			require.NoError(t, err)
			require.Equal(t, initialBulk, selected)

			uri = "s3://spfresh-mvp/retry"
			if initialBulk {
				uri = ""
			}
			// Configuration changes cannot change a route within the same snapshot.
			selected, err = admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
			require.NoError(t, err)
			require.Equal(t, initialBulk, selected)

			// Only retrying the uncommitted first transaction admits the new route.
			job.RealStartTS++
			selected, err = admitSPFreshBulkBuild(w, job, tbl, []*model.IndexInfo{index})
			require.NoError(t, err)
			require.Equal(t, !initialBulk, selected)
			require.Equal(t, selected, job.ReorgMeta.UseCloudStorage)
			run := w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk
			if selected {
				require.NotNil(t, run)
				require.Equal(t, job.RealStartTS, run.sourceSnapshotTS)
			} else {
				require.Nil(t, run)
			}
		})
	}
}

func TestSPFreshBulkOrchestratorOrder(t *testing.T) {
	var calls []string
	stage := func(name string) func(context.Context, *spfreshBulkRunContext) error {
		return func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, name)
			return nil
		}
	}
	err := runSPFreshBulkOrchestrator(context.Background(), &spfreshBulkRunContext{
		sourceRows: 1,
		buildID:    "1-2-3-4",
		runID:      "run-8",
	}, spfreshBulkStages{
		building:          stage("building"),
		scanAndUpload:     stage("scan_upload"),
		buildAndWriteRuns: stage("offline_build_run_write"),
		importRuns:        stage("import"),
		verify:            stage("verify"),
		publish:           stage("publish"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"building", "scan_upload", "offline_build_run_write", "import", "verify", "publish",
	}, calls)
}

func TestSPFreshBulkStageLogging(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	originalLogger := logutil.AppLogger
	logutil.AppLogger = zap.New(core)
	t.Cleanup(func() { logutil.AppLogger = originalLogger })

	run := &spfreshBulkRunContext{
		buildID:            "1-2-3-4",
		runID:              "run-13",
		sourceSnapshotTS:   101,
		sourceRows:         100000,
		baseObjectBytes:    300,
		leafEntryCount:     100000,
		nonRootRunURI:      "s3://spfresh-mvp/run-13/runs/non-root.spfmvp",
		nonRootRecordCount: 200,
		nonRootObjectBytes: 201,
		rootRunURI:         "s3://spfresh-mvp/run-13/runs/root.spfmvp",
		rootRecordCount:    3,
		rootObjectBytes:    4,
		bulkImportTS:       102,
	}
	started := time.Now()
	logSPFreshBulkStage(run, "offline_build", started)
	logSPFreshBulkStage(run, "run_write", started)
	logSPFreshBulkStage(run, "verify", started)

	entries := observed.FilterMessage("SPFresh MVP bulk stage completed").All()
	require.Len(t, entries, 3)
	require.Equal(t, uint64(100000), entries[0].ContextMap()["leaf_entries"])
	require.Equal(t, run.nonRootRunURI, entries[1].ContextMap()["non_root_artifact_uri"])
	require.Equal(t, run.rootRunURI, entries[1].ContextMap()["root_artifact_uri"])
	require.Equal(t, uint64(102), entries[2].ContextMap()["bulk_import_ts"])
}

func TestSPFreshBulkOrchestratorStopsAtFirstError(t *testing.T) {
	var calls []string
	stages := spfreshBulkStages{
		building: func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, "building")
			return nil
		},
		scanAndUpload: func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, "scan_upload")
			return errors.New("scan failed")
		},
		buildAndWriteRuns: func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, "unexpected")
			return nil
		},
		importRuns: func(context.Context, *spfreshBulkRunContext) error { return nil },
		verify:     func(context.Context, *spfreshBulkRunContext) error { return nil },
		publish:    func(context.Context, *spfreshBulkRunContext) error { return nil },
	}
	err := runSPFreshBulkOrchestrator(context.Background(), &spfreshBulkRunContext{}, stages)
	require.EqualError(t, err, "scan failed")
	require.Equal(t, []string{"building", "scan_upload"}, calls)
}

func TestSPFreshBulkDDLRouteFailsClosedWithoutInProcessContext(t *testing.T) {
	err := runSPFreshBulkReorgWithStages(context.Background(), nil, spfreshBulkStages{})
	require.ErrorContains(t, err, "refusing fallback")
}

func newSPFreshBulkTestJob() *model.Job {
	return &model.Job{
		ID:          2,
		RealStartTS: 123,
		ReorgMeta:   &model.DDLReorgMeta{},
	}
}

func newSPFreshBulkTestWorker(jobID int64) *worker {
	ddlCtx := &ddlCtx{}
	ddlCtx.jobCtx.jobCtxMap = make(map[int64]*ReorgContext)
	ddlCtx.jobCtx.jobCtxMap[jobID] = NewReorgContext()
	return &worker{ddlCtx: ddlCtx}
}

func newSPFreshBulkTestTable() (*model.TableInfo, *model.IndexInfo) {
	pkType := fieldtypes.NewFieldType(mysql.TypeLonglong)
	pkType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	vectorType := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vectorType.SetFlen(768)
	vectorType.SetFlag(mysql.NotNullFlag)
	indexInfo := &model.IndexInfo{
		ID:    4,
		Name:  pmodel.NewCIStr("idx_vec"),
		State: model.StateNone,
		Tp:    pmodel.IndexTypeSPFresh,
		Columns: []*model.IndexColumn{{
			Name:   pmodel.NewCIStr("vec"),
			Offset: 1,
			Length: fieldtypes.UnspecifiedLength,
		}},
		VectorInfo: &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindSPFresh,
			Dimension:      768,
			DistanceMetric: model.DistanceMetricL2,
		},
	}
	tblInfo := &model.TableInfo{
		ID:         3,
		Name:       pmodel.NewCIStr("t"),
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *pkType,
			},
			{
				ID:        2,
				Name:      pmodel.NewCIStr("vec"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *vectorType,
			},
		},
		Indices: []*model.IndexInfo{indexInfo},
	}
	return tblInfo, indexInfo
}

func TestSPFreshBulkAdmissionUsesExclusiveJobPrefixes(t *testing.T) {
	tbl, idx := newSPFreshBulkTestTable()
	job := newSPFreshBulkTestJob()
	job.ReorgMeta.IsDistReorg = true
	uri := "s3://spfresh-mvp/shared?endpoint=http%3A%2F%2Fminio%3A9000&force-path-style=true"
	first, selected, err := prepareSPFreshBulkAdmission(uri, job, tbl, idx, 0, []string{"http://worker:19000"})
	require.NoError(t, err)
	require.True(t, selected)
	require.Equal(t, "shared-j2", first.runID)
	require.Equal(t, "s3://spfresh-mvp/shared-j2", first.artifactPrefix)
	require.Contains(t, first.cloudStorageURI, "endpoint=")
	job.ID++
	second, _, err := prepareSPFreshBulkAdmission(uri, job, tbl, idx, 0, []string{"http://worker:19000"})
	require.NoError(t, err)
	require.NotEqual(t, first.runID, second.runID)
}

func TestSPFreshBulkEmptyBuildRequiresBootstrapAcknowledgement(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "bootstrap_failure"}[fail], func(t *testing.T) {
			var calls []string
			run := &spfreshBulkRunContext{sourceSnapshotTS: 123}
			err := runSPFreshBulkOrchestrator(context.Background(), run, spfreshBulkStages{
				building:      func(context.Context, *spfreshBulkRunContext) error { calls = append(calls, "building"); return nil },
				scanAndUpload: func(context.Context, *spfreshBulkRunContext) error { calls = append(calls, "scan"); return nil },
				// Build/import/verify/nonempty publish are deliberately absent: zero input
				// must reach its own Storage publication barrier without fabricating SSTs.
				publishEmpty: func(_ context.Context, r *spfreshBulkRunContext) error {
					calls = append(calls, "bootstrap")
					require.Equal(t, uint64(123), r.sourceSnapshotTS)
					require.Error(t, r.checkReadyForPublic())
					if fail {
						return errors.New("bootstrap failed")
					}
					return nil
				},
			})
			require.Equal(t, []string{"building", "scan", "bootstrap"}, calls)
			require.Zero(t, run.bulkImportTS)
			require.Zero(t, run.readyIndexTS)
			if fail {
				require.ErrorContains(t, err, "bootstrap failed")
				require.Error(t, run.checkReadyForPublic())
			} else {
				require.NoError(t, err)
				require.NoError(t, run.checkReadyForPublic())
			}
		})
	}
}
