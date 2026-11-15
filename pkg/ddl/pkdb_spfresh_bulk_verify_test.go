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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSPFreshBulkReadyRequestUsesRunDistance(t *testing.T) {
	run := newSPFreshBulkScanTestRun(4, 123, "cosine-ready")
	run.distance = "cosine"
	run.vectorColumnID = 2
	run.sourceRows = 4
	run.sourceFingerprint = pkdbspfreshbulk.MultisetFingerprint{
		Count: 4, XOR256: strings.Repeat("0", 64), Sum256: strings.Repeat("0", 64),
	}
	run.finalManifestRef = pkdbspfreshbulk.ManifestRef{URI: run.artifactPrefix + "/final.json", SHA256: strings.Repeat("1", 64)}
	run.finalManifest = &pkdbspfreshbulk.FinalManifest{
		Identity: spfreshBulkBuildIdentity(run), SourceRows: 4, LeafEntryCount: 4,
		Fingerprint: run.sourceFingerprint, PartitionCount: 10, LeafPartitionCount: 4,
		RootLevel: 4, MaxPartitionKey: 802,
	}
	run.importCompleted = true
	run.bulkImportTS = 456
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body remote.SPFreshBulkReadyRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		require.Equal(t, run.distance, body.Distance)
		require.Equal(t, run.bulkImportTS, body.BulkImportTS)
		require.Equal(t, run.finalManifest.MaxPartitionKey, body.MaxPartitionKey)
		require.Equal(t, run.finalManifest.PartitionCount, body.PartitionCount)
		require.NoError(t, json.NewEncoder(w).Encode(remote.SPFreshBulkReadyResponse{
			BuildID: run.buildID, SourceSnapshotTS: run.sourceSnapshotTS,
			IndexTS: 456, RootLevel: 4, RootIsLeaf: false,
		}))
	}))
	defer server.Close()
	run.workerURL = server.URL
	require.NoError(t, publishSPFreshBulkReady(context.Background(), run))
	require.Equal(t, uint64(456), run.readyIndexTS)
}

func TestSPFreshBulkReadyRequestUsesSingleWorkerTreeFacts(t *testing.T) {
	run := newSPFreshBulkScanTestRun(4, 123, "single-ready")
	run.vectorColumnID = 2
	run.shardCount = 1
	run.sourceRows = 400
	run.leafEntryCount = 400
	run.builtPartitionCount = 7
	run.builtLeafPartitions = 4
	run.builtRootLevel = 2
	run.builtMaxPartitionKey = 7
	run.importCompleted = true
	run.bulkImportTS = 456
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body remote.SPFreshBulkReadyRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		require.Equal(t, run.builtMaxPartitionKey, body.MaxPartitionKey)
		require.Equal(t, run.builtPartitionCount, body.PartitionCount)
		require.Equal(t, run.builtLeafPartitions, body.LeafPartitionCount)
		require.Equal(t, run.builtRootLevel, body.RootLevel)
		require.NoError(t, json.NewEncoder(w).Encode(remote.SPFreshBulkReadyResponse{
			BuildID: run.buildID, SourceSnapshotTS: run.sourceSnapshotTS,
			IndexTS: run.bulkImportTS, RootLevel: run.builtRootLevel,
		}))
	}))
	defer server.Close()
	run.workerURL = server.URL
	require.NoError(t, publishSPFreshBulkReady(context.Background(), run))
	require.Equal(t, run.bulkImportTS, run.readyIndexTS)
}

func TestSPFreshBulkVerifyFailureStopsBeforeReadyAndPublic(t *testing.T) {
	var calls []string
	stage := func(name string) func(context.Context, *spfreshBulkRunContext) error {
		return func(context.Context, *spfreshBulkRunContext) error {
			calls = append(calls, name)
			return nil
		}
	}
	err := runSPFreshBulkReorgWithStages(
		context.Background(),
		&spfreshBulkRunContext{sourceRows: 1},
		spfreshBulkStages{
			building:          stage("building"),
			scanAndUpload:     stage("scan"),
			buildAndWriteRuns: stage("build"),
			importRuns:        stage("import"),
			verify: func(context.Context, *spfreshBulkRunContext) error {
				calls = append(calls, "verify")
				return errors.New("verification failed")
			},
			publish: stage("unexpected_ready"),
		},
	)
	require.EqualError(t, err, "verification failed")
	require.Equal(t, []string{"building", "scan", "build", "import", "verify"}, calls)
}

func TestSPFreshBulkPublicRequiresReadyGuard(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	originalLogger := logutil.AppLogger
	logutil.AppLogger = zap.New(core)
	t.Cleanup(func() { logutil.AppLogger = originalLogger })

	job := newSPFreshBulkTestJob()
	job.ReorgMeta.UseCloudStorage = true
	w := newSPFreshBulkTestWorker(job.ID)
	_, index := newSPFreshBulkTestTable()
	index.State = model.StateWriteReorganization
	reorgCtx := w.jobContext(job.ID, job.ReorgMeta)
	// A fresh owner's missing context must not bypass the bulk publication guard.
	require.ErrorContains(t, requireSPFreshBulkReadyBeforePublic(w, job, []*model.IndexInfo{index}), "before Ready")
	logSPFreshBulkPublic(w, job, []*model.IndexInfo{index})
	reorgCtx.spfresh.bulk = &spfreshBulkRunContext{sourceSnapshotTS: 123}

	require.ErrorContains(t, requireSPFreshBulkReadyBeforePublic(w, job, []*model.IndexInfo{index}), "before Ready")
	require.NotEqual(t, model.StatePublic, index.State)
	reorgCtx.spfresh.bulk.finalManifest = &pkdbspfreshbulk.FinalManifest{}
	reorgCtx.spfresh.bulk.bulkImportTS = 456
	reorgCtx.spfresh.bulk.readyIndexTS = 455
	require.ErrorContains(t, requireSPFreshBulkReadyBeforePublic(w, job, []*model.IndexInfo{index}), "before Ready")
	logSPFreshBulkPublic(w, job, []*model.IndexInfo{index})
	require.Empty(t, observed.FilterMessage("SPFresh MVP bulk stage completed").All())
	reorgCtx.spfresh.bulk.readyIndexTS = 456
	require.NoError(t, requireSPFreshBulkReadyBeforePublic(w, job, []*model.IndexInfo{index}))
	logSPFreshBulkPublic(w, job, []*model.IndexInfo{index})
	require.Len(t, observed.FilterMessage("SPFresh MVP bulk stage completed").FilterField(zap.String("stage", "publish")).All(), 1)
}

func TestSPFreshBulkDistributedVerifyRequiresActualImportCompletion(t *testing.T) {
	for _, fault := range []string{"incomplete", "timestamp", "source", "identity"} {
		t.Run(fault, func(t *testing.T) {
			store, run := newSPFreshBulkFinalManifestFixture(t)
			require.NoError(t, proveSPFreshTestBuild(context.Background(), store, run))
			run.importCompleted = true
			run.bulkImportTS = run.sourceSnapshotTS + 1
			require.NoError(t, verifySPFreshDistributedImportState(run))
			switch fault {
			case "incomplete":
				run.importCompleted = false
			case "timestamp":
				run.bulkImportTS = run.sourceSnapshotTS
			case "source":
				run.sourceRows++
			case "identity":
				run.indexID++
			}
			require.ErrorContains(t, verifySPFreshDistributedImportState(run), "does not match")
		})
	}
}
