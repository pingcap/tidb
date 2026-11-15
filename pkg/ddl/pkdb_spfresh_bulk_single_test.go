// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/stretchr/testify/require"
)

type singleProofStorage struct {
	storage.ExternalStorage
	opens map[string]int
}

func (s *singleProofStorage) Open(ctx context.Context, key string, options *storage.ReaderOption) (storage.ExternalFileReader, error) {
	s.opens[key]++
	return s.ExternalStorage.Open(ctx, key, options)
}

func newSPFreshSingleProofFixture(t *testing.T, leafRoot bool) (*singleProofStorage, *spfreshBulkRunContext) {
	t.Helper()
	store := &singleProofStorage{ExternalStorage: storage.NewMemStorage(), opens: make(map[string]int)}
	run := newSPFreshBulkScanTestRun(4, 123, "single-proof")
	run.shardCount, run.sourceRows, run.leafEntryCount = 1, 2, 2
	run.builtRootLevel, run.builtPartitionCount, run.builtLeafPartitions, run.builtMaxPartitionKey = 2, 3, 2, 3
	write := func(artifact, role string, count uint64, records []pkdbspfreshbulk.SortedRunRecord) {
		encoded := encodeSPFreshBulkTestRun(t, run, artifact, role, count, 1, 3, records)
		key := "runs/" + artifact + ".spfmvp"
		require.NoError(t, store.WriteFile(context.Background(), key, encoded))
		ref := pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+key, encoded)
		if role == spfreshBulkRootRole {
			run.rootRef, run.rootRecordCount, run.rootObjectBytes = ref, uint64(len(records)), uint64(len(encoded))
		} else {
			run.nonRootRef, run.nonRootRecordCount, run.nonRootObjectBytes = ref, uint64(len(records)), uint64(len(encoded))
		}
	}
	if leafRoot {
		run.builtRootLevel, run.builtPartitionCount, run.builtLeafPartitions, run.builtMaxPartitionKey = 1, 1, 1, 1
		write("root", spfreshBulkRootRole, 2, spfreshBulkTestPartitionRecords(t, run, 1, 1, []uint64{1, 2}))
	} else {
		nonroot := append(spfreshBulkTestPartitionRecords(t, run, 2, 1, []uint64{1}), spfreshBulkTestPartitionRecords(t, run, 3, 1, []uint64{2})...)
		write("non-root", spfreshBulkNonRootRole, 2, nonroot)
		write("root", spfreshBulkRootRole, 0, spfreshBulkTestPartitionRecords(t, run, 1, 2, []uint64{2, 3}))
	}
	return store, run
}

func TestSPFreshBulkSingleProofReadsEachRunOnceAndStagesRootLast(t *testing.T) {
	for _, leafRoot := range []bool{false, true} {
		store, run := newSPFreshSingleProofFixture(t, leafRoot)
		var roles []string
		require.NoError(t, proveAndStageSPFreshSingleRuns(context.Background(), store, run,
			func(_ context.Context, decoded spfreshBulkSortedRun, _ pkdbspfreshbulk.ManifestRef) error {
				roles = append(roles, decoded.header.RunRole)
				return nil
			}))
		if leafRoot {
			require.Equal(t, []string{"root"}, roles)
			require.Equal(t, map[string]int{"runs/root.spfmvp": 1}, store.opens)
		} else {
			require.Equal(t, []string{"non_root", "root"}, roles)
			require.Equal(t, map[string]int{"runs/non-root.spfmvp": 1, "runs/root.spfmvp": 1}, store.opens)
		}
		// No BaseFragments are present: source authentication belongs to the
		// worker's mandatory snapshot-bound read, never a later TiDB rescan.
		require.False(t, run.importCompleted)
		require.Zero(t, run.bulkImportTS)
		require.Error(t, verifySPFreshSingleImportState(run))
		run.importCompleted = true
		run.bulkImportTS = run.sourceSnapshotTS + 1
		require.NoError(t, verifySPFreshSingleImportState(run))
		run.bulkImportTS = run.sourceSnapshotTS
		require.Error(t, verifySPFreshSingleImportState(run))
	}
}

func TestSPFreshBulkSingleProofRejectsCorruptionAndBrokenConservation(t *testing.T) {
	for _, fault := range []string{"pin", "identity", "source count", "tree counts", "root level", "unexpected non-root", "staging failure"} {
		t.Run(fault, func(t *testing.T) {
			store, run := newSPFreshSingleProofFixture(t, false)
			stage := discardSPFreshTestRun
			switch fault {
			case "pin":
				run.rootRef.SHA256 = strings.Repeat("0", 64)
			case "identity":
				run.indexID++
			case "source count":
				run.sourceRows++
			case "tree counts":
				run.builtLeafPartitions++
			case "unexpected non-root":
				run.builtRootLevel = 1
			case "staging failure":
				stage = func(context.Context, spfreshBulkSortedRun, pkdbspfreshbulk.ManifestRef) error {
					return errors.New("memory import failed")
				}
			case "root level":
				encoded := encodeSPFreshBulkTestRun(t, run, "root", "root", 0, 1, 1,
					spfreshBulkTestPartitionRecords(t, run, 1, 3, []uint64{2, 3}))
				require.NoError(t, store.WriteFile(context.Background(), "runs/root.spfmvp", encoded))
				run.rootRef = pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/runs/root.spfmvp", encoded)
				run.rootObjectBytes = uint64(len(encoded))
			}
			require.Error(t, proveAndStageSPFreshSingleRuns(context.Background(), store, run, stage))
			require.False(t, run.importCompleted)
			require.Zero(t, run.bulkImportTS)
			spfreshBulkReads.mu.Lock()
			require.Zero(t, spfreshBulkReads.used)
			spfreshBulkReads.mu.Unlock()
		})
	}
}
