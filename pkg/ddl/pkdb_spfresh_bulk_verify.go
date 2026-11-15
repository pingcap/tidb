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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
)

// verifySPFreshDistributedImport checks the accepted manifest and completed
// import. The immutable manifest is not consumed again after import.
func verifySPFreshDistributedImport(_ context.Context, run *spfreshBulkRunContext) error {
	return verifySPFreshDistributedImportState(run)
}

func verifySPFreshDistributedImportState(run *spfreshBulkRunContext) error {
	if run == nil || run.finalManifest == nil || run.finalManifestRef.URI == "" {
		return errors.New("SPFresh distributed verify requires FinalManifest")
	}
	manifest := run.finalManifest
	if manifest.Identity != spfreshBulkBuildIdentity(run) ||
		manifest.SourceRows != run.sourceRows ||
		manifest.LeafEntryCount != run.sourceRows ||
		manifest.Fingerprint != run.sourceFingerprint ||
		!run.importCompleted ||
		run.bulkImportTS == 0 || run.bulkImportTS <= run.sourceSnapshotTS {
		return errors.New("SPFresh distributed import does not match FinalManifest conservation or timestamp")
	}
	return nil
}

func verifySPFreshSingleImportState(run *spfreshBulkRunContext) error {
	if run == nil || !run.importCompleted || run.shardCount != 1 || run.finalManifest != nil || run.sourceRows == 0 ||
		run.leafEntryCount != run.sourceRows || run.builtPartitionCount == 0 ||
		run.builtLeafPartitions == 0 || run.builtLeafPartitions > run.builtPartitionCount ||
		run.builtMaxPartitionKey != run.builtPartitionCount ||
		run.bulkImportTS == 0 || run.bulkImportTS <= run.sourceSnapshotTS {
		return errors.New("SPFresh single-worker import does not match direct-build conservation or timestamp")
	}
	return nil
}

func exactSPFreshBulkObjectKey(prefix, uri string) (string, error) {
	prefix += "/"
	if !strings.HasPrefix(uri, prefix) {
		return "", errors.Errorf("SPFresh MVP artifact URI %q is outside admitted prefix", uri)
	}
	key := strings.TrimPrefix(uri, prefix)
	if key == "" || strings.Contains(key, "?") || strings.Contains(key, "#") {
		return "", errors.Errorf("SPFresh MVP artifact URI %q is not an exact object", uri)
	}
	return key, nil
}

func publishSPFreshBulkReady(ctx context.Context, run *spfreshBulkRunContext) error {
	var maxPartitionKey, partitionCount, leafPartitionCount, leafEntryCount uint64
	var rootLevel uint32
	if run != nil && run.finalManifest != nil {
		if err := verifySPFreshDistributedImportState(run); err != nil {
			return err
		}
		maxPartitionKey = run.finalManifest.MaxPartitionKey
		partitionCount = run.finalManifest.PartitionCount
		leafPartitionCount = run.finalManifest.LeafPartitionCount
		leafEntryCount = run.finalManifest.LeafEntryCount
		rootLevel = run.finalManifest.RootLevel
	} else {
		if err := verifySPFreshSingleImportState(run); err != nil {
			return err
		}
		maxPartitionKey = run.builtMaxPartitionKey
		partitionCount = run.builtPartitionCount
		leafPartitionCount = run.builtLeafPartitions
		leafEntryCount = run.leafEntryCount
		rootLevel = run.builtRootLevel
	}
	result, err := remote.CommitSPFreshBulkReady(
		ctx,
		util.InternalHTTPClient(),
		run.workerURL,
		remote.SPFreshBulkReadyRequest{
			StoredColumns:      run.storedColumns,
			HandleColumn:       run.handleColumn,
			BuildID:            run.buildID,
			KeyspaceID:         run.keyspaceID,
			TableID:            run.tableID,
			IndexID:            run.indexID,
			SourceSnapshotTS:   run.sourceSnapshotTS,
			Dimension:          uint64(run.dimension),
			Distance:           run.distance,
			AlgorithmSeed:      run.algorithmSeed,
			VectorColumnID:     run.vectorColumnID,
			BulkImportTS:       run.bulkImportTS,
			MaxPartitionKey:    maxPartitionKey,
			PartitionCount:     partitionCount,
			LeafPartitionCount: leafPartitionCount,
			LeafEntryCount:     leafEntryCount,
			RootLevel:          rootLevel,
		},
	)
	if err != nil {
		return err
	}
	run.readyIndexTS = result.IndexTS
	run.rootLevel = result.RootLevel
	return nil
}

func requireSPFreshBulkReadyBeforePublic(
	w *worker,
	job *model.Job,
	indexInfos []*model.IndexInfo,
) error {
	if !isSPFreshBulkReorg(job, indexInfos) {
		return nil
	}
	return w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk.checkReadyForPublic()
}

func (run *spfreshBulkRunContext) checkReadyForPublic() error {
	if run != nil && run.sourceSnapshotTS != 0 && run.sourceRows == 0 && run.emptyReady {
		return nil
	}
	if run == nil || run.sourceSnapshotTS == 0 ||
		run.bulkImportTS == 0 || run.readyIndexTS != run.bulkImportTS {
		return errors.New("SPFresh MVP bulk index cannot become Public before Ready(B)")
	}
	return nil
}

func logSPFreshBulkPublic(w *worker, job *model.Job, indexInfos []*model.IndexInfo) {
	if w == nil || w.ddlCtx == nil || !isSPFreshBulkReorg(job, indexInfos) {
		return
	}
	run := w.jobContext(job.ID, job.ReorgMeta).spfresh.bulk
	if run.checkReadyForPublic() != nil {
		return
	}
	logSPFreshBulkStage(run, "publish", run.readyStartedAt)
}
