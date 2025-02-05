// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"context"
	"net"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lightning "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// Register implements BackendCtx.
func (bc *litBackendCtx) Register(indexIDs []int64, uniques []bool, tbl table.Table) ([]Engine, error) {
	ret := make([]Engine, 0, len(indexIDs))

	for _, indexID := range indexIDs {
		en, ok := bc.engines[indexID]
		if !ok {
			continue
		}
		ret = append(ret, en)
	}
	if l := len(ret); l > 0 {
		if l != len(indexIDs) {
			return nil, errors.Errorf(
				"engines index ID number mismatch: job ID %d, required number of index IDs: %d, actual number of engines: %d",
				bc.jobID, len(indexIDs), l,
			)
		}
		return ret, nil
	}

	bc.memRoot.RefreshConsumption()
	numIdx := int64(len(indexIDs))
	ok := bc.memRoot.CheckConsume(numIdx * structSizeEngineInfo)
	if !ok {
		return nil, genEngineAllocMemFailedErr(bc.ctx, bc.memRoot, bc.jobID, indexIDs)
	}

	mgr := backend.MakeEngineManager(bc.backend)
	cfg := generateLocalEngineConfig(bc.GetImportTS())

	openedEngines := make(map[int64]*engineInfo, numIdx)

	for i, indexID := range indexIDs {
		openedEngine, err := mgr.OpenEngine(bc.ctx, cfg, tbl.Meta().Name.L, int32(indexID))
		if err != nil {
			logutil.Logger(bc.ctx).Warn(LitErrCreateEngineFail,
				zap.Int64("job ID", bc.jobID),
				zap.Int64("index ID", indexID),
				zap.Error(err))

			for _, e := range openedEngines {
				e.Close(true)
			}
			return nil, errors.Trace(err)
		}

		openedEngines[indexID] = newEngineInfo(
			bc.ctx,
			bc.jobID,
			indexID,
			uniques[i],
			cfg,
			openedEngine,
			openedEngine.GetEngineUUID(),
			bc.memRoot,
		)
	}

	for _, indexID := range indexIDs {
		ei := openedEngines[indexID]
		ret = append(ret, ei)
		bc.engines[indexID] = ei
	}
	bc.tbl = tbl

	logutil.Logger(bc.ctx).Info(LitInfoOpenEngine, zap.Int64("job ID", bc.jobID),
		zap.Int64s("index IDs", indexIDs),
		zap.Int64("current memory usage", bc.memRoot.CurrentUsage()),
		zap.Int64("memory limitation", bc.memRoot.MaxMemoryQuota()))
	return ret, nil
}

// UnregisterOpt controls the behavior of backend context unregistering.
type UnregisterOpt int

const (
	// OptCloseEngines only closes engines, it does not clean up sort path data.
	OptCloseEngines UnregisterOpt = 1 << iota
	// OptCleanData cleans up local sort dir data.
	OptCleanData
	// OptCheckDup checks if there is duplicate entry for unique indexes.
	OptCheckDup
)

// FinishAndUnregisterEngines implements BackendCtx.
func (bc *litBackendCtx) FinishAndUnregisterEngines(opt UnregisterOpt) error {
	bc.unregisterMu.Lock()
	defer bc.unregisterMu.Unlock()

	if len(bc.engines) == 0 {
		return nil
	}
	for _, ei := range bc.engines {
		ei.Close(opt&OptCleanData != 0)
	}

	if opt&OptCheckDup != 0 {
		for _, ei := range bc.engines {
			if ei.unique {
				dupeCtrl := bc.backend.GetDupeController(bc.cfg.WorkerConcurrency, nil)
				err := CollectAndHandleDuplicateErrors(bc.ctx, dupeCtrl, bc.tbl, ei.indexID, bc.initTS)
				if err != nil {
					return errors.Trace(err)
				}
				failpoint.Inject("mockCollectRemoteDuplicateRowsFailed", func(_ failpoint.Value) {
					failpoint.Return(context.DeadlineExceeded)
				})
			}
		}
	}

	bc.engines = make(map[int64]*engineInfo, 10)

	return nil
}

// NewRemoteDupControllerForDDLIngest creates a remote duplicate controller for DDL ingest.
func NewRemoteDupControllerForDDLIngest(ctx context.Context, job *model.Job, store kv.Storage) (dupCtrl *local.DupeController, cleanup func(), err error) {
	tidbCfg := config.GetGlobalConfig()
	tls, err := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort))),
		nil, nil, nil,
	)
	if err != nil {
		return nil, nil, err
	}
	resGroupName := job.ReorgMeta.ResourceGroupName
	concurrency := job.ReorgMeta.GetConcurrency()
	maxWriteSpeed := job.ReorgMeta.GetMaxWriteSpeed()
	cfg := newLocalBackendConfig(ctx, "", LitMemRoot, true, resGroupName, concurrency, maxWriteSpeed)
	//nolint: forcetypeassert
	pdCli := store.(tikv.Storage).GetRegionCache().PDClient()
	discovery := pdCli.GetServiceDiscovery()
	return local.NewRemoteDupeController(ctx, tls, *cfg, discovery)
}

// CollectAndHandleDuplicateErrors collects and handles duplicate errors.
func CollectAndHandleDuplicateErrors(
	ctx context.Context,
	dc *local.DupeController,
	tbl table.Table,
	idxID int64,
	minCommitTS uint64,
) error {
	hasDupe, err := dc.CollectRemoteDuplicateRows(ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
		SQLMode:     mysql.ModeStrictAllTables,
		SysVars:     defaultSystemVarsForDuplicateCheck,
		IndexID:     idxID,
		MinCommitTS: minCommitTS,
	}, lightning.ErrorOnDup)
	return handleErrorAfterCollectRemoteDuplicateRows(ctx, err, idxID, tbl, hasDupe)
}

func handleErrorAfterCollectRemoteDuplicateRows(
	ctx context.Context,
	err error,
	indexID int64,
	tbl table.Table,
	hasDupe bool,
) error {
	if err != nil && !common.ErrFoundIndexConflictRecords.Equal(err) {
		logutil.Logger(ctx).Error("remote duplicate checking failed", zap.Error(err),
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return errors.Trace(err)
	} else if hasDupe {
		logutil.Logger(ctx).Error("remote duplicate index key exist",
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))

		if common.ErrFoundIndexConflictRecords.Equal(err) {
			tErr, ok := errors.Cause(err).(*terror.Error)
			if !ok {
				return errors.Trace(kv.ErrKeyExists)
			}
			if len(tErr.Args()) != 4 {
				return errors.Trace(kv.ErrKeyExists)
			}
			//nolint: forcetypeassert
			indexName := tErr.Args()[1].(string)
			//nolint: forcetypeassert
			keyCols := tErr.Args()[2].([]string)
			return errors.Trace(kv.GenKeyExistsErr(keyCols, indexName))
		}
		return errors.Trace(kv.ErrKeyExists)
	}
	return nil
}
