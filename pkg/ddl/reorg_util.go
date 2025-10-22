// Copyright 2025 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

func initJobReorgMetaFromVariables(ctx context.Context, job *model.Job, tbl table.Table, sctx sessionctx.Context) error {
	m := NewDDLReorgMeta(sctx)

	//nolint:forbidigo
	// sctx comes from the user session.
	sessVars := sctx.GetSessionVars()

	var setReorgParam bool
	var setDistTaskParam bool

	switch job.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		setReorgParam = true
		setDistTaskParam = true
	case model.ActionModifyColumn:
		setReorgParam = true
		setDistTaskParam = modifyColumnNeedReorg(job.CtxVars)
	case model.ActionReorganizePartition,
		model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		setReorgParam = true
	case model.ActionMultiSchemaChange:
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			switch sub.Type {
			case model.ActionAddIndex, model.ActionAddPrimaryKey:
				setReorgParam = true
				setDistTaskParam = true
			case model.ActionReorganizePartition,
				model.ActionRemovePartitioning,
				model.ActionAlterTablePartitioning:
				setReorgParam = true
			case model.ActionModifyColumn:
				setReorgParam = true
				setDistTaskParam = modifyColumnNeedReorg(sub.CtxVars)
			}
		}
	default:
		return nil
	}
	var tableSizeInBytes int64
	var cpuNum int
	// we don't use DXF service for bootstrap/upgrade related DDL, so no need to
	// calculate resources.
	initing := sctx.Value(sessionctx.Initing) != nil
	// some mock context may not have store, such as the schema tracker test.
	shouldCalResource := kerneltype.IsNextGen() && !initing && sctx.GetStore() != nil
	if (setReorgParam || setDistTaskParam) && shouldCalResource {
		tableSizeInBytes = getTableSizeByID(ctx, sctx.GetStore(), tbl)
		var err error
		cpuNum, err = scheduler.GetExecCPUNode(ctx)
		if err != nil {
			return err
		}
	}

	failpoint.Inject("MockTableSize", func(v failpoint.Value) {
		if size, ok := v.(int); ok && size > 0 {
			tableSizeInBytes = int64(size)
		}
	})

	var (
		autoConc, autoMaxNode int
		factorField           = zap.Skip()
	)
	if shouldCalResource {
		factors, err := dxfhandle.GetScheduleTuneFactors(ctx, sctx.GetStore().GetKeyspace())
		if err != nil {
			return err
		}
		calc := scheduler.NewRCCalcForAddIndex(tableSizeInBytes, cpuNum, factors)
		autoConc = calc.CalcConcurrency()
		autoMaxNode = calc.CalcMaxNodeCountForAddIndex()
		factorField = zap.Float64("amplifyFactor", factors.AmplifyFactor)
	}
	if setReorgParam {
		if shouldCalResource && setDistTaskParam {
			m.SetConcurrency(autoConc)
		} else {
			if sv, ok := sessVars.GetSystemVar(vardef.TiDBDDLReorgWorkerCount); ok {
				m.SetConcurrency(variable.TidbOptInt(sv, 0))
			}
		}
		if sv, ok := sessVars.GetSystemVar(vardef.TiDBDDLReorgBatchSize); ok {
			m.SetBatchSize(variable.TidbOptInt(sv, 0))
		}
		m.SetMaxWriteSpeed(int(vardef.DDLReorgMaxWriteSpeed.Load()))
	}

	if setDistTaskParam {
		m.IsDistReorg = vardef.EnableDistTask.Load()
		m.IsFastReorg = vardef.EnableFastReorg.Load()
		m.TargetScope = dxfhandle.GetTargetScope()
		if shouldCalResource {
			m.MaxNodeCount = autoMaxNode
		} else {
			if sv, ok := sessVars.GetSystemVar(vardef.TiDBMaxDistTaskNodes); ok {
				m.MaxNodeCount = variable.TidbOptInt(sv, 0)
				if m.MaxNodeCount == -1 { // -1 means calculate automatically
					m.MaxNodeCount = scheduler.CalcMaxNodeCountByStoresNum(ctx, sctx.GetStore())
				}
			}
		}

		if hasSysDB(job) {
			if m.IsDistReorg {
				logutil.DDLLogger().Info("cannot use distributed task execution on system DB",
					zap.Stringer("job", job))
			}
			m.IsDistReorg = false
			m.IsFastReorg = false
			failpoint.Inject("reorgMetaRecordFastReorgDisabled", func(_ failpoint.Value) {
				LastReorgMetaFastReorgDisabled = true
			})
		}
		if m.IsDistReorg && !m.IsFastReorg {
			return dbterror.ErrUnsupportedDistTask
		}
	}
	failpoint.InjectCall("beforeInitReorgMeta", m)
	job.ReorgMeta = m
	logutil.DDLLogger().Info("initialize reorg meta",
		zap.Int64("jobID", job.ID),
		zap.String("jobSchema", job.SchemaName),
		zap.String("jobTable", job.TableName),
		zap.Stringer("jobType", job.Type),
		zap.Bool("enableDistTask", m.IsDistReorg),
		zap.Bool("enableFastReorg", m.IsFastReorg),
		zap.String("targetScope", m.TargetScope),
		zap.Int("maxNodeCount", m.MaxNodeCount),
		zap.String("tableSizeInBytes", units.BytesSize(float64(tableSizeInBytes))),
		zap.Int("concurrency", m.GetConcurrency()),
		zap.Int("batchSize", m.GetBatchSize()),
		factorField,
	)
	return nil
}

func modifyColumnNeedReorg(jobCtxVars []any) bool {
	if len(jobCtxVars) > 0 {
		if v, ok := jobCtxVars[0].(bool); ok {
			return v
		}
	}
	return false
}

func getTableSizeByID(ctx context.Context, store kv.Storage, tbl table.Table) int64 {
	helperStore, ok := store.(helper.Storage)
	if !ok {
		logutil.DDLLogger().Warn("store does not implement helper.Storage interface",
			zap.String("storeType", fmt.Sprintf("%T", store)))
		return 0
	}
	h := helper.NewHelper(helperStore)
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		logutil.DDLLogger().Warn("failed to get PD HTTP client for calculating table size",
			zap.Int64("tableID", tbl.Meta().ID),
			zap.Error(err))
		return 0
	}
	var pids []int64
	if tbl.Meta().Partition != nil {
		for _, def := range tbl.Meta().Partition.Definitions {
			pids = append(pids, def.ID)
		}
	} else {
		pids = []int64{tbl.Meta().ID}
	}
	var totalSize int64
	for _, pid := range pids {
		size, err := estimateTableSizeByID(ctx, pdCli, helperStore, pid)
		if err != nil {
			logutil.DDLLogger().Warn("failed to estimate table size for calculating concurrency",
				zap.Int64("physicalID", pid),
				zap.Error(err))
		}
		if size == 0 {
			regionStats, err := h.GetPDRegionStats(ctx, pid, false)
			if err != nil {
				logutil.DDLLogger().Warn("failed to get region stats for calculating concurrency",
					zap.Int64("physicalID", pid),
					zap.Error(err))
				return 0
			}
			totalSize += regionStats.StorageSize * units.MiB
		}
		totalSize += size
	}
	return totalSize
}

func estimateTableSizeByID(ctx context.Context, pdCli pdhttp.Client, store helper.Storage, pid int64) (int64, error) {
	sk, ek := tablecodec.GetTableHandleKeyRange(pid)
	start, end := store.GetCodec().EncodeRegionRange(sk, ek)
	var totalSize int64
	for {
		regionInfos, err := pdCli.GetRegionsByKeyRange(ctx, pdhttp.NewKeyRange(start, end), 128)
		if err != nil {
			return 0, err
		}
		if len(regionInfos.Regions) == 0 {
			break
		}
		for _, r := range regionInfos.Regions {
			totalSize += r.ApproximateSize * units.MiB
		}
		lastKey := regionInfos.Regions[len(regionInfos.Regions)-1].EndKey
		start, err = hex.DecodeString(lastKey)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(start, end) >= 0 {
			break
		}
	}
	return totalSize, nil
}
