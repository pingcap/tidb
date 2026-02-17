// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	meter_config "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/tidb/pkg/config"
	disthandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lcom "github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/cpu"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)
func (do *Domain) InitDistTaskLoop() error {
	taskManager := storage.NewTaskManager(do.dxfSessionPool)
	storage.SetTaskManager(taskManager)
	failpoint.Inject("MockDisableDistTask", func() {
		failpoint.Return(nil)
	})

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	if kv.IsUserKS(do.store) {
		sp, err := do.GetKSSessPool(keyspace.System)
		if err != nil {
			return err
		}
		storage.SetDXFSvcTaskMgr(storage.NewTaskManager(sp))

		// in nextgen, DXF runs as a service on SYSTEM ks and are shared by all
		// user keyspace
		logutil.BgLogger().Info("skip running DXF in user keyspace")
		return nil
	}
	if kv.IsSystemKS(do.store) {
		tidbCfg := config.GetGlobalConfig()
		if tidbCfg.MeteringStorageURI == "" {
			logutil.BgLogger().Warn("metering storage uri is empty, metering will be disabled")
		} else {
			mCfg, err := meter_config.NewFromURI(tidbCfg.MeteringStorageURI)
			if err != nil {
				return errors.Wrap(err, "failed to parse metering storage uri")
			}
			m, err := metering.NewMeter(mCfg)
			if err != nil {
				return errors.Wrap(err, "failed to create metering")
			}
			metering.SetMetering(m)
			do.wg.Run(func() {
				defer func() {
					metering.SetMetering(nil)
				}()
				m.StartFlushLoop(do.ctx)
			}, "dxfMeteringFlushLoop")
		}
	}

	var serverID string
	if intest.InTest {
		do.InitInfo4Test()
		serverID = disttaskutil.GenerateSubtaskExecID4Test(do.ddl.GetID())
	} else {
		serverID = disttaskutil.GenerateSubtaskExecID(ctx, do.ddl.GetID())
	}

	if serverID == "" {
		errMsg := fmt.Sprintf("TiDB node ID( = %s ) not found in available TiDB nodes list", do.ddl.GetID())
		return errors.New(errMsg)
	}
	managerCtx, cancel := context.WithCancel(ctx)
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancel)
	do.cancelFns.mu.Unlock()
	nodeRes, err := calculateNodeResource()
	if err != nil {
		return err
	}
	disthandle.SetNodeResource(nodeRes)
	executorManager, err := taskexecutor.NewManager(managerCtx, do.store, serverID, taskManager, nodeRes)
	if err != nil {
		return err
	}

	if err = executorManager.InitMeta(); err != nil {
		// executor manager loop will try to recover meta repeatedly, so we can
		// just log the error here.
		logutil.BgLogger().Warn("init task executor manager meta failed", zap.Error(err))
	}
	do.wg.Run(func() {
		defer func() {
			storage.SetTaskManager(nil)
		}()
		do.distTaskFrameworkLoop(ctx, taskManager, executorManager, serverID, nodeRes)
	}, "distTaskFrameworkLoop")
	if err := kv.RunInNewTxn(ctx, do.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		logger := logutil.BgLogger()
		return local.InitializeRateLimiterParam(m, logger)
	}); err != nil {
		logutil.BgLogger().Error("initialize global max batch split ranges failed", zap.Error(err))
	}
	return nil
}

func calculateNodeResource() (*proto.NodeResource, error) {
	logger := logutil.ErrVerboseLogger()
	totalMem, err := memory.MemTotal()
	if err != nil {
		// should not happen normally, as in main function of tidb-server, we assert
		// that memory.MemTotal() will not fail.
		return nil, err
	}
	totalCPU := cpu.GetCPUCount()
	if totalCPU <= 0 || totalMem <= 0 {
		return nil, errors.Errorf("invalid cpu or memory, cpu: %d, memory: %d", totalCPU, totalMem)
	}
	var totalDisk uint64
	cfg := config.GetGlobalConfig()
	sz, err := lcom.GetStorageSize(cfg.TempDir)
	if err != nil {
		logger.Warn("get storage size failed, use tidb_ddl_disk_quota instead", zap.Error(err))
		totalDisk = vardef.DDLDiskQuota.Load()
	} else {
		totalDisk = sz.Capacity
	}
	logger.Info("initialize node resource",
		zap.Int("total-cpu", totalCPU),
		zap.String("total-mem", units.BytesSize(float64(totalMem))),
		zap.String("total-disk", units.BytesSize(float64(totalDisk))))
	return proto.NewNodeResource(totalCPU, int64(totalMem), totalDisk), nil
}

func (do *Domain) distTaskFrameworkLoop(ctx context.Context, taskManager *storage.TaskManager, executorManager *taskexecutor.Manager, serverID string, nodeRes *proto.NodeResource) {
	err := executorManager.Start()
	if err != nil {
		logutil.BgLogger().Error("dist task executor manager start failed", zap.Error(err))
		return
	}
	logutil.BgLogger().Info("dist task executor manager started")
	defer func() {
		logutil.BgLogger().Info("stopping dist task executor manager")
		executorManager.Stop()
		logutil.BgLogger().Info("dist task executor manager stopped")
	}()

	var schedulerManager *scheduler.Manager
	startSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			return
		}
		schedulerManager = scheduler.NewManager(ctx, do.store, taskManager, serverID, nodeRes)
		schedulerManager.Start()
	}
	stopSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			logutil.BgLogger().Info("stopping dist task scheduler manager because the current node is not DDL owner anymore", zap.String("id", do.ddl.GetID()))
			schedulerManager.Stop()
			logutil.BgLogger().Info("dist task scheduler manager stopped", zap.String("id", do.ddl.GetID()))
		}
	}

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-do.exit:
			stopSchedulerMgrIfNeeded()
			return
		case <-ticker.C:
			if do.ddl.OwnerManager().IsOwner() {
				startSchedulerMgrIfNeeded()
			} else {
				stopSchedulerMgrIfNeeded()
			}
		}
	}
}

