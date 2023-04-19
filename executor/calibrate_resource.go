// Copyright 2023 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

var (
	// workloadBaseRUCostMap contains the base resource cost rate per 1 kv cpu within 1 second,
	// the data is calculated from benchmark result, these data might not be very accurate,
	// but is enough here because the maximum RU capacity is depended on both the cluster and
	// the workload.
	workloadBaseRUCostMap = map[ast.CalibrateResourceType]*baseResourceCost{
		ast.TPCC: {
			tidbCPU:       0.6,
			kvCPU:         0.15,
			readBytes:     units.MiB / 2,
			writeBytes:    units.MiB,
			readReqCount:  300,
			writeReqCount: 1750,
		},
		ast.OLTPREADWRITE: {
			tidbCPU:       1.25,
			kvCPU:         0.35,
			readBytes:     units.MiB * 4.25,
			writeBytes:    units.MiB / 3,
			readReqCount:  1600,
			writeReqCount: 1400,
		},
		ast.OLTPREADONLY: {
			tidbCPU:       2,
			kvCPU:         0.52,
			readBytes:     units.MiB * 28,
			writeBytes:    0,
			readReqCount:  4500,
			writeReqCount: 0,
		},
		ast.OLTPWRITEONLY: {
			tidbCPU:       1,
			kvCPU:         0,
			readBytes:     0,
			writeBytes:    units.MiB,
			readReqCount:  0,
			writeReqCount: 3550,
		},
	}

	// resourceGroupCtl is the ResourceGroupController in pd client
	resourceGroupCtl *rmclient.ResourceGroupsController
)

// SetResourceGroupController set a inited ResourceGroupsController for calibrate usage.
func SetResourceGroupController(rc *rmclient.ResourceGroupsController) {
	resourceGroupCtl = rc
}

// GetResourceGroupController returns the ResourceGroupsController.
func GetResourceGroupController() *rmclient.ResourceGroupsController {
	return resourceGroupCtl
}

// the resource cost rate of a specified workload per 1 tikv cpu.
type baseResourceCost struct {
	// the average tikv cpu time, this is used to calculate whether tikv cpu
	// or tidb cpu is the performance bottle neck.
	tidbCPU float64
	// the kv CPU time for calculate RU, it's smaller than the actual cpu usage.
	kvCPU float64
	// the read bytes rate per 1 tikv cpu.
	readBytes uint64
	// the write bytes rate per 1 tikv cpu.
	writeBytes uint64
	// the average tikv read request count per 1 tikv cpu.
	readReqCount uint64
	// the average tikv write request count per 1 tikv cpu.
	writeReqCount uint64
}

type calibrateResourceExec struct {
	baseExecutor

	workloadType ast.CalibrateResourceType
	done         bool
}

func (e *calibrateResourceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	if !variable.EnableResourceControl.Load() {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	// first fetch the ru settings config.
	if resourceGroupCtl == nil {
		return errors.New("resource group controller is not initialized")
	}

	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	totalKVCPUQuota, err := getTiKVTotalCPUQuota(ctx, exec)
	if err != nil {
		return err
	}
	totalTiDBCPU, err := getTiDBTotalCPUQuota(ctx, exec)
	if err != nil {
		return err
	}

	// The default workload to calculate the RU capacity.
	if e.workloadType == ast.WorkloadNone {
		e.workloadType = ast.TPCC
	}
	baseCost, ok := workloadBaseRUCostMap[e.workloadType]
	if !ok {
		return errors.Errorf("unknown workload '%T'", e.workloadType)
	}

	if totalTiDBCPU/baseCost.tidbCPU < totalKVCPUQuota {
		totalKVCPUQuota = totalTiDBCPU / baseCost.tidbCPU
	}
	ruCfg := resourceGroupCtl.GetConfig()
	ruPerKVCPU := float64(ruCfg.ReadBaseCost)*float64(baseCost.readReqCount) +
		float64(ruCfg.CPUMsCost)*baseCost.kvCPU +
		float64(ruCfg.ReadBytesCost)*float64(baseCost.readBytes) +
		float64(ruCfg.WriteBaseCost)*float64(baseCost.writeReqCount) +
		float64(ruCfg.WriteBytesCost)*float64(baseCost.writeBytes)
	quota := totalKVCPUQuota * ruPerKVCPU
	req.AppendUint64(0, uint64(quota))

	return nil
}

func getTiKVTotalCPUQuota(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) (float64, error) {
	query := "SELECT SUM(value) FROM METRICS_SCHEMA.tikv_cpu_quota GROUP BY time ORDER BY time desc limit 1"
	return getNumberFromMetrics(ctx, exec, query, "tikv_cpu_quota")
}

func getTiDBTotalCPUQuota(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) (float64, error) {
	query := "SELECT SUM(value) FROM METRICS_SCHEMA.tidb_server_maxprocs GROUP BY time ORDER BY time desc limit 1"
	return getNumberFromMetrics(ctx, exec, query, "tidb_server_maxprocs")
}

func getNumberFromMetrics(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, query, metrics string) (float64, error) {
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, query)
	if err != nil {
		return 0.0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0.0, errors.Errorf("metrics '%s' is empty", metrics)
	}

	return rows[0].GetFloat64(0), nil
}
