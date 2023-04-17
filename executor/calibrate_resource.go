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
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// workloadBaseRUCostMap contains the base resource cost rate per 1 kv cpu within 1 second,
// the data is calculated from benchmark result, these data might not be very accurate,
// but is enough here because the maximum RU capacity is depended on both the cluster and
// the workload.
var workloadBaseRUCostMap = map[ast.CalibrateResourceType]*baseResourceCost{
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

// the resource cost rate of a specified workload per 1 tikv cpu
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

	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	// first fetch the ru settings config.
	ruCfg, err := getRUSettings(ctx, exec)
	if err != nil {
		return err
	}

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
	ruPerKVCPU := ruCfg.readBaseCost*float64(baseCost.readReqCount) +
		ruCfg.readCostCPU*baseCost.kvCPU +
		ruCfg.readCostPerByte*float64(baseCost.readBytes) +
		ruCfg.writeBaseCost*float64(baseCost.writeReqCount) +
		ruCfg.writeCostPerByte*float64(baseCost.writeBytes)
	quota := totalKVCPUQuota * ruPerKVCPU
	req.AppendUint64(0, uint64(quota))

	return nil
}

type ruConfig struct {
	readBaseCost     float64
	writeBaseCost    float64
	readCostCPU      float64
	readCostPerByte  float64
	writeCostPerByte float64
}

func getRUSettings(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) (*ruConfig, error) {
	rows, fields, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, "SHOW CONFIG WHERE TYPE = 'pd' AND name like 'controller.request-unit.%'")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, errors.New("PD request-unit config not found")
	}
	var nameIdx, valueIdx int
	for i, f := range fields {
		switch f.ColumnAsName.L {
		case "name":
			nameIdx = i
		case "value":
			valueIdx = i
		}
	}

	cfg := &ruConfig{}
	for _, row := range rows {
		val, err := strconv.ParseFloat(row.GetString(valueIdx), 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		name, _ := strings.CutPrefix(row.GetString(nameIdx), "controller.request-unit.")

		switch name {
		case "read-base-cost":
			cfg.readBaseCost = val
		case "read-cost-per-byte":
			cfg.readCostPerByte = val
		case "read-cpu-ms-cost":
			cfg.readCostCPU = val
		case "write-base-cost":
			cfg.writeBaseCost = val
		case "write-cost-per-byte":
			cfg.writeCostPerByte = val
		}
	}

	return cfg, nil
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
