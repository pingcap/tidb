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
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/duration"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
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

const (
	valuableUsageThreshold = 0.2
	lowUsageThreshold      = 0.1
	percentOfPass          = 0.9
	discardRate            = 0.1

	maxDuration = time.Hour * 24
	minDuration = time.Minute * 10
)

type calibrateResourceExec struct {
	baseExecutor
	optionList   []*ast.DynamicCalibrateResourceOption
	workloadType ast.CalibrateResourceType
	done         bool
}

func (e *calibrateResourceExec) checkDynamicCalibrateOptions() (startTime time.Time, endTime time.Time, err error) {
	var dur time.Duration
	var ts uint64
	for _, op := range e.optionList {
		switch op.Tp {
		case ast.CalibrateStartTime:
			ts, err = staleread.CalculateAsOfTsExpr(e.ctx, e.optionList[0].Ts)
			if err != nil {
				return
			}
			startTime = oracle.GetTimeFromTS(ts)
		case ast.CalibrateEndTime:
			ts, err = staleread.CalculateAsOfTsExpr(e.ctx, e.optionList[1].Ts)
			if err != nil {
				return
			}
			endTime = oracle.GetTimeFromTS(ts)
		case ast.CalibrateDuration:
			dur, err = duration.ParseDuration(e.optionList[1].StrValue)
			if err != nil {
				return
			}
		}
	}
	if startTime.IsZero() {
		err = errors.Errorf("start time should not be 0")
		return
	}
	// If endTime is set, duration will be ignored.
	if endTime.IsZero() {
		if dur != time.Duration(0) {
			endTime = startTime.Add(dur)
		} else {
			endTime = time.Now()
		}
	}
	// check the duration
	dura := endTime.Sub(startTime)
	if dura > maxDuration {
		err = errors.Errorf("the duration of calibration is too long, should be less than %s", maxDuration.String())
		return
	}
	if dura < minDuration {
		err = errors.Errorf("the duration of calibration is too short, should be greater than %s", minDuration.String())
	}

	return
}

func (e *calibrateResourceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	if len(e.optionList) > 0 && e.workloadType != ast.WorkloadNone {
		return errors.Errorf("dynamic and static calibration cannot be performed at the same time")
	}
	if len(e.optionList) > 0 {
		return e.dynamicCalibrate(ctx, req, exec)
	}
	return e.staticCalibrate(ctx, req, exec)
}

func (e *calibrateResourceExec) dynamicCalibrate(ctx context.Context, req *chunk.Chunk, exec sqlexec.RestrictedSQLExecutor) error {
	startTs, endTs, err := e.checkDynamicCalibrateOptions()
	startTime := startTs.In(e.ctx.GetSessionVars().Location()).Format("2006-01-02 15:04:05")
	endTime := endTs.In(e.ctx.GetSessionVars().Location()).Format("2006-01-02 15:04:05")
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
	rus, err := getRUPerSec(ctx, exec, startTime, endTime)
	if err != nil {
		return err
	}
	tikvCPUs, err := getComponentCPUUsagePerSec(ctx, exec, "tikv", startTime, endTime)
	if err != nil {
		return err
	}
	tidbCPUs, err := getComponentCPUUsagePerSec(ctx, exec, "tidb", startTime, endTime)
	if err != nil {
		return err
	}
	quotas := make([]float64, 0)
	lowCount := 0
	tidbCPULowCount := 0
	tikvCPULowCOunt := 0
	for idx, ru := range rus {
		if idx >= len(tikvCPUs) || idx >= len(tidbCPUs) {
			break
		}
		tikvQuota := tikvCPUs[idx] / totalKVCPUQuota
		tidbQuota := tidbCPUs[idx] / totalTiDBCPU
		// If one of the two cpu usage is greater than the `valuableUsageThreshold`, we can accept it.
		// And if both are greater than the `lowUsageThreshold`, we can also accpet it.
		if tikvQuota > valuableUsageThreshold || tidbQuota > valuableUsageThreshold {
			quotas = append(quotas, ru/mathutil.Max(tikvQuota, tidbQuota))
		} else if tikvQuota < lowUsageThreshold {
			lowCount++
			tikvCPULowCOunt++
			if tidbQuota < lowUsageThreshold {
				tidbCPULowCount++
			}
		} else if tidbQuota < lowUsageThreshold {
			lowCount++
			tidbCPULowCount++
		} else {
			quotas = append(quotas, ru/mathutil.Max(tikvQuota, tidbQuota))
		}
	}
	if len(quotas) < 5 {
		return errors.Errorf("there are too few metrics points available")
	}
	if float64(len(quotas))/float64(len(quotas)+lowCount) > percentOfPass {
		sort.Slice(quotas, func(i, j int) bool {
			return quotas[i] > quotas[j]
		})
		lowerBound := int(math.Round(float64(len(quotas)) * float64(discardRate)))
		upperBound := len(quotas) - lowerBound
		sum := 0.
		for i := lowerBound; i < upperBound; i++ {
			sum += quotas[i]
		}
		quota := sum / float64(upperBound-lowerBound)
		req.AppendUint64(0, uint64(quota))
	} else {
		if tidbCPULowCount > 0 && tikvCPULowCOunt > 0 {
			return errors.Errorf("The CPU utilizations of TiDB and TiKV are less than one tenth in some of the time")
		} else if tidbCPULowCount > 0 {
			return errors.Errorf("The CPU utilization of TiDB is less than one tenth in some of the time")
		} else {
			return errors.Errorf("The CPU utilization of TiKV is less than one tenth in some of the time")
		}
	}
	return nil
}

func (e *calibrateResourceExec) staticCalibrate(ctx context.Context, req *chunk.Chunk, exec sqlexec.RestrictedSQLExecutor) error {
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

func getRUPerSec(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, startTime, endTime string) ([]float64, error) {
	query := fmt.Sprintf("SELECT value FROM METRICS_SCHEMA.resource_manager_resource_unit where time >= '%s' and time <= '%s' ORDER BY time desc", startTime, endTime)
	return getValuesFromMetrics(ctx, exec, query, "resource_manager_resource_unit")
}

func getComponentCPUUsagePerSec(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, component, startTime, endTime string) ([]float64, error) {
	query := fmt.Sprintf("SELECT sum(value) FROM METRICS_SCHEMA.process_cpu_usage where time >= '%s' and time <= '%s' and job like '%%%s' GROUP BY time ORDER BY time desc", startTime, endTime, component)
	return getValuesFromMetrics(ctx, exec, query, "process_cpu_usage")
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

func getValuesFromMetrics(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, query, metrics string) ([]float64, error) {
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, errors.Errorf("metrics '%s' is empty", metrics)
	}
	ret := make([]float64, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.GetFloat64(0))
	}
	return ret, nil
}
