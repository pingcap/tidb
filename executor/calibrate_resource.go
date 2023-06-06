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
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/duration"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

var (
	// workloadBaseRUCostMap contains the base resource cost rate per 1 kv cpu within 1 second,
	// the data is calculated from benchmark result, these data might not be very accurate,
	// but is enough here because the maximum RU capacity is depended on both the cluster and
	// the workload.
	workloadBaseRUCostMap = map[ast.CalibrateResourceType]*baseResourceCost{
		ast.TPCC: {
			tidbToKVCPURatio: 0.6,
			kvCPU:            0.15,
			readBytes:        units.MiB / 2,
			writeBytes:       units.MiB,
			readReqCount:     300,
			writeReqCount:    1750,
		},
		ast.OLTPREADWRITE: {
			tidbToKVCPURatio: 1.25,
			kvCPU:            0.35,
			readBytes:        units.MiB * 4.25,
			writeBytes:       units.MiB / 3,
			readReqCount:     1600,
			writeReqCount:    1400,
		},
		ast.OLTPREADONLY: {
			tidbToKVCPURatio: 2,
			kvCPU:            0.52,
			readBytes:        units.MiB * 28,
			writeBytes:       0,
			readReqCount:     4500,
			writeReqCount:    0,
		},
		ast.OLTPWRITEONLY: {
			tidbToKVCPURatio: 1,
			kvCPU:            0,
			readBytes:        0,
			writeBytes:       units.MiB,
			readReqCount:     0,
			writeReqCount:    3550,
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
	// represents the average ratio of TiDB CPU time to TiKV CPU time, this is used to calculate whether tikv cpu
	// or tidb cpu is the performance bottle neck.
	tidbToKVCPURatio float64
	// the kv CPU time for calculate RU, it's smaller than the actual cpu usage. The unit is seconds.
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
	// valuableUsageThreshold is the threshold used to determine whether the CPU is high enough.
	// The sampling point is available when the CPU utilization of tikv or tidb is higher than the valuableUsageThreshold.
	valuableUsageThreshold = 0.2
	// lowUsageThreshold is the threshold used to determine whether the CPU is too low.
	// When the CPU utilization of tikv or tidb is lower than lowUsageThreshold, but neither is higher than valuableUsageThreshold, the sampling point is unavailable
	lowUsageThreshold = 0.1
	// calibration is performed only when the available time point exceeds the percentOfPass
	percentOfPass = 0.9
	// For quotas computed at each point in time, the maximum and minimum portions are discarded, and discardRate is the percentage discarded
	discardRate = 0.1

	// duration Indicates the supported calibration duration
	maxDuration = time.Hour * 24
	minDuration = time.Minute * 10
)

type calibrateResourceExec struct {
	baseExecutor
	optionList   []*ast.DynamicCalibrateResourceOption
	workloadType ast.CalibrateResourceType
	done         bool
}

func (e *calibrateResourceExec) parseCalibrateDuration(ctx context.Context) (startTime time.Time, endTime time.Time, err error) {
	var dur time.Duration
	var ts uint64
	for _, op := range e.optionList {
		switch op.Tp {
		case ast.CalibrateStartTime:
			ts, err = staleread.CalculateAsOfTsExpr(ctx, e.ctx, op.Ts)
			if err != nil {
				return
			}
			startTime = oracle.GetTimeFromTS(ts)
		case ast.CalibrateEndTime:
			ts, err = staleread.CalculateAsOfTsExpr(ctx, e.ctx, op.Ts)
			if err != nil {
				return
			}
			endTime = oracle.GetTimeFromTS(ts)
		case ast.CalibrateDuration:
			dur, err = duration.ParseDuration(op.StrValue)
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
	dur = endTime.Sub(startTime)
	if dur > maxDuration {
		err = errors.Errorf("the duration of calibration is too long, which could lead to inaccurate output. Please make the duration between %s and %s", minDuration.String(), maxDuration.String())
		return
	}
	if dur < minDuration {
		err = errors.Errorf("the duration of calibration is too short, which could lead to inaccurate output. Please make the duration between %s and %s", minDuration.String(), maxDuration.String())
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
	if len(e.optionList) > 0 {
		return e.dynamicCalibrate(ctx, req, exec)
	}
	return e.staticCalibrate(ctx, req, exec)
}

var (
	errLowUsage          = errors.Errorf("The workload in selected time window is too low, with which TiDB is unable to reach a capacity estimation; please select another time window with higher workload, or calibrate resource by hardware instead")
	errNoCPUQuotaMetrics = errors.Normalize("There is no CPU quota metrics, %v")
)

func (e *calibrateResourceExec) dynamicCalibrate(ctx context.Context, req *chunk.Chunk, exec sqlexec.RestrictedSQLExecutor) error {
	startTs, endTs, err := e.parseCalibrateDuration(ctx)
	if err != nil {
		return err
	}
	startTime := startTs.In(e.ctx.GetSessionVars().Location()).Format(time.DateTime)
	endTime := endTs.In(e.ctx.GetSessionVars().Location()).Format(time.DateTime)

	totalKVCPUQuota, err := getTiKVTotalCPUQuota(ctx, exec)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	totalTiDBCPU, err := getTiDBTotalCPUQuota(ctx, exec)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	rus, err := getRUPerSec(ctx, e.ctx, exec, startTime, endTime)
	if err != nil {
		return err
	}
	tikvCPUs, err := getComponentCPUUsagePerSec(ctx, e.ctx, exec, "tikv", startTime, endTime)
	if err != nil {
		return err
	}
	tidbCPUs, err := getComponentCPUUsagePerSec(ctx, e.ctx, exec, "tidb", startTime, endTime)
	if err != nil {
		return err
	}
	quotas := make([]float64, 0)
	lowCount := 0
	for {
		if rus.isEnd() || tikvCPUs.isEnd() || tidbCPUs.isEnd() {
			break
		}
		// make time point match
		maxTime := rus.getTime()
		if tikvCPUs.getTime().After(maxTime) {
			maxTime = tikvCPUs.getTime()
		}
		if tidbCPUs.getTime().After(maxTime) {
			maxTime = tidbCPUs.getTime()
		}
		if !rus.advance(maxTime) || !tikvCPUs.advance(maxTime) || !tidbCPUs.advance(maxTime) {
			continue
		}
		tikvQuota, tidbQuota := tikvCPUs.getValue()/totalKVCPUQuota, tidbCPUs.getValue()/totalTiDBCPU
		// If one of the two cpu usage is greater than the `valuableUsageThreshold`, we can accept it.
		// And if both are greater than the `lowUsageThreshold`, we can also accept it.
		if tikvQuota > valuableUsageThreshold || tidbQuota > valuableUsageThreshold {
			quotas = append(quotas, rus.getValue()/mathutil.Max(tikvQuota, tidbQuota))
		} else if tikvQuota < lowUsageThreshold || tidbQuota < lowUsageThreshold {
			lowCount++
		} else {
			quotas = append(quotas, rus.getValue()/mathutil.Max(tikvQuota, tidbQuota))
		}
		rus.next()
		tidbCPUs.next()
		tikvCPUs.next()
	}
	if len(quotas) < 5 {
		return errLowUsage
	}
	if float64(len(quotas))/float64(len(quotas)+lowCount) > percentOfPass {
		sort.Slice(quotas, func(i, j int) bool {
			return quotas[i] > quotas[j]
		})
		lowerBound := int(math.Round(float64(len(quotas)) * discardRate))
		upperBound := len(quotas) - lowerBound
		sum := 0.
		for i := lowerBound; i < upperBound; i++ {
			sum += quotas[i]
		}
		quota := sum / float64(upperBound-lowerBound)
		req.AppendUint64(0, uint64(quota))
	} else {
		return errLowUsage
	}
	return nil
}

func (e *calibrateResourceExec) staticCalibrate(ctx context.Context, req *chunk.Chunk, exec sqlexec.RestrictedSQLExecutor) error {
	if !variable.EnableResourceControl.Load() {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	// first fetch the ru settings config.
	if resourceGroupCtl == nil {
		return errors.New("resource group controller is not initialized")
	}

	totalKVCPUQuota, err := getTiKVTotalCPUQuota(ctx, exec)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	totalTiDBCPU, err := getTiDBTotalCPUQuota(ctx, exec)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}

	// The default workload to calculate the RU capacity.
	if e.workloadType == ast.WorkloadNone {
		e.workloadType = ast.TPCC
	}
	baseCost, ok := workloadBaseRUCostMap[e.workloadType]
	if !ok {
		return errors.Errorf("unknown workload '%T'", e.workloadType)
	}

	if totalTiDBCPU/baseCost.tidbToKVCPURatio < totalKVCPUQuota {
		totalKVCPUQuota = totalTiDBCPU / baseCost.tidbToKVCPURatio
	}
	ruCfg := resourceGroupCtl.GetConfig()
	ruPerKVCPU := float64(ruCfg.ReadBaseCost)*float64(baseCost.readReqCount) +
		float64(ruCfg.CPUMsCost)*baseCost.kvCPU*1000 + // convert to ms
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

type timePointValue struct {
	tp  time.Time
	val float64
}

type timeSeriesValues struct {
	idx  int
	vals []*timePointValue
}

func (t *timeSeriesValues) isEnd() bool {
	return t.idx >= len(t.vals)
}

func (t *timeSeriesValues) next() {
	t.idx++
}

func (t *timeSeriesValues) getTime() time.Time {
	return t.vals[t.idx].tp
}

func (t *timeSeriesValues) getValue() float64 {
	return t.vals[t.idx].val
}

func (t *timeSeriesValues) advance(target time.Time) bool {
	for ; t.idx < len(t.vals); t.idx++ {
		// `target` is maximal time in other timeSeriesValues,
		// so we should find the time which offset is less than 10s.
		if t.vals[t.idx].tp.Add(time.Second * 10).After(target) {
			return t.vals[t.idx].tp.Add(-time.Second * 10).Before(target)
		}
	}
	return false
}

func getRUPerSec(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, startTime, endTime string) (*timeSeriesValues, error) {
	query := fmt.Sprintf("SELECT time, value FROM METRICS_SCHEMA.resource_manager_resource_unit where time >= '%s' and time <= '%s' ORDER BY time asc", startTime, endTime)
	return getValuesFromMetrics(ctx, sctx, exec, query, "resource_manager_resource_unit")
}

func getComponentCPUUsagePerSec(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, component, startTime, endTime string) (*timeSeriesValues, error) {
	query := fmt.Sprintf("SELECT time, sum(value) FROM METRICS_SCHEMA.process_cpu_usage where time >= '%s' and time <= '%s' and job like '%%%s' GROUP BY time ORDER BY time asc", startTime, endTime, component)
	return getValuesFromMetrics(ctx, sctx, exec, query, "process_cpu_usage")
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

func getValuesFromMetrics(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, query, metrics string) (*timeSeriesValues, error) {
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make([]*timePointValue, 0, len(rows))
	for _, row := range rows {
		if tp, err := row.GetTime(0).AdjustedGoTime(sctx.GetSessionVars().Location()); err == nil {
			ret = append(ret, &timePointValue{
				tp:  tp,
				val: row.GetFloat64(1),
			})
		}
	}
	return &timeSeriesValues{idx: 0, vals: ret}, nil
}
