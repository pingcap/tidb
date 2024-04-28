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

package calibrateresource

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/duration"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
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
)

const (
	// serverTypeTiDB is tidb's instance type name
	serverTypeTiDB = "tidb"
	// serverTypeTiKV is tikv's instance type name
	serverTypeTiKV = "tikv"
	// serverTypeTiFlash is tiflash's instance type name
	serverTypeTiFlash = "tiflash"
)

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
	// For quotas computed at each point in time, the maximum and minimum portions are discarded, and discardRate is the percentage discarded
	discardRate = 0.1

	// duration Indicates the supported calibration duration
	maxDuration = time.Hour * 24
	minDuration = time.Minute
)

// Executor is used as executor of calibrate resource.
type Executor struct {
	OptionList []*ast.DynamicCalibrateResourceOption
	exec.BaseExecutor
	WorkloadType ast.CalibrateResourceType
	done         bool
}

func (e *Executor) parseTsExpr(ctx context.Context, tsExpr ast.ExprNode) (time.Time, error) {
	ts, err := staleread.CalculateAsOfTsExpr(ctx, e.Ctx().GetPlanCtx(), tsExpr)
	if err != nil {
		return time.Time{}, err
	}
	return oracle.GetTimeFromTS(ts), nil
}

func (e *Executor) parseCalibrateDuration(ctx context.Context) (startTime time.Time, endTime time.Time, err error) {
	var dur time.Duration
	// startTimeExpr and endTimeExpr are used to calc endTime by FuncCallExpr when duration begin with `interval`.
	var startTimeExpr ast.ExprNode
	var endTimeExpr ast.ExprNode
	for _, op := range e.OptionList {
		switch op.Tp {
		case ast.CalibrateStartTime:
			startTimeExpr = op.Ts
			startTime, err = e.parseTsExpr(ctx, startTimeExpr)
			if err != nil {
				return
			}
		case ast.CalibrateEndTime:
			endTimeExpr = op.Ts
			endTime, err = e.parseTsExpr(ctx, op.Ts)
			if err != nil {
				return
			}
		}
	}
	for _, op := range e.OptionList {
		if op.Tp != ast.CalibrateDuration {
			continue
		}
		// string duration
		if len(op.StrValue) > 0 {
			dur, err = duration.ParseDuration(op.StrValue)
			if err != nil {
				return
			}
			// If startTime is not set, startTime will be now() - duration.
			if startTime.IsZero() {
				toTime := endTime
				if toTime.IsZero() {
					toTime = time.Now()
				}
				startTime = toTime.Add(-dur)
			}
			// If endTime is set, duration will be ignored.
			if endTime.IsZero() {
				endTime = startTime.Add(dur)
			}
			continue
		}
		// interval duration
		// If startTime is not set, startTime will be now() - duration.
		if startTimeExpr == nil {
			toTimeExpr := endTimeExpr
			if endTime.IsZero() {
				toTimeExpr = &ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP")}
			}
			startTimeExpr = &ast.FuncCallExpr{
				FnName: model.NewCIStr("DATE_SUB"),
				Args: []ast.ExprNode{
					toTimeExpr,
					op.Ts,
					&ast.TimeUnitExpr{Unit: op.Unit}},
			}
			startTime, err = e.parseTsExpr(ctx, startTimeExpr)
			if err != nil {
				return
			}
		}
		// If endTime is set, duration will be ignored.
		if endTime.IsZero() {
			endTime, err = e.parseTsExpr(ctx, &ast.FuncCallExpr{
				FnName: model.NewCIStr("DATE_ADD"),
				Args: []ast.ExprNode{startTimeExpr,
					op.Ts,
					&ast.TimeUnitExpr{Unit: op.Unit}},
			})
			if err != nil {
				return
			}
		}
	}

	if startTime.IsZero() {
		err = errors.Errorf("start time should not be 0")
		return
	}
	if endTime.IsZero() {
		endTime = time.Now()
	}
	// check the duration
	dur = endTime.Sub(startTime)
	// add the buffer duration
	if dur > maxDuration+time.Minute {
		err = errors.Errorf("the duration of calibration is too long, which could lead to inaccurate output. Please make the duration between %s and %s", minDuration.String(), maxDuration.String())
		return
	}
	// We only need to consider the case where the duration is slightly enlarged.
	if dur < minDuration {
		err = errors.Errorf("the duration of calibration is too short, which could lead to inaccurate output. Please make the duration between %s and %s", minDuration.String(), maxDuration.String())
	}
	return
}

// Next implements the interface of Executor.
func (e *Executor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	if !variable.EnableResourceControl.Load() {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	if len(e.OptionList) > 0 {
		return e.dynamicCalibrate(ctx, req)
	}
	return e.staticCalibrate(req)
}

var (
	errLowUsage          = errors.Errorf("The workload in selected time window is too low, with which TiDB is unable to reach a capacity estimation; please select another time window with higher workload, or calibrate resource by hardware instead")
	errNoCPUQuotaMetrics = errors.Normalize("There is no CPU quota metrics, %v")
)

func (e *Executor) dynamicCalibrate(ctx context.Context, req *chunk.Chunk) error {
	exec := e.Ctx().GetRestrictedSQLExecutor()
	startTs, endTs, err := e.parseCalibrateDuration(ctx)
	if err != nil {
		return err
	}
	clusterInfo, err := infoschema.GetClusterServerInfo(e.Ctx())
	if err != nil {
		return err
	}
	tidbQuota, err1 := e.getTiDBQuota(ctx, exec, clusterInfo, startTs, endTs)
	tiflashQuota, err2 := e.getTiFlashQuota(ctx, exec, clusterInfo, startTs, endTs)
	if err1 != nil && err2 != nil {
		return err1
	}

	req.AppendUint64(0, uint64(tidbQuota+tiflashQuota))
	return nil
}

func (e *Executor) getTiDBQuota(
	ctx context.Context,
	exec sqlexec.RestrictedSQLExecutor,
	serverInfos []infoschema.ServerInfo,
	startTs, endTs time.Time,
) (float64, error) {
	startTime := startTs.In(e.Ctx().GetSessionVars().Location()).Format(time.DateTime)
	endTime := endTs.In(e.Ctx().GetSessionVars().Location()).Format(time.DateTime)

	totalKVCPUQuota, err := getTiKVTotalCPUQuota(serverInfos)
	if err != nil {
		return 0, errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	totalTiDBCPU, err := getTiDBTotalCPUQuota(serverInfos)
	if err != nil {
		return 0, errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	rus, err := getRUPerSec(ctx, e.Ctx(), exec, startTime, endTime)
	if err != nil {
		return 0, err
	}
	tikvCPUs, err := getComponentCPUUsagePerSec(ctx, e.Ctx(), exec, "tikv", startTime, endTime)
	if err != nil {
		return 0, err
	}
	tidbCPUs, err := getComponentCPUUsagePerSec(ctx, e.Ctx(), exec, "tidb", startTime, endTime)
	if err != nil {
		return 0, err
	}

	failpoint.Inject("mockMetricsDataFilter", func() {
		ret := make([]*timePointValue, 0)
		for _, point := range tikvCPUs.vals {
			if point.tp.After(endTs) || point.tp.Before(startTs) {
				continue
			}
			ret = append(ret, point)
		}
		tikvCPUs.vals = ret
		ret = make([]*timePointValue, 0)
		for _, point := range tidbCPUs.vals {
			if point.tp.After(endTs) || point.tp.Before(startTs) {
				continue
			}
			ret = append(ret, point)
		}
		tidbCPUs.vals = ret
		ret = make([]*timePointValue, 0)
		for _, point := range rus.vals {
			if point.tp.After(endTs) || point.tp.Before(startTs) {
				continue
			}
			ret = append(ret, point)
		}
		rus.vals = ret
	})
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
			quotas = append(quotas, rus.getValue()/max(tikvQuota, tidbQuota))
		} else if tikvQuota < lowUsageThreshold || tidbQuota < lowUsageThreshold {
			lowCount++
		} else {
			quotas = append(quotas, rus.getValue()/max(tikvQuota, tidbQuota))
		}
		rus.next()
		tidbCPUs.next()
		tikvCPUs.next()
	}
	quota, err := setupQuotas(quotas)
	if err != nil {
		return 0, err
	}
	return quota, nil
}

func setupQuotas(quotas []float64) (float64, error) {
	if len(quotas) < 2 {
		return 0, errLowUsage
	}
	sort.Slice(quotas, func(i, j int) bool {
		return quotas[i] > quotas[j]
	})
	lowerBound := int(math.Round(float64(len(quotas)) * discardRate))
	upperBound := len(quotas) - lowerBound
	sum := 0.
	for i := lowerBound; i < upperBound; i++ {
		sum += quotas[i]
	}
	return sum / float64(upperBound-lowerBound), nil
}

func (e *Executor) getTiFlashQuota(
	ctx context.Context,
	exec sqlexec.RestrictedSQLExecutor,
	serverInfos []infoschema.ServerInfo,
	startTs, endTs time.Time,
) (float64, error) {
	startTime := startTs.In(e.Ctx().GetSessionVars().Location()).Format(time.DateTime)
	endTime := endTs.In(e.Ctx().GetSessionVars().Location()).Format(time.DateTime)

	quotas := make([]float64, 0)
	totalTiFlashLogicalCores, err := getTiFlashLogicalCores(serverInfos)
	if err != nil {
		return 0, errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	tiflashCPUs, err := getTiFlashCPUUsagePerSec(ctx, e.Ctx(), exec, startTime, endTime)
	if err != nil {
		return 0, err
	}
	tiflashRUs, err := getTiFlashRUPerSec(ctx, e.Ctx(), exec, startTime, endTime)
	if err != nil {
		return 0, err
	}
	for {
		if tiflashRUs.isEnd() || tiflashCPUs.isEnd() {
			break
		}
		// make time point match
		maxTime := tiflashRUs.getTime()
		if tiflashCPUs.getTime().After(maxTime) {
			maxTime = tiflashCPUs.getTime()
		}
		if !tiflashRUs.advance(maxTime) || !tiflashCPUs.advance(maxTime) {
			continue
		}
		tiflashQuota := tiflashCPUs.getValue() / totalTiFlashLogicalCores
		if tiflashQuota > lowUsageThreshold {
			quotas = append(quotas, tiflashRUs.getValue()/tiflashQuota)
		}
		tiflashRUs.next()
		tiflashCPUs.next()
	}
	return setupQuotas(quotas)
}

func (e *Executor) staticCalibrate(req *chunk.Chunk) error {
	resourceGroupCtl := domain.GetDomain(e.Ctx()).ResourceGroupsController()
	// first fetch the ru settings config.
	if resourceGroupCtl == nil {
		return errors.New("resource group controller is not initialized")
	}
	clusterInfo, err := infoschema.GetClusterServerInfo(e.Ctx())
	if err != nil {
		return err
	}
	ruCfg := resourceGroupCtl.GetConfig()
	if e.WorkloadType == ast.TPCH10 {
		return staticCalibrateTpch10(req, clusterInfo, ruCfg)
	}

	totalKVCPUQuota, err := getTiKVTotalCPUQuota(clusterInfo)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}
	totalTiDBCPUQuota, err := getTiDBTotalCPUQuota(clusterInfo)
	if err != nil {
		return errNoCPUQuotaMetrics.FastGenByArgs(err.Error())
	}

	// The default workload to calculate the RU capacity.
	if e.WorkloadType == ast.WorkloadNone {
		e.WorkloadType = ast.TPCC
	}
	baseCost, ok := workloadBaseRUCostMap[e.WorkloadType]
	if !ok {
		return errors.Errorf("unknown workload '%T'", e.WorkloadType)
	}

	if totalTiDBCPUQuota/baseCost.tidbToKVCPURatio < totalKVCPUQuota {
		totalKVCPUQuota = totalTiDBCPUQuota / baseCost.tidbToKVCPURatio
	}
	ruPerKVCPU := float64(ruCfg.ReadBaseCost)*float64(baseCost.readReqCount) +
		float64(ruCfg.CPUMsCost)*baseCost.kvCPU*1000 + // convert to ms
		float64(ruCfg.ReadBytesCost)*float64(baseCost.readBytes) +
		float64(ruCfg.WriteBaseCost)*float64(baseCost.writeReqCount) +
		float64(ruCfg.WriteBytesCost)*float64(baseCost.writeBytes)
	quota := totalKVCPUQuota * ruPerKVCPU
	req.AppendUint64(0, uint64(quota))
	return nil
}

func staticCalibrateTpch10(req *chunk.Chunk, clusterInfo []infoschema.ServerInfo, ruCfg *resourceControlClient.RUConfig) error {
	// TPCH10 only considers the resource usage of the TiFlash including cpu and read bytes. Others are ignored.
	// cpu usage: 105494.666484 / 20 / 20 = 263.74
	// read bytes: 401799161689.0 / 20 / 20 = 1004497904.22
	const cpuTimePerCPUPerSec float64 = 263.74
	const readBytesPerCPUPerSec float64 = 1004497904.22
	ruPerCPU := float64(ruCfg.CPUMsCost)*cpuTimePerCPUPerSec + float64(ruCfg.ReadBytesCost)*readBytesPerCPUPerSec
	totalTiFlashLogicalCores, err := getTiFlashLogicalCores(clusterInfo)
	if err != nil {
		return err
	}
	quota := totalTiFlashLogicalCores * ruPerCPU
	req.AppendUint64(0, uint64(quota))
	return nil
}

func getTiDBTotalCPUQuota(clusterInfo []infoschema.ServerInfo) (float64, error) {
	cpuQuota := float64(runtime.GOMAXPROCS(0))
	failpoint.Inject("mockGOMAXPROCS", func(val failpoint.Value) {
		if val != nil {
			cpuQuota = float64(val.(int))
		}
	})
	instanceNum := count(clusterInfo, serverTypeTiDB)
	return cpuQuota * float64(instanceNum), nil
}

func getTiKVTotalCPUQuota(clusterInfo []infoschema.ServerInfo) (float64, error) {
	instanceNum := count(clusterInfo, serverTypeTiKV)
	if instanceNum == 0 {
		return 0.0, errors.New("no server with type 'tikv' is found")
	}
	cpuQuota, err := fetchServerCPUQuota(clusterInfo, serverTypeTiKV, "tikv_server_cpu_cores_quota")
	if err != nil {
		return 0.0, err
	}
	return cpuQuota * float64(instanceNum), nil
}

func getTiFlashLogicalCores(clusterInfo []infoschema.ServerInfo) (float64, error) {
	instanceNum := count(clusterInfo, serverTypeTiFlash)
	if instanceNum == 0 {
		return 0.0, nil
	}
	cpuQuota, err := fetchServerCPUQuota(clusterInfo, serverTypeTiFlash, "tiflash_proxy_tikv_server_cpu_cores_quota")
	if err != nil {
		return 0.0, err
	}
	return cpuQuota * float64(instanceNum), nil
}

func getTiFlashRUPerSec(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, startTime, endTime string) (*timeSeriesValues, error) {
	query := fmt.Sprintf("SELECT time, value FROM METRICS_SCHEMA.tiflash_resource_manager_resource_unit where time >= '%s' and time <= '%s' ORDER BY time asc", startTime, endTime)
	return getValuesFromMetrics(ctx, sctx, exec, query)
}

func getTiFlashCPUUsagePerSec(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, startTime, endTime string) (*timeSeriesValues, error) {
	query := fmt.Sprintf("SELECT time, sum(value) FROM METRICS_SCHEMA.tiflash_process_cpu_usage where time >= '%s' and time <= '%s' and job = 'tiflash' GROUP BY time ORDER BY time asc", startTime, endTime)
	return getValuesFromMetrics(ctx, sctx, exec, query)
}

type timePointValue struct {
	tp  time.Time
	val float64
}

type timeSeriesValues struct {
	vals []*timePointValue
	idx  int
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
	return getValuesFromMetrics(ctx, sctx, exec, query)
}

func getComponentCPUUsagePerSec(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, component, startTime, endTime string) (*timeSeriesValues, error) {
	query := fmt.Sprintf("SELECT time, sum(value) FROM METRICS_SCHEMA.process_cpu_usage where time >= '%s' and time <= '%s' and job like '%%%s' GROUP BY time ORDER BY time asc", startTime, endTime, component)
	return getValuesFromMetrics(ctx, sctx, exec, query)
}

func getValuesFromMetrics(ctx context.Context, sctx sessionctx.Context, exec sqlexec.RestrictedSQLExecutor, query string) (*timeSeriesValues, error) {
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

func count(clusterInfo []infoschema.ServerInfo, ty string) int {
	num := 0
	for _, e := range clusterInfo {
		if e.ServerType == ty {
			num++
		}
	}
	return num
}

func fetchServerCPUQuota(serverInfos []infoschema.ServerInfo, serverType string, metricName string) (float64, error) {
	var cpuQuota float64
	err := fetchStoreMetrics(serverInfos, serverType, func(addr string, resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("request %s failed: %s", addr, resp.Status)
		}
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, metricName) {
				continue
			}
			// the metrics format is like following:
			// tikv_server_cpu_cores_quota 8
			quota, err := strconv.ParseFloat(line[len(metricName)+1:], 64)
			if err == nil {
				cpuQuota = quota
			}
			return errors.Trace(err)
		}
		return errors.Errorf("metrics '%s' not found from server '%s'", metricName, addr)
	})
	return cpuQuota, err
}

func fetchStoreMetrics(serversInfo []infoschema.ServerInfo, serverType string, onResp func(string, *http.Response) error) error {
	var firstErr error
	for _, srv := range serversInfo {
		if srv.ServerType != serverType {
			continue
		}
		if len(srv.StatusAddr) == 0 {
			continue
		}
		url := fmt.Sprintf("%s://%s/metrics", util.InternalHTTPSchema(), srv.StatusAddr)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		var resp *http.Response
		failpoint.Inject("mockMetricsResponse", func(val failpoint.Value) {
			if val != nil {
				data, _ := base64.StdEncoding.DecodeString(val.(string))
				resp = &http.Response{
					StatusCode: http.StatusOK,
					Body: noopCloserWrapper{
						Reader: strings.NewReader(string(data)),
					},
				}
			}
		})
		if resp == nil {
			var err1 error
			// ignore false positive go line, can't use defer here because it's in a loop.
			//nolint:bodyclose
			resp, err1 = util.InternalHTTPClient().Do(req)
			if err1 != nil {
				if firstErr == nil {
					firstErr = err1
				}
				continue
			}
		}
		err = onResp(srv.Address, resp)
		resp.Body.Close()
		return err
	}
	if firstErr == nil {
		firstErr = errors.Errorf("no server with type '%s' is found", serverType)
	}
	return firstErr
}

type noopCloserWrapper struct {
	io.Reader
}

func (noopCloserWrapper) Close() error {
	return nil
}
