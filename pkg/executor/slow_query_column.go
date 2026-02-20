// Copyright 2019 PingCAP, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"go.uber.org/zap"
)

type slowQueryColumnValueFactory func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error)

func parseUserOrHostValue(value string) string {
	// the new User&Host format: root[root] @ localhost [127.0.0.1]
	tmp := strings.Split(value, "[")
	return strings.TrimSpace(tmp[0])
}

func getColumnValueFactoryByName(colName string, columnIdx int) (slowQueryColumnValueFactory, error) {
	switch colName {
	case variable.SlowLogTimeStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *slowLogChecker) (bool, error) {
			t, err := ParseTime(value)
			if err != nil {
				return false, err
			}
			timeValue := types.NewTime(types.FromGoTime(t.In(tz)), mysql.TypeTimestamp, types.MaxFsp)
			if checker != nil {
				valid := checker.isTimeValid(timeValue)
				if !valid {
					return valid, nil
				}
			}
			row[columnIdx] = types.NewTimeDatum(timeValue)
			return true, nil
		}, nil
	case variable.SlowLogBackoffDetail:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			backoffDetail := row[columnIdx].GetString()
			if len(backoffDetail) > 0 {
				backoffDetail += " "
			}
			backoffDetail += value
			row[columnIdx] = types.NewStringDatum(backoffDetail)
			return true, nil
		}, nil
	case variable.SlowLogPlan:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			plan := parsePlan(value)
			row[columnIdx] = types.NewStringDatum(plan)
			return true, nil
		}, nil
	case variable.SlowLogBinaryPlan:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			if strings.HasPrefix(value, variable.SlowLogBinaryPlanPrefix) {
				value = value[len(variable.SlowLogBinaryPlanPrefix) : len(value)-len(variable.SlowLogPlanSuffix)]
			}
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	case variable.SlowLogConnIDStr, variable.SlowLogExecRetryCount, variable.SlowLogPreprocSubQueriesStr,
		execdetails.WriteKeysStr, execdetails.WriteSizeStr, execdetails.PrewriteRegionStr, execdetails.TxnRetryStr,
		execdetails.RequestCountStr, execdetails.TotalKeysStr, execdetails.ProcessKeysStr,
		execdetails.RocksdbDeleteSkippedCountStr, execdetails.RocksdbKeySkippedCountStr,
		execdetails.RocksdbBlockCacheHitCountStr, execdetails.RocksdbBlockReadCountStr,
		variable.SlowLogTxnStartTSStr, execdetails.RocksdbBlockReadByteStr:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewUintDatum(v)
			return true, nil
		}, nil
	case variable.SlowLogExecRetryTime, variable.SlowLogQueryTimeStr, variable.SlowLogParseTimeStr,
		variable.SlowLogCompileTimeStr, variable.SlowLogRewriteTimeStr, variable.SlowLogPreProcSubQueryTimeStr,
		variable.SlowLogOptimizeTimeStr, variable.SlowLogWaitTSTimeStr, execdetails.PreWriteTimeStr,
		execdetails.WaitPrewriteBinlogTimeStr, execdetails.CommitTimeStr, execdetails.GetCommitTSTimeStr,
		execdetails.CommitBackoffTimeStr, execdetails.ResolveLockTimeStr, execdetails.LocalLatchWaitTimeStr,
		execdetails.CopTimeStr, execdetails.ProcessTimeStr, execdetails.WaitTimeStr, execdetails.BackoffTimeStr,
		execdetails.LockKeysTimeStr, variable.SlowLogCopProcAvg, variable.SlowLogCopProcP90, variable.SlowLogCopProcMax,
		variable.SlowLogCopWaitAvg, variable.SlowLogCopWaitP90, variable.SlowLogCopWaitMax, variable.SlowLogKVTotal,
		variable.SlowLogPDTotal, variable.SlowLogBackoffTotal, variable.SlowLogWriteSQLRespTotal, variable.SlowLogRRU,
		variable.SlowLogWRU, variable.SlowLogWaitRUDuration, variable.SlowLogTidbCPUUsageDuration, variable.SlowLogTikvCPUUsageDuration, variable.SlowLogMemArbitration:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewFloat64Datum(v)
			return true, nil
		}, nil
	case variable.SlowLogUserStr, variable.SlowLogHostStr, execdetails.BackoffTypesStr, variable.SlowLogDBStr, variable.SlowLogIndexNamesStr, variable.SlowLogDigestStr,
		variable.SlowLogStatsInfoStr, variable.SlowLogCopProcAddr, variable.SlowLogCopWaitAddr, variable.SlowLogPlanDigest,
		variable.SlowLogPrevStmt, variable.SlowLogQuerySQLStr, variable.SlowLogWarnings, variable.SlowLogSessAliasStr,
		variable.SlowLogResourceGroup:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	case variable.SlowLogMemMax, variable.SlowLogDiskMax, variable.SlowLogResultRows, variable.SlowLogUnpackedBytesSentTiKVTotal,
		variable.SlowLogUnpackedBytesReceivedTiKVTotal, variable.SlowLogUnpackedBytesSentTiKVCrossZone, variable.SlowLogUnpackedBytesReceivedTiKVCrossZone,
		variable.SlowLogUnpackedBytesSentTiFlashTotal, variable.SlowLogUnpackedBytesReceivedTiFlashTotal, variable.SlowLogUnpackedBytesSentTiFlashCrossZone,
		variable.SlowLogUnpackedBytesReceivedTiFlashCrossZone:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewIntDatum(v)
			return true, nil
		}, nil
	case variable.SlowLogPrepared, variable.SlowLogSucc, variable.SlowLogPlanFromCache, variable.SlowLogPlanFromBinding,
		variable.SlowLogIsInternalStr, variable.SlowLogIsExplicitTxn, variable.SlowLogIsWriteCacheTable, variable.SlowLogHasMoreResults,
		variable.SlowLogStorageFromKV, variable.SlowLogStorageFromMPP:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseBool(value)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewDatum(v)
			return true, nil
		}, nil
	}
	return nil, nil
}

func getInstanceColumnValueFactory(sctx sessionctx.Context, columnIdx int) (func(row []types.Datum), error) {
	instanceAddr, err := infoschema.GetInstanceAddr(sctx)
	if err != nil {
		return nil, err
	}
	return func(row []types.Datum) {
		row[columnIdx] = types.NewStringDatum(instanceAddr)
	}, nil
}

func parsePlan(planString string) string {
	if len(planString) <= len(variable.SlowLogPlanPrefix)+len(variable.SlowLogPlanSuffix) {
		return planString
	}
	planString = planString[len(variable.SlowLogPlanPrefix) : len(planString)-len(variable.SlowLogPlanSuffix)]
	decodePlanString, err := plancodec.DecodePlan(planString)
	if err == nil {
		planString = decodePlanString
	} else {
		logutil.BgLogger().Error("decode plan in slow log failed", zap.String("plan", planString), zap.Error(err))
	}
	return planString
}

// ParseTime exports for testing.
func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		// This is for compatibility.
		t, err = time.Parse(logutil.OldSlowLogTimeFormat, s)
		if err != nil {
			err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
		}
	}
	return t, err
}
