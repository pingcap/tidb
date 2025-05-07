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

package variable

import (
	"context"
	"encoding/json"
	goerr "errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/privilege/privileges/ldap"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/servicescope"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/pingcap/tidb/pkg/util/tikvutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/tls"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tikvstore "github.com/tikv/client-go/v2/kv"
	tikvcliutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// All system variables declared here are ordered by their scopes, which follow the order of scopes below:
//
//	[NONE, SESSION, INSTANCE, GLOBAL, GLOBAL & SESSION]
//
// If you are adding a new system variable, please put it in the corresponding area.
var defaultSysVars = []*SysVar{
	/* The system variables below have NONE scope  */
	{Scope: vardef.ScopeNone, Name: vardef.SystemTimeZone, Value: "CST"},
	{Scope: vardef.ScopeNone, Name: vardef.Hostname, Value: vardef.DefHostname},
	{Scope: vardef.ScopeNone, Name: vardef.Port, Value: "4000", Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: vardef.ScopeNone, Name: vardef.VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 8.0 compatible"},
	{Scope: vardef.ScopeNone, Name: vardef.Version, Value: mysql.ServerVersion},
	{Scope: vardef.ScopeNone, Name: vardef.DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: vardef.ScopeNone, Name: vardef.Socket, Value: ""},
	{Scope: vardef.ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: vardef.ScopeNone, Name: "have_ssl", Value: "DISABLED", Type: vardef.TypeBool},
	{Scope: vardef.ScopeNone, Name: "have_openssl", Value: "DISABLED", Type: vardef.TypeBool},
	{Scope: vardef.ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: vardef.ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: vardef.ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: vardef.ScopeNone, Name: "version_compile_os", Value: runtime.GOOS},
	{Scope: vardef.ScopeNone, Name: "version_compile_machine", Value: runtime.GOARCH},
	/* TiDB specific variables */
	{Scope: vardef.ScopeNone, Name: vardef.TiDBEnableEnhancedSecurity, Value: vardef.Off, Type: vardef.TypeBool},
	{Scope: vardef.ScopeNone, Name: vardef.TiDBAllowFunctionForExpressionIndex, ReadOnly: true, Value: collectAllowFuncName4ExpressionIndex()},

	/* The system variables below have SESSION scope  */
	{Scope: vardef.ScopeSession, Name: vardef.Timestamp, Value: vardef.DefTimestamp, MinValue: 0, MaxValue: math.MaxInt32, Type: vardef.TypeFloat, GetSession: func(s *SessionVars) (string, error) {
		if timestamp, ok := s.systems[vardef.Timestamp]; ok && timestamp != vardef.DefTimestamp {
			return timestamp, nil
		}
		timestamp := s.StmtCtx.GetOrStoreStmtCache(stmtctx.StmtNowTsCacheKey, time.Now()).(time.Time)
		return types.ToString(float64(timestamp.UnixNano()) / float64(time.Second))
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		timestamp, ok := s.systems[vardef.Timestamp]
		return timestamp, ok && timestamp != vardef.DefTimestamp, nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		val := tidbOptFloat64(originalValue, vardef.DefTimestampFloat)
		if val > math.MaxInt32 {
			return originalValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.Timestamp, originalValue)
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.WarningCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(s.SysWarningCount), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.ErrorCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(int(s.SysErrorCount)), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.LastInsertID, Value: "0", Type: vardef.TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.Identity, Value: "0", Type: vardef.TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	/* TiDB specific variables */
	{Scope: vardef.ScopeSession, Name: vardef.TiDBTxnReadTS, Value: "", Hidden: true, SetSession: func(s *SessionVars, val string) error {
		return setTxnReadTS(s, val)
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBReadStaleness, Value: strconv.Itoa(vardef.DefTiDBReadStaleness), Type: vardef.TypeInt, MinValue: math.MinInt32, MaxValue: 0, AllowEmpty: true, Hidden: false, SetSession: func(s *SessionVars, val string) error {
		return setReadStaleness(s, val)
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnforceMPPExecution, Type: vardef.TypeBool, Value: BoolToOnOff(config.GetGlobalConfig().Performance.EnforceMPP), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && !vars.allowMPPExecution {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("tidb_enforce_mpp", "1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first.")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.enforceMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxTiFlashThreads, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxThreads), MinValue: -1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxThreads = TidbOptInt64(val, vardef.DefTiFlashMaxThreads)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalJoin), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalJoin = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalJoin)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalGroupBy), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalGroupBy = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalGroupBy)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalSort, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalSort), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalSort = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalSort)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashMemQuotaQueryPerNode, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMemQuotaQueryPerNode), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxQueryMemoryPerNode = TidbOptInt64(val, vardef.DefTiFlashMemQuotaQueryPerNode)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashQuerySpillRatio, Type: vardef.TypeFloat, Value: strconv.FormatFloat(vardef.DefTiFlashQuerySpillRatio, 'f', -1, 64), MinValue: 0, MaxValue: 1, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, flag vardef.ScopeFlag) (string, error) {
		val, err := strconv.ParseFloat(normalizedValue, 64)
		if err != nil {
			return "", err
		}
		if val > 0.85 || val < 0 {
			return "", errors.New("The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashQuerySpillRatio = tidbOptFloat64(val, vardef.DefTiFlashQuerySpillRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashHashJoinVersion, Value: vardef.DefTiFlashHashJoinVersion, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			lowerValue := strings.ToLower(normalizedValue)
			if lowerValue != joinversion.HashJoinVersionLegacy && lowerValue != joinversion.HashJoinVersionOptimized {
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s", originalValue, vardef.TiFlashHashJoinVersion, joinversion.HashJoinVersionLegacy+", "+joinversion.HashJoinVersionOptimized)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashHashJoinVersion = val
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTiFlashPipelineMode, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBEnableTiFlashPipelineMode), SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.TiFlashEnablePipelineMode.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.TiFlashEnablePipelineMode.Load()), nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGenByArgs(vardef.TiDBEnableTiFlashPipelineMode))
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSnapshot, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptProjectionPushDown, Value: BoolToOnOff(vardef.DefOptEnableProjectionPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowProjectionPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDeriveTopN, Value: BoolToOnOff(vardef.DefOptDeriveTopN), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDeriveTopN = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptAggPushDown, Value: BoolToOnOff(vardef.DefOptAggPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSkewDistinctAgg, Value: BoolToOnOff(vardef.DefTiDBSkewDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableSkewDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOpt3StageDistinctAgg, Value: BoolToOnOff(vardef.DefTiDB3StageDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnable3StageMultiDistinctAgg, Value: BoolToOnOff(vardef.DefTiDB3StageMultiDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageMultiDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptExplainNoEvaledSubQuery, Value: BoolToOnOff(vardef.DefTiDBOptExplainEvaledSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ExplainNonEvaledSubQuery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptWriteRowID, Value: BoolToOnOff(vardef.DefOptWriteRowID), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowWriteRowID = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBChecksumTableConcurrency, Value: strconv.Itoa(vardef.DefChecksumTableConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchInsert, Value: BoolToOnOff(vardef.DefBatchInsert), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchDelete, Value: BoolToOnOff(vardef.DefBatchDelete), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchDelete = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchCommit, Value: BoolToOnOff(vardef.DefBatchCommit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBCurrentTS, Value: strconv.Itoa(vardef.DefCurretTS), Type: vardef.TypeInt, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxInt64, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.TxnCtx.StartTS, 10), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastTxnInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return s.LastTxnInfo, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastQueryInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastQueryInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnableChunkRPC, Value: BoolToOnOff(config.GetGlobalConfig().TiKVClient.EnableChunkRPC), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChunkRPC = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TxnIsolationOneShot, Value: "", skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
		return nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		if s.txnIsolationLevelOneShot.state != oneShotDef {
			return s.txnIsolationLevelOneShot.value, true, nil
		}
		return "", false, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(vardef.DefTiDBOptimizerSelectivityLevel), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, vardef.DefTiDBOptimizerSelectivityLevel)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableOuterJoinReorder, Value: BoolToOnOff(vardef.DefTiDBEnableOuterJoinReorder), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableOuterJoinReorder = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableNAAJ, Value: BoolToOnOff(vardef.DefTiDBEnableNAAJ), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNAAJ = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBDDLReorgPriority, Value: "PRIORITY_LOW", Type: vardef.TypeEnum, skipInit: true, PossibleValues: []string{"PRIORITY_LOW", "PRIORITY_NORMAL", "PRIORITY_HIGH"}, SetSession: func(s *SessionVars, val string) error {
		s.setDDLReorgPriority(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSlowQueryFile, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.SlowQueryFile = val
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBWaitSplitRegionFinish, Value: BoolToOnOff(vardef.DefTiDBWaitSplitRegionFinish), skipInit: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionFinish = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(vardef.DefWaitSplitRegionTimeout), skipInit: true, Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, vardef.DefWaitSplitRegionTimeout))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBLowResolutionTSO, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.lowResolutionTSO = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBAllowRemoveAutoInc, Value: BoolToOnOff(vardef.DefTiDBAllowRemoveAutoInc), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowRemoveAutoInc = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBIsolationReadEngines, Value: strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ","), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			if i != 0 {
				formatVal += ","
			}
			switch {
			case strings.EqualFold(engine, kv.TiKV.Name()):
				formatVal += kv.TiKV.Name()
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				formatVal += kv.TiFlash.Name()
			case strings.EqualFold(engine, kv.TiDB.Name()):
				formatVal += kv.TiDB.Name()
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBIsolationReadEngines, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.IsolationReadEngines = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case kv.TiKV.Name():
				s.IsolationReadEngines[kv.TiKV] = struct{}{}
			case kv.TiFlash.Name():
				// If the tiflash is removed by the strict SQL mode. The hint should also not take effect.
				if !s.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
					s.IsolationReadEngines[kv.TiFlash] = struct{}{}
				}
			case kv.TiDB.Name():
				s.IsolationReadEngines[kv.TiDB] = struct{}{}
			}
		}
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMetricSchemaStep, Value: strconv.Itoa(vardef.DefTiDBMetricSchemaStep), Type: vardef.TypeUnsigned, skipInit: true, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaStep = TidbOptInt64(val, vardef.DefTiDBMetricSchemaStep)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBCDCWriteSource, Value: "0", Type: vardef.TypeInt, MinValue: 0, MaxValue: 15, SetSession: func(s *SessionVars, val string) error {
		s.CDCWriteSource = uint64(TidbOptInt(val, 0))
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(vardef.DefTiDBMetricSchemaRangeDuration), skipInit: true, Type: vardef.TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaRangeDuration = TidbOptInt64(val, vardef.DefTiDBMetricSchemaRangeDuration)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBFoundInPlanCache, Value: BoolToOnOff(vardef.DefTiDBFoundInPlanCache), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInPlanCache), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBFoundInBinding, Value: BoolToOnOff(vardef.DefTiDBFoundInBinding), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInBinding), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.RandSeed1, Type: vardef.TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed1(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed1()), 10), true, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.RandSeed2, Type: vardef.TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed2(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed2()), 10), true, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBReadConsistency, Value: string(ReadConsistencyStrict), Type: vardef.TypeStr, Hidden: true,
		Validation: func(_ *SessionVars, normalized string, _ string, _ vardef.ScopeFlag) (string, error) {
			return normalized, validateReadConsistencyLevel(normalized)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.ReadConsistency = ReadConsistencyLevel(val)
			return nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastDDLInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastDDLInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastPlanReplayerToken, Value: "", ReadOnly: true,
		GetSession: func(s *SessionVars) (string, error) {
			return s.LastPlanReplayerToken, nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBUseAlloc, Value: BoolToOnOff(vardef.DefTiDBUseAlloc), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.preUseChunkAlloc), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBExplicitRequestSourceType, Value: "", Type: vardef.TypeEnum, PossibleValues: tikvcliutil.ExplicitTypeList, GetSession: func(s *SessionVars) (string, error) {
		return s.ExplicitRequestSourceType, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.ExplicitRequestSourceType = val
		return nil
	}},
	/* The system variables below have INSTANCE scope  */
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBLogFileMaxDays, Value: strconv.Itoa(config.GetGlobalConfig().Log.File.MaxDays), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		maxAge, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}
		vardef.GlobalLogMaxDays.Store(int32(maxAge))
		cfg := config.GetGlobalConfig().Log.ToLogConfig()
		cfg.Config.File.MaxDays = int(maxAge)

		err = logutil.ReplaceLogger(cfg, keyspace.WrapZapcoreWithKeyspace())
		if err != nil {
			return err
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(int64(vardef.GlobalLogMaxDays.Load()), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBConfig, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetJSONConfig()
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBGeneralLog, Value: BoolToOnOff(vardef.DefTiDBGeneralLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.ProcessGeneralLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.ProcessGeneralLog.Load()), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSlowTxnLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowTxnThreshold),
		Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
			s.SlowTxnThreshold = TidbOptUint64(val, logutil.DefaultSlowTxnThreshold)
			return nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&config.GetGlobalConfig().Instance.SlowThreshold, uint64(TidbOptInt64(val, logutil.DefaultSlowThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog, uint32(TidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
		return BoolToOnOff(enabled), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.EnableSlowLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBPProfSQLCPU, Value: strconv.Itoa(vardef.DefTiDBPProfSQLCPU), Type: vardef.TypeInt, MinValue: 0, MaxValue: 1, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, vardef.DefTiDBPProfSQLCPU)) > 0)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		val := "0"
		if vardef.EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBDDLSlowOprThreshold, Value: strconv.Itoa(vardef.DefTiDBDDLSlowOprThreshold), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&vardef.DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLSlowOprThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(atomic.LoadUint32(&vardef.DDLSlowOprThreshold)), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBForcePriority, Value: mysql.Priority2Str[vardef.DefTiDBForcePriority], Type: vardef.TypeEnum, PossibleValues: []string{"NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreInt32(&vardef.ForcePriority, int32(mysql.Str2Priority(val)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&vardef.ForcePriority))], nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(vardef.DefTiDBExpensiveQueryTimeThreshold), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinExpensiveQueryTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&vardef.ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, vardef.DefTiDBExpensiveQueryTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&vardef.ExpensiveQueryTimeThreshold), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBExpensiveTxnTimeThreshold, Value: strconv.Itoa(vardef.DefTiDBExpensiveTxnTimeThreshold), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinExpensiveTxnTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&vardef.ExpensiveTxnTimeThreshold, uint64(tidbOptPositiveInt32(val, vardef.DefTiDBExpensiveTxnTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&vardef.ExpensiveTxnTimeThreshold), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(vardef.DefTiDBEnableCollectExecutionInfo), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		oldConfig := config.GetGlobalConfig()
		newValue := TiDBOptOn(val)
		if oldConfig.Instance.EnableCollectExecutionInfo.Load() != newValue {
			newConfig := *oldConfig
			newConfig.Instance.EnableCollectExecutionInfo.Store(newValue)
			config.StoreGlobalConfig(&newConfig)
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginLoad, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginLoad, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginDir, Value: "/data/deploy/plugin", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginDir, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.MaxConnections, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 100000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.MaxConnections = uint32(TidbOptInt64(val, 0))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableDDL, Value: BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			oldVal, newVal := config.GetGlobalConfig().Instance.TiDBEnableDDL.Load(), TiDBOptOn(val)
			if oldVal != newVal {
				err := switchDDL(newVal)
				if err != nil {
					return err
				}
				config.GetGlobalConfig().Instance.TiDBEnableDDL.Store(newVal)
			}
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()), nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableStatsOwner, Value: BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load()), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			oldVal, newVal := config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load(), TiDBOptOn(val)
			if oldVal != newVal {
				err := switchStats(newVal)
				if err != nil {
					return err
				}
				config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Store(newVal)
			}
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load()), nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBRCReadCheckTS, Value: BoolToOnOff(vardef.DefRCReadCheckTS), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableRCReadCheckTS.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableRCReadCheckTS.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryEnablePersistent, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFilename, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.StmtSummaryFilename, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxDays, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxDays), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxSize, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxSize), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxBackups, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxBackups), nil
	}},

	/* The system variables below have GLOBAL scope  */
	{Scope: vardef.ScopeGlobal, Name: vardef.MaxPreparedStmtCount, Value: strconv.FormatInt(vardef.DefMaxPreparedStmtCount, 10), Type: vardef.TypeInt, MinValue: -1, MaxValue: 1048576,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			vardef.MaxPreparedStmtCountValue.Store(num)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.InitConnect, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		p := parser.New()
		p.SetSQLMode(vars.SQLMode)
		p.SetParserConfig(vars.BuildParserConfig())
		_, _, err := p.ParseSQL(normalizedValue)
		if err != nil {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(vardef.InitConnect)
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordEnable, Value: vardef.Off, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordPolicy, Value: "MEDIUM", Type: vardef.TypeEnum, PossibleValues: []string{"LOW", "MEDIUM", "STRONG"}},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordCheckUserName, Value: vardef.On, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordLength, Value: "8", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			numberCount, specialCharCount, mixedCaseCount := vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationSpecialCharCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			length, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + specialCharCount + 2*mixedCaseCount; int32(length) < minLength {
				return strconv.FormatInt(int64(minLength), 10), nil
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationLength.Store(int32(TidbOptInt64(val, 8)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationLength.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordMixedCaseCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, numberCount, specialCharCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationSpecialCharCount.Load()
			mixedCaseCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + specialCharCount + 2*int32(mixedCaseCount); length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationMixedCaseCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationMixedCaseCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordNumberCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, specialCharCount, mixedCaseCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidationSpecialCharCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			numberCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := int32(numberCount) + specialCharCount + 2*mixedCaseCount; length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidtaionNumberCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidtaionNumberCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordSpecialCharCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, numberCount, mixedCaseCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			specialCharCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + int32(specialCharCount) + 2*mixedCaseCount; length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationSpecialCharCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationSpecialCharCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordDictionary, Value: "", Type: vardef.TypeStr},
	{Scope: vardef.ScopeGlobal, Name: vardef.DefaultPasswordLifetime, Value: "0", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: vardef.ScopeGlobal, Name: vardef.DisconnectOnExpiredPassword, Value: vardef.On, Type: vardef.TypeBool, ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(!vardef.IsSandBoxModeEnabled.Load()), nil
	}},

	/* TiDB specific variables */
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTSOClientBatchMaxWaitTime, Value: strconv.FormatFloat(vardef.DefTiDBTSOClientBatchMaxWaitTime, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 10,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.MaxTSOBatchWaitInterval.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return (*SetPDClientDynamicOption.Load())(vardef.TiDBTSOClientBatchMaxWaitTime, val)
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTSOFollowerProxy, Value: BoolToOnOff(vardef.DefTiDBEnableTSOFollowerProxy), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTSOFollowerProxy.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.TiDBEnableTSOFollowerProxy, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PDEnableFollowerHandleRegion, Value: BoolToOnOff(vardef.DefPDEnableFollowerHandleRegion), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnablePDFollowerHandleRegion.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.PDEnableFollowerHandleRegion, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableBatchQueryRegion, Value: BoolToOnOff(vardef.DefTiDBEnableBatchQueryRegion), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableBatchQueryRegion.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.TiDBEnableBatchQueryRegion, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableLocalTxn, Value: BoolToOnOff(vardef.DefTiDBEnableLocalTxn), Hidden: true, Type: vardef.TypeBool, Depended: true, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableLocalTxn.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		oldVal := vardef.EnableLocalTxn.Load()
		newVal := TiDBOptOn(val)
		// Make sure the TxnScope is always Global when disable the Local Txn.
		// ON -> OFF
		if oldVal && !newVal {
			s.TxnScope = kv.NewGlobalTxnScopeVar()
		}
		vardef.EnableLocalTxn.Store(newVal)
		return nil
	}},
	{
		Scope:    vardef.ScopeGlobal,
		Name:     vardef.TiDBAutoAnalyzeRatio,
		Value:    strconv.FormatFloat(vardef.DefAutoAnalyzeRatio, 'f', -1, 64),
		Type:     vardef.TypeFloat,
		MinValue: 0,
		MaxValue: math.MaxUint64,
		// The value of TiDBAutoAnalyzeRatio should be greater than 0.00001 or equal to 0.00001.
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			ratio, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				return "", err
			}
			const minRatio = 0.00001
			const tolerance = 1e-9
			if ratio < minRatio && math.Abs(ratio-minRatio) > tolerance {
				return "", errors.Errorf("the value of %s should be greater than or equal to %f", vardef.TiDBAutoAnalyzeRatio, minRatio)
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeStartTime, Value: vardef.DefAutoAnalyzeStartTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeEndTime, Value: vardef.DefAutoAnalyzeEndTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemQuotaBindingCache, Value: strconv.FormatInt(vardef.DefTiDBMemQuotaBindingCache, 10), Type: vardef.TypeUnsigned, MaxValue: math.MaxInt32, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.MemQuotaBindingCache.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemQuotaBindingCache.Store(TidbOptInt64(val, vardef.DefTiDBMemQuotaBindingCache))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLFlashbackConcurrency, Value: strconv.Itoa(vardef.DefTiDBDDLFlashbackConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLFlashbackConcurrency(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLFlashbackConcurrency)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDDLReorgWorkerCount, Value: strconv.Itoa(vardef.DefTiDBDDLReorgWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLReorgWorkerCount)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDDLReorgBatchSize, Value: strconv.Itoa(vardef.DefTiDBDDLReorgBatchSize), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinDDLReorgBatchSize), MaxValue: uint64(vardef.MaxDDLReorgBatchSize), SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLReorgBatchSize)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLReorgMaxWriteSpeed, Value: strconv.Itoa(vardef.DefTiDBDDLReorgMaxWriteSpeed), Type: vardef.TypeStr,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			i64, err := units.RAMInBytes(val)
			if err != nil {
				return errors.Trace(err)
			}
			if i64 < 0 || i64 > units.PiB {
				// Here we limit the max value to 1 PiB instead of math.MaxInt64, since:
				// 1. it is large enough
				// 2. units.RAMInBytes would first cast the size to a float, and may lose precision when the size is too large
				return fmt.Errorf("invalid value for '%d', it should be within [%d, %d]", i64, 0, units.PiB)
			}
			vardef.DDLReorgMaxWriteSpeed.Store(i64)
			return nil
		}, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.DDLReorgMaxWriteSpeed.Load(), 10), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLErrorCountLimit, Value: strconv.Itoa(vardef.DefTiDBDDLErrorCountLimit), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLErrorCountLimit(TidbOptInt64(val, vardef.DefTiDBDDLErrorCountLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(vardef.DefTiDBMaxDeltaSchemaCount), Type: vardef.TypeUnsigned, MinValue: 100, MaxValue: 16384, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		// It's a global variable, but it also wants to be cached in server.
		vardef.SetMaxDeltaSchemaCount(TidbOptInt64(val, vardef.DefTiDBMaxDeltaSchemaCount))
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnablePointGetCache, Value: BoolToOnOff(vardef.DefTiDBPointGetCache), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePointGetCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBScatterRegion, Value: vardef.DefTiDBScatterRegion, PossibleValues: []string{vardef.ScatterOff, vardef.ScatterTable, vardef.ScatterGlobal}, Type: vardef.TypeStr,
		SetSession: func(vars *SessionVars, val string) error {
			vars.ScatterRegion = val
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return vars.ScatterRegion, nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			lowerVal := strings.ToLower(normalizedValue)
			if lowerVal != vardef.ScatterOff && lowerVal != vardef.ScatterTable && lowerVal != vardef.ScatterGlobal {
				return "", fmt.Errorf("invalid value for '%s', it should be either '%s', '%s' or '%s'", lowerVal, vardef.ScatterOff, vardef.ScatterTable, vardef.ScatterGlobal)
			}
			return lowerVal, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableStmtSummary, Value: BoolToOnOff(vardef.DefTiDBEnableStmtSummary), Type: vardef.TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnabled(TiDBOptOn(val))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(vardef.DefTiDBStmtSummaryInternalQuery), Type: vardef.TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnableInternalQuery(TiDBOptOn(val))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryRefreshInterval), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			// convert val to int64
			return stmtsummaryv2.SetRefreshInterval(TidbOptInt64(val, vardef.DefTiDBStmtSummaryRefreshInterval))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryHistorySize, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryHistorySize), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetHistorySize(TidbOptInt(val, vardef.DefTiDBStmtSummaryHistorySize))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryMaxStmtCount, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryMaxStmtCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxInt16, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxStmtCount(TidbOptInt(val, vardef.DefTiDBStmtSummaryMaxStmtCount))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryMaxSQLLength, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryMaxSQLLength), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxSQLLength(TidbOptInt(val, vardef.DefTiDBStmtSummaryMaxSQLLength))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCapturePlanBaseline, Value: vardef.DefTiDBCapturePlanBaseline, Type: vardef.TypeBool, AllowEmptyAll: true},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(vardef.DefTiDBEvolvePlanTaskMaxTime), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskStartTime, Value: vardef.DefTiDBEvolvePlanTaskStartTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskEndTime, Value: vardef.DefTiDBEvolvePlanTaskEndTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(tikvstore.StoreLimit.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvstore.StoreLimit.Store(TidbOptInt64(val, vardef.DefTiDBStoreLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTxnCommitBatchSize, Value: strconv.FormatUint(tikvstore.DefTxnCommitBatchSize, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 1 << 30,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatUint(tikvstore.TxnCommitBatchSize.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tikvstore.TxnCommitBatchSize.Store(uint64(TidbOptInt64(val, int64(tikvstore.DefTxnCommitBatchSize))))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBRestrictedReadOnly, Value: BoolToOnOff(vardef.DefTiDBRestrictedReadOnly), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		// For user initiated SET GLOBAL, also change the value of TiDBSuperReadOnly
		if on && s.StmtCtx.StmtType == "Set" {
			err := s.GlobalVarsAccessor.SetGlobalSysVarOnly(context.Background(), vardef.TiDBSuperReadOnly, "ON", false)
			if err != nil {
				return err
			}
			err = GetSysVar(vardef.TiDBSuperReadOnly).SetGlobal(context.Background(), s, "ON")
			if err != nil {
				return err
			}
		}
		vardef.RestrictedReadOnly.Store(on)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSuperReadOnly, Value: BoolToOnOff(vardef.DefTiDBSuperReadOnly), Type: vardef.TypeBool, Validation: func(s *SessionVars, normalizedValue string, _ string, _ vardef.ScopeFlag) (string, error) {
		on := TiDBOptOn(normalizedValue)
		if !on && s.StmtCtx.StmtType == "Set" {
			result, err := s.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBRestrictedReadOnly)
			if err != nil {
				return normalizedValue, err
			}
			if TiDBOptOn(result) {
				return normalizedValue, fmt.Errorf("can't turn off %s when %s is on", vardef.TiDBSuperReadOnly, vardef.TiDBRestrictedReadOnly)
			}
		}
		return normalizedValue, nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.VarTiDBSuperReadOnly.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableGOGCTuner, Value: BoolToOnOff(vardef.DefTiDBEnableGOGCTuner), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		gctuner.EnableGOGCTuner.Store(on)
		if !on {
			gctuner.SetDefaultGOGC()
		}
		gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerMaxValue, Value: strconv.Itoa(vardef.DefTiDBGOGCMaxValue),
		Type: vardef.TypeInt, MinValue: 10, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			maxValue := TidbOptInt64(val, vardef.DefTiDBGOGCMaxValue)
			gctuner.SetMaxGCPercent(uint32(maxValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MaxGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope vardef.ScopeFlag) (string, error) {
			maxValue := TidbOptInt64(origin, vardef.DefTiDBGOGCMaxValue)
			if maxValue <= int64(gctuner.MinGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_max_value should be more than tidb_gogc_tuner_min_value")
			}
			return origin, nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerMinValue, Value: strconv.Itoa(vardef.DefTiDBGOGCMinValue),
		Type: vardef.TypeInt, MinValue: 10, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			minValue := TidbOptInt64(val, vardef.DefTiDBGOGCMinValue)
			gctuner.SetMinGCPercent(uint32(minValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MinGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope vardef.ScopeFlag) (string, error) {
			minValue := TidbOptInt64(origin, vardef.DefTiDBGOGCMinValue)
			if minValue >= int64(gctuner.MaxGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_min_value should be less than tidb_gogc_tuner_max_value")
			}
			return origin, nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTelemetry, Value: BoolToOnOff(vardef.DefTiDBEnableTelemetry), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return "OFF", nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("tidb_enable_telemetry is deprecated since Telemetry has been removed, this variable is 'OFF' always."))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableHistoricalStats, Value: vardef.Off, Type: vardef.TypeBool, Depended: true},
	/* tikv gc metrics */
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCEnable, Value: vardef.On, Type: vardef.TypeBool, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_enable", vardef.On)
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_enable", val, "Current GC enable status")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCRunInterval, Value: "10m0s", Type: vardef.TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_run_interval", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_run_interval", val, "GC run interval, at least 10m, in Go format.")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCLifetime, Value: "10m0s", Type: vardef.TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_life_time", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_life_time", val, "All versions within life time will not be collected by GC, at least 10m, in Go format.")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCConcurrency, Value: "-1", Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		autoConcurrencyVal, err := getTiDBTableValue(s, "tikv_gc_auto_concurrency", vardef.On)
		if err == nil && autoConcurrencyVal == vardef.On {
			return "-1", nil // convention for "AUTO"
		}
		return getTiDBTableValue(s, "tikv_gc_concurrency", "-1")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		autoConcurrency := vardef.Off
		if val == "-1" {
			autoConcurrency = vardef.On
		}
		// Update both autoconcurrency and concurrency.
		if err := setTiDBTableValue(s, "tikv_gc_auto_concurrency", autoConcurrency, "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used"); err != nil {
			return err
		}
		return setTiDBTableValue(s, "tikv_gc_concurrency", val, "How many goroutines used to do GC parallel, [1, 256], default 2")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCScanLockMode, Value: "LEGACY", Type: vardef.TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_scan_lock_mode", "LEGACY")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_scan_lock_mode", val, "Mode of scanning locks, \"physical\" or \"legacy\"")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCMaxWaitTime, Value: strconv.Itoa(vardef.DefTiDBGCMaxWaitTime), Type: vardef.TypeInt, MinValue: 600, MaxValue: 31536000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.GCMaxWaitTime.Store(TidbOptInt64(val, vardef.DefTiDBGCMaxWaitTime))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTableCacheLease, Value: strconv.Itoa(vardef.DefTiDBTableCacheLease), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 10, SetGlobal: func(_ context.Context, s *SessionVars, sVal string) error {
		var val int64
		val, err := strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		vardef.TableCacheLease.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzePartitionBatchSize,
		Value: strconv.Itoa(vardef.DefTiDBAutoAnalyzePartitionBatchSize),
		Type:  vardef.TypeUnsigned, MinValue: 1, MaxValue: mysql.PartitionCountLimit,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			var val int64
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			vardef.AutoAnalyzePartitionBatchSize.Store(val)
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs(vardef.TiDBAutoAnalyzePartitionBatchSize))
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.MaxUserConnections, Value: strconv.FormatUint(vardef.DefMaxUserConnections, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 100000,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.MaxUserConnectionsValue.Store(uint32(TidbOptInt64(val, vardef.DefMaxUserConnections)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatUint(uint64(vardef.MaxUserConnectionsValue.Load()), 10), nil
		},
	},
	// variable for top SQL feature.
	// TopSQL enable only be controlled by TopSQL pub/sub sinker.
	// This global variable only uses to update the global config which store in PD(ETCD).
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTopSQL, Value: BoolToOnOff(topsqlstate.DefTiDBTopSQLEnable), Type: vardef.TypeBool, AllowEmpty: true, GlobalConfigName: vardef.GlobalConfigEnableTopSQL},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSourceID, Value: "1", Type: vardef.TypeInt, MinValue: 1, MaxValue: 15, GlobalConfigName: vardef.GlobalConfigSourceID},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTopSQLMaxTimeSeriesCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxTimeSeriesCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: 5000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxStatementCount.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxStatementCount.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTopSQLMaxMetaCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxMetaCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxCollect.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxCollect.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.SkipNameResolve, Value: vardef.Off, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.DefaultAuthPlugin, Value: mysql.AuthNativePassword, Type: vardef.TypeEnum, PossibleValues: []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthLDAPSASL, mysql.AuthLDAPSimple}},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBPersistAnalyzeOptions,
		Value: BoolToOnOff(vardef.DefTiDBPersistAnalyzeOptions),
		Type:  vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.PersistAnalyzeOptions.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			persist := TiDBOptOn(val)
			vardef.PersistAnalyzeOptions.Store(persist)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableAutoAnalyze, Value: BoolToOnOff(vardef.DefTiDBEnableAutoAnalyze), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.RunAutoAnalyze.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.RunAutoAnalyze.Store(TiDBOptOn(val))
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBAnalyzeColumnOptions,
		Value: vardef.DefTiDBAnalyzeColumnOptions,
		Type:  vardef.TypeStr,
		GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
			return vardef.AnalyzeColumnOptions.Load(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.AnalyzeColumnOptions.Store(strings.ToUpper(val))
			return nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			choice := strings.ToUpper(normalizedValue)
			if choice != ast.AllColumns.String() && choice != ast.PredicateColumns.String() {
				return "", errors.Errorf(
					"invalid value for %s, it should be either '%s' or '%s'",
					vardef.TiDBAnalyzeColumnOptions,
					ast.AllColumns.String(),
					ast.PredicateColumns.String(),
				)
			}
			return normalizedValue, nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableAutoAnalyzePriorityQueue, Value: BoolToOnOff(vardef.DefTiDBEnableAutoAnalyzePriorityQueue), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableAutoAnalyzePriorityQueue.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.EnableAutoAnalyzePriorityQueue.Store(TiDBOptOn(val))
			return nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("tidb_enable_auto_analyze_priority_queue will be removed in the future and TiDB will always use priority queue to execute auto analyze."))
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerThreshold, Value: strconv.FormatFloat(vardef.DefTiDBGOGCTunerThreshold, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.GOGCTunerThreshold.Load(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			floatValue := tidbOptFloat64(normalizedValue, vardef.DefTiDBGOGCTunerThreshold)
			globalMemoryLimitTuner := gctuner.GlobalMemoryLimitTuner.GetPercentage()
			if floatValue < 0 && floatValue > 0.9 {
				return "", ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBGOGCTunerThreshold, normalizedValue)
			}
			// globalMemoryLimitTuner must not be 0. it will be 0 when tidb_server_memory_limit_gc_trigger is not set during startup.
			if globalMemoryLimitTuner != 0 && globalMemoryLimitTuner < floatValue+0.05 {
				return "", errors.New("tidb_gogc_tuner_threshold should be less than tidb_server_memory_limit_gc_trigger - 0.05")
			}
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) (err error) {
			factor := tidbOptFloat64(val, vardef.DefTiDBGOGCTunerThreshold)
			vardef.GOGCTunerThreshold.Store(factor)
			memTotal := memory.ServerMemoryLimit.Load()
			if memTotal == 0 {
				memTotal, err = memory.MemTotal()
				if err != nil {
					return err
				}
			}
			if factor > 0 {
				threshold := float64(memTotal) * factor
				gctuner.Tuning(uint64(threshold))
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBServerMemoryLimit, Value: vardef.DefTiDBServerMemoryLimit, Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitOriginText.Load(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			_, str, err := parseMemoryLimit(s, normalizedValue, originalValue)
			if err != nil {
				return "", err
			}
			return str, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			bt, str, err := parseMemoryLimit(s, val, val)
			if err != nil {
				return err
			}
			memory.ServerMemoryLimitOriginText.Store(str)
			memory.ServerMemoryLimit.Store(bt)
			threshold := float64(bt) * vardef.GOGCTunerThreshold.Load()
			gctuner.Tuning(uint64(threshold))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBServerMemoryLimitSessMinSize, Value: strconv.FormatUint(vardef.DefTiDBServerMemoryLimitSessMinSize, 10), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitSessMinSize.String(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			intVal, err := strconv.ParseUint(normalizedValue, 10, 64)
			if err != nil {
				bt, str := parseByteSize(normalizedValue)
				if str == "" {
					return "", err
				}
				intVal = bt
			}
			if intVal > 0 && intVal < 128 { // 128 Bytes
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBServerMemoryLimitSessMinSize, originalValue))
				intVal = 128
			}
			return strconv.FormatUint(intVal, 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			intVal, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return err
			}
			memory.ServerMemoryLimitSessMinSize.Store(intVal)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBServerMemoryLimitGCTrigger, Value: strconv.FormatFloat(vardef.DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(gctuner.GlobalMemoryLimitTuner.GetPercentage(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			floatValue, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				perc, str := parsePercentage(normalizedValue)
				if len(str) == 0 {
					return "", err
				}
				floatValue = float64(perc) / 100
			}
			gogcTunerThreshold := vardef.GOGCTunerThreshold.Load()
			if floatValue < 0.51 || floatValue > 1 { // 51% ~ 100%
				return "", ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBServerMemoryLimitGCTrigger, normalizedValue)
			}
			// gogcTunerThreshold must not be 0. it will be 0 when tidb_gogc_tuner_threshold is not set during startup.
			if gogcTunerThreshold != 0 && floatValue < gogcTunerThreshold+0.05 {
				return "", errors.New("tidb_server_memory_limit_gc_trigger should be greater than tidb_gogc_tuner_threshold + 0.05")
			}

			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			floatValue, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return err
			}
			gctuner.GlobalMemoryLimitTuner.SetPercentage(floatValue)
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableColumnTracking,
		Value: BoolToOnOff(true),
		Type:  vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(true), nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			// This variable is deprecated and will be removed in the future.
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("The 'tidb_enable_column_tracking' variable is deprecated and will be removed in future versions of TiDB. It is always set to 'ON' now."))
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.RequireSecureTransport, Value: BoolToOnOff(vardef.DefRequireSecureTransport), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(tls.RequireSecureTransport.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tls.RequireSecureTransport.Store(TiDBOptOn(val))
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && TiDBOptOn(normalizedValue) {
				// On tidbcloud dedicated cluster with the default configuration, if an user modify
				// @@global.require_secure_transport=on, he can not login the cluster anymore!
				// A workaround for this is making require_secure_transport read-only for that case.
				// SEM(security enhanced mode) is enabled by default with only that settings.
				cfg := config.GetGlobalConfig()
				if cfg.Security.EnableSEM {
					return "", errors.New("require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled")
				}
				// Refuse to set RequireSecureTransport to ON if the connection
				// issuing the change is not secure. This helps reduce the chance of users being locked out.
				if vars.TLSConnectionState == nil {
					return "", errors.New("require_secure_transport can only be set to ON if the connection issuing the change is secure")
				}
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStatsLoadPseudoTimeout, Value: BoolToOnOff(vardef.DefTiDBStatsLoadPseudoTimeout), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.StatsLoadPseudoTimeout.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.StatsLoadPseudoTimeout.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableBatchDML, Value: BoolToOnOff(vardef.DefTiDBEnableBatchDML), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableBatchDML.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableBatchDML.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStatsCacheMemQuota, Value: strconv.Itoa(vardef.DefTiDBStatsCacheMemQuota),
		MinValue: 0, MaxValue: vardef.MaxTiDBStatsCacheMemQuota, Type: vardef.TypeInt,
		GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.StatsCacheMemQuota.Load(), 10), nil
		}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			v := TidbOptInt64(s, vardef.DefTiDBStatsCacheMemQuota)
			oldv := vardef.StatsCacheMemQuota.Load()
			if v != oldv {
				vardef.StatsCacheMemQuota.Store(v)
				SetStatsCacheCapacityFunc := SetStatsCacheCapacity.Load()
				(*SetStatsCacheCapacityFunc)(v)
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBQueryLogMaxLen, Value: strconv.Itoa(vardef.DefTiDBQueryLogMaxLen), Type: vardef.TypeInt, MinValue: 0, MaxValue: 1073741824, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.QueryLogMaxLen.Store(int32(TidbOptInt64(val, vardef.DefTiDBQueryLogMaxLen)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(vardef.QueryLogMaxLen.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCommitterConcurrency, Value: strconv.Itoa(vardef.DefTiDBCommitterConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvutil.CommitterConcurrency.Store(int32(TidbOptInt64(val, vardef.DefTiDBCommitterConcurrency)))
		cfg := config.GetGlobalConfig().GetTiKVConfig()
		tikvcfg.StoreGlobalConfig(cfg)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(tikvutil.CommitterConcurrency.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemQuotaAnalyze, Value: strconv.Itoa(vardef.DefTiDBMemQuotaAnalyze), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(GetMemQuotaAnalyze(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			SetMemQuotaAnalyze(TidbOptInt64(val, vardef.DefTiDBMemQuotaAnalyze))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePrepPlanCache, Value: BoolToOnOff(vardef.DefTiDBEnablePrepPlanCache), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPrepPlanCacheSize, Aliases: []string{vardef.TiDBSessionPlanCacheSize}, Value: strconv.FormatUint(uint64(vardef.DefTiDBPrepPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBPrepPlanCacheSize, vardef.TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePrepPlanCacheMemoryMonitor, Value: BoolToOnOff(vardef.DefTiDBEnablePrepPlanCacheMemoryMonitor), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCacheMemoryMonitor = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBPrepPlanCacheMemoryGuardRatio, Value: strconv.FormatFloat(vardef.DefTiDBPrepPlanCacheMemoryGuardRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			vardef.PreparedPlanCacheMemoryGuardRatio.Store(f)
		}
		return err
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatFloat(vardef.PreparedPlanCacheMemoryGuardRatio.Load(), 'f', -1, 64), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNonPreparedPlanCache, Value: BoolToOnOff(vardef.DefTiDBEnableNonPreparedPlanCache), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNonPreparedPlanCacheForDML, Value: BoolToOnOff(vardef.DefTiDBEnableNonPreparedPlanCacheForDML), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCacheForDML = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptEnableFuzzyBinding,
		Value:                   BoolToOnOff(false),
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFuzzyBinding = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBNonPreparedPlanCacheSize, Value: strconv.FormatUint(uint64(vardef.DefTiDBNonPreparedPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.NonPreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBNonPreparedPlanCacheSize, vardef.TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlanCacheMaxPlanSize, Value: strconv.FormatUint(vardef.DefTiDBPlanCacheMaxPlanSize, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PlanCacheMaxPlanSize = uVal
		}
		return err
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSessionPlanCacheSize, Aliases: []string{vardef.TiDBPrepPlanCacheSize}, Value: strconv.FormatUint(uint64(vardef.DefTiDBSessionPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.SessionPlanCacheSize = uVal
		}
		return err
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableInstancePlanCache, Value: vardef.Off, Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableInstancePlanCache.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.EnableInstancePlanCache.Store(TiDBOptOn(val))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBInstancePlanCacheReservedPercentage,
		Value: strconv.FormatFloat(vardef.DefTiDBInstancePlanCacheReservedPercentage, 'f', -1, 64),
		Type:  vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.InstancePlanCacheReservedPercentage.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			v := tidbOptFloat64(val, vardef.DefTiDBInstancePlanCacheReservedPercentage)
			if v < 0 || v > 1 {
				return errors.Errorf("invalid tidb_instance_plan_cache_reserved_percentage value %s", val)
			}
			vardef.InstancePlanCacheReservedPercentage.Store(v)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBInstancePlanCacheMaxMemSize, Value: strconv.Itoa(int(vardef.DefTiDBInstancePlanCacheMaxMemSize)), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.InstancePlanCacheMaxMemSize.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			v, str := parseByteSize(val)
			if str == "" || v < 0 {
				return errors.Errorf("invalid tidb_instance_plan_cache_max_mem_size value %s", val)
			}
			if v < vardef.MinTiDBInstancePlanCacheMemSize {
				return errors.Errorf("tidb_instance_plan_cache_max_mem_size should be at least 100MiB")
			}
			vardef.InstancePlanCacheMaxMemSize.Store(int64(v))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemOOMAction, Value: vardef.DefTiDBMemOOMAction, PossibleValues: []string{"CANCEL", "LOG"}, Type: vardef.TypeEnum,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return vardef.OOMAction.Load(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.OOMAction.Store(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMaxAutoAnalyzeTime, Value: strconv.Itoa(vardef.DefTiDBMaxAutoAnalyzeTime), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.MaxAutoAnalyzeTime.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				vardef.MaxAutoAnalyzeTime.Store(num)
			}
			return err
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeConcurrency,
		Value:    strconv.Itoa(vardef.DefTiDBAutoAnalyzeConcurrency),
		Type:     vardef.TypeInt,
		MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.AutoAnalyzeConcurrency.Load()), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				vardef.AutoAnalyzeConcurrency.Store(int32(num))
			}
			return err
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			// Check if auto-analyze and auto-analyze priority queue are enabled
			enableAutoAnalyze := vardef.RunAutoAnalyze.Load()
			enableAutoAnalyzePriorityQueue := vardef.EnableAutoAnalyzePriorityQueue.Load()

			// Validate that both required settings are enabled
			if !enableAutoAnalyze || !enableAutoAnalyzePriorityQueue {
				return originalValue, errors.Errorf(
					"cannot set %s: requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue to be true. Current values: tidb_enable_auto_analyze=%v, tidb_enable_auto_analyze_priority_queue=%v",
					vardef.TiDBAutoAnalyzeConcurrency,
					enableAutoAnalyze,
					enableAutoAnalyzePriorityQueue,
				)
			}

			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableMDL, Value: BoolToOnOff(vardef.DefTiDBEnableMDL), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, vars *SessionVars, val string) error {
		if vardef.EnableMDL.Load() != TiDBOptOn(val) {
			err := SwitchMDL(TiDBOptOn(val))
			if err != nil {
				return err
			}
		}
		return nil
	}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableMDL.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableDistTask, Value: BoolToOnOff(vardef.DefTiDBEnableDistTask), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if vardef.EnableDistTask.Load() != TiDBOptOn(val) {
			vardef.EnableDistTask.Store(TiDBOptOn(val))
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableDistTask.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableFastCreateTable, Value: BoolToOnOff(vardef.DefTiDBEnableFastCreateTable), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if vardef.EnableFastCreateTable.Load() != TiDBOptOn(val) {
			vardef.EnableFastCreateTable.Store(TiDBOptOn(val))
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableFastCreateTable.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableNoopVariables, Value: BoolToOnOff(vardef.DefTiDBEnableNoopVariables), Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableNoopVariables.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableNoopVariables.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableGCAwareMemoryTrack, Value: BoolToOnOff(vardef.DefEnableTiDBGCAwareMemoryTrack), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		memory.EnableGCAwareMemoryTrack.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(memory.EnableGCAwareMemoryTrack.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTmpStorageOnOOM, Value: BoolToOnOff(vardef.DefTiDBEnableTmpStorageOnOOM), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableTmpStorageOnOOM.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTmpStorageOnOOM.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoBuildStatsConcurrency, Value: strconv.Itoa(vardef.DefTiDBAutoBuildStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSysProcScanConcurrency, Value: strconv.Itoa(vardef.DefTiDBSysProcScanConcurrency), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(vardef.DefMemoryUsageAlarmRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemoryUsageAlarmRatio.Store(tidbOptFloat64(val, vardef.DefMemoryUsageAlarmRatio))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprintf("%g", vardef.MemoryUsageAlarmRatio.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemoryUsageAlarmKeepRecordNum, Value: strconv.Itoa(vardef.DefMemoryUsageAlarmKeepRecordNum), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemoryUsageAlarmKeepRecordNum.Store(TidbOptInt64(val, vardef.DefMemoryUsageAlarmKeepRecordNum))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.MemoryUsageAlarmKeepRecordNum.Load(), 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PasswordReuseHistory, Value: strconv.Itoa(vardef.DefPasswordReuseHistory), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.PasswordHistory.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.PasswordHistory.Store(TidbOptInt64(val, vardef.DefPasswordReuseHistory))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PasswordReuseTime, Value: strconv.Itoa(vardef.DefPasswordReuseTime), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.PasswordReuseInterval.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.PasswordReuseInterval.Store(TidbOptInt64(val, vardef.DefPasswordReuseTime))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableHistoricalStatsForCapture, Value: BoolToOnOff(vardef.DefTiDBEnableHistoricalStatsForCapture), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			vardef.EnableHistoricalStatsForCapture.Store(TiDBOptOn(s))
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableHistoricalStatsForCapture.Load()), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBHistoricalStatsDuration, Value: vardef.DefTiDBHistoricalStatsDuration.String(), Type: vardef.TypeDuration, MinValue: int64(time.Second), MaxValue: uint64(time.Hour * 24 * 365),
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.HistoricalStatsDuration.Load().String(), nil
		}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			vardef.HistoricalStatsDuration.Store(d)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBLowResolutionTSOUpdateInterval, Value: strconv.Itoa(vardef.DefTiDBLowResolutionTSOUpdateInterval), Type: vardef.TypeInt, MinValue: 10, MaxValue: 60000,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.LowResolutionTSOUpdateInterval.Store(uint32(TidbOptInt64(val, vardef.DefTiDBLowResolutionTSOUpdateInterval)))
			if SetLowResolutionTSOUpdateInterval != nil {
				interval := time.Duration(vardef.LowResolutionTSOUpdateInterval.Load()) * time.Millisecond
				return SetLowResolutionTSOUpdateInterval(interval)
			}
			return nil
		},
	},

	/* The system variables below have GLOBAL and SESSION scope  */
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanReplayerContinuousCapture, Value: BoolToOnOff(false), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			historicalStatsEnabled, err := s.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableHistoricalStats)
			if err != nil {
				return err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(val) {
				return errors.Errorf("%v should be enabled before enabling %v", vardef.TiDBEnableHistoricalStats, vardef.TiDBEnablePlanReplayerContinuousCapture)
			}
			s.EnablePlanReplayedContinuesCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayedContinuesCapture), nil
		},
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			historicalStatsEnabled, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableHistoricalStats)
			if err != nil {
				return "", err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(s) {
				return "", errors.Errorf("%v should be enabled before enabling %v", vardef.TiDBEnableHistoricalStats, vardef.TiDBEnablePlanReplayerContinuousCapture)
			}
			return s, nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanReplayerCapture, Value: BoolToOnOff(vardef.DefTiDBEnablePlanReplayerCapture), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnablePlanReplayerCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayerCapture), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRowFormatVersion, Value: strconv.Itoa(vardef.DefTiDBRowFormatV1), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 2, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgRowFormat(TidbOptInt64(val, vardef.DefTiDBRowFormatV2))
		return nil
	}, SetSession: func(s *SessionVars, val string) error {
		formatVersion := TidbOptInt64(val, vardef.DefTiDBRowFormatV1)
		if formatVersion == vardef.DefTiDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == vardef.DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableRowLevelChecksum, Value: BoolToOnOff(vardef.DefTiDBEnableRowLevelChecksum), Type: vardef.TypeBool,
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableRowLevelChecksum.Load()), nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			vardef.EnableRowLevelChecksum.Store(TiDBOptOn(s))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.SQLSelectLimit, Value: "18446744073709551615", Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DefaultWeekFormat, Value: vardef.DefDefaultWeekFormat, Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 7},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.SQLModeVar,
		Value:                   mysql.DefaultSQLMode,
		IsHintUpdatableVerified: true,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag,
		) (string, error) {
			// Ensure the SQL mode parses
			normalizedValue = mysql.FormatSQLModeStr(normalizedValue)
			if _, err := mysql.GetSQLMode(normalizedValue); err != nil {
				return originalValue, err
			}
			return normalizedValue, nil
		}, SetSession: func(s *SessionVars, val string) error {
			val = mysql.FormatSQLModeStr(val)
			// Modes is a list of different modes separated by commas.
			sqlMode, err := mysql.GetSQLMode(val)
			if err != nil {
				return errors.Trace(err)
			}
			s.SQLMode = sqlMode
			s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal,
		Name:                    vardef.TiDBLoadBindingTimeout,
		Value:                   "200",
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: false,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			timeoutMS := tidbOptPositiveInt32(s, 0)
			vars.LoadBindingTimeout = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.MaxExecutionTime,
		Value:                   "0",
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.MaxExecutionTime = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiKVClientReadTimeout,
		Value:                   "0",
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.TiKVClientReadTimeout = uint64(timeoutMS)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharacterSetServer] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DefaultCollationForUTF8MB4, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		coll, err := checkDefaultCollationForUTF8MB4(vars, normalizedValue, originalValue, scope)
		if err == nil {
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs(vardef.DefaultCollationForUTF8MB4))
		}
		return coll, err
	}, SetSession: func(s *SessionVars, val string) error {
		s.DefaultCollationForUTF8MB4 = val
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TimeZone,
		Value:                   "SYSTEM",
		IsHintUpdatableVerified: true,
		Validation: func(
			varErrFunctionsNoopImpls *SessionVars, normalizedValue string, originalValue string,
			scope vardef.ScopeFlag,
		) (string, error) {
			if strings.EqualFold(normalizedValue, "SYSTEM") {
				return "SYSTEM", nil
			}
			_, err := timeutil.ParseTimeZone(normalizedValue)
			return normalizedValue, err
		}, SetSession: func(s *SessionVars, val string) error {
			tz, err := timeutil.ParseTimeZone(val)
			if err != nil {
				return err
			}
			s.TimeZone = tz
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.ForeignKeyChecks, Value: BoolToOnOff(vardef.DefTiDBForeignKeyChecks), Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = true
			return vardef.On, nil
		} else if !TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = false
			return vardef.Off, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.ForeignKeyChecks, originalValue)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableForeignKey, Value: BoolToOnOff(true), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableForeignKey.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableForeignKey.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationDatabase, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharsetDatabase] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoIncrementIncrement, Value: strconv.FormatInt(vardef.DefAutoIncrementIncrement, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = tidbOptPositiveInt32(val, vardef.DefAutoIncrementIncrement)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoIncrementOffset, Value: strconv.FormatInt(vardef.DefAutoIncrementOffset, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = tidbOptPositiveInt32(val, vardef.DefAutoIncrementOffset)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetClient, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetClient)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetResults, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return normalizedValue, nil
		}
		return checkCharacterSet(normalizedValue, "")
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TxnIsolation, Value: "REPEATABLE-READ", Type: vardef.TypeEnum, Aliases: []string{vardef.TransactionIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		// MySQL appends a warning here for tx_isolation is deprecated
		// TiDB doesn't currently, but may in future. It is still commonly used by applications
		// So it might be noisy to do so.
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TransactionIsolation, Value: "REPEATABLE-READ", Type: vardef.TypeEnum, Aliases: []string{vardef.TxnIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationConnection, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharacterSetConnection] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoCommit, Value: vardef.On, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		isAutocommit := TiDBOptOn(val)
		// Implicitly commit the possible ongoing transaction if mode is changed from off to on.
		if !s.IsAutocommit() && isAutocommit {
			s.SetInTxn(false)
		}
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharsetDatabase, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharsetDatabase)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationDatabase] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.WaitTimeout, Value: strconv.FormatInt(vardef.DefWaitTimeout, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.InteractiveTimeout, Value: "28800", Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: secondsPerYear},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.InnodbLockWaitTimeout, Value: strconv.FormatInt(vardef.DefInnodbLockWaitTimeout, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 3600, SetSession: func(s *SessionVars, val string) error {
		lockWaitSec := TidbOptInt64(val, vardef.DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = lockWaitSec * 1000
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.GroupConcatMaxLen,
		Value:                   strconv.FormatUint(vardef.DefGroupConcatMaxLen, 10),
		IsHintUpdatableVerified: true,
		Type:                    vardef.TypeUnsigned,
		MinValue:                4,
		MaxValue:                math.MaxUint64,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag,
		) (string, error) {
			// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
			// Minimum Value 4
			// Maximum Value (64-bit platforms) 18446744073709551615
			// Maximum Value (32-bit platforms) 4294967295
			if mathutil.IntBits == 32 {
				if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
					if val > uint64(math.MaxUint32) {
						vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.GroupConcatMaxLen, originalValue))
						return strconv.FormatInt(int64(math.MaxUint32), 10), nil
					}
				}
			}
			return normalizedValue, nil
		},
		SetSession: func(sv *SessionVars, s string) error {
			var err error
			if sv.GroupConcatMaxLen, err = strconv.ParseUint(s, 10, 64); err != nil {
				return err
			}
			return nil
		},
		GetSession: func(sv *SessionVars) (string, error) {
			return strconv.FormatUint(sv.GroupConcatMaxLen, 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetConnection, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetConnection)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationConnection] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetServer, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetServer)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationServer] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MaxAllowedPacket, Value: strconv.FormatUint(vardef.DefMaxAllowedPacket, 10), Type: vardef.TypeUnsigned, MinValue: 1024, MaxValue: vardef.MaxOfMaxAllowedPacket,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && scope == vardef.ScopeSession {
				err := ErrReadOnly.GenWithStackByArgs("SESSION", vardef.MaxAllowedPacket, "GLOBAL")
				return normalizedValue, err
			}
			// Truncate the value of max_allowed_packet to be a multiple of 1024,
			// nonmultiples are rounded down to the nearest multiple.
			u, err := strconv.ParseUint(normalizedValue, 10, 64)
			if err != nil {
				return normalizedValue, err
			}
			remainder := u % 1024
			if remainder != 0 {
				vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.MaxAllowedPacket, normalizedValue))
				u -= remainder
			}
			return strconv.FormatUint(u, 10), nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return strconv.FormatUint(s.MaxAllowedPacket, 10), nil
		},
		SetSession: func(s *SessionVars, val string) error {
			var err error
			if s.MaxAllowedPacket, err = strconv.ParseUint(val, 10, 64); err != nil {
				return err
			}
			return nil
		},
	},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.WindowingUseHighPrecision,
		Value:                   vardef.On,
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.WindowingUseHighPrecision = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.BlockEncryptionMode, Value: vardef.DefBlockEncryptionMode, Type: vardef.TypeEnum, PossibleValues: []string{"aes-128-ecb", "aes-192-ecb", "aes-256-ecb", "aes-128-cbc", "aes-192-cbc", "aes-256-cbc", "aes-128-ofb", "aes-192-ofb", "aes-256-ofb", "aes-128-cfb", "aes-192-cfb", "aes-256-cfb"}},
	/* TiDB specific variables */
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowMPPExecution, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBAllowMPPExecution), Depended: true, SetSession: func(s *SessionVars, val string) error {
		s.allowMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowTiFlashCop, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBAllowTiFlashCop), SetSession: func(s *SessionVars, val string) error {
		s.allowTiFlashCop = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFastScan, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiFlashFastScan), SetSession: func(s *SessionVars, val string) error {
		s.TiFlashFastScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMPPStoreFailTTL, Type: vardef.TypeStr, Value: vardef.DefTiDBMPPStoreFailTTL, SetSession: func(s *SessionVars, val string) error {
		s.MPPStoreFailTTL = val
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashExchangeWithNewCollation, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBHashExchangeWithNewCollation), SetSession: func(s *SessionVars, val string) error {
		s.HashExchangeWithNewCollation = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBCJThresholdCount, Value: strconv.Itoa(vardef.DefBroadcastJoinThresholdCount), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdCount = TidbOptInt64(val, vardef.DefBroadcastJoinThresholdCount)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBCJThresholdSize, Value: strconv.Itoa(vardef.DefBroadcastJoinThresholdSize), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdSize = TidbOptInt64(val, vardef.DefBroadcastJoinThresholdSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPreferBCJByExchangeDataSize, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefPreferBCJByExchangeDataSize), SetSession: func(s *SessionVars, val string) error {
		s.PreferBCJByExchangeDataSize = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBuildStatsConcurrency, Value: strconv.Itoa(vardef.DefBuildStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBuildSamplingStatsConcurrency, Value: strconv.Itoa(vardef.DefBuildSamplingStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCartesianBCJ, Value: strconv.Itoa(vardef.DefOptCartesianBCJ), Type: vardef.TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowCartesianBCJ = TidbOptInt(val, vardef.DefOptCartesianBCJ)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMPPOuterJoinFixedBuildSide, Value: BoolToOnOff(vardef.DefOptMPPOuterJoinFixedBuildSide), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.MPPOuterJoinFixedBuildSide = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBExecutorConcurrency, Value: strconv.Itoa(vardef.DefExecutorConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.ExecutorConcurrency = tidbOptPositiveInt32(val, vardef.DefExecutorConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDistSQLScanConcurrency, Value: strconv.Itoa(vardef.DefDistSQLScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.distSQLScanConcurrency = tidbOptPositiveInt32(val, vardef.DefDistSQLScanConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeDistSQLScanConcurrency, Value: strconv.Itoa(vardef.DefAnalyzeDistSQLScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.analyzeDistSQLScanConcurrency = tidbOptPositiveInt32(val, vardef.DefAnalyzeDistSQLScanConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(vardef.DefOptInSubqToJoinAndAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptPreferRangeScan,
		Value:                   BoolToOnOff(vardef.DefOptPreferRangeScan),
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.SetAllowPreferRangeScan(TiDBOptOn(val))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptLimitPushDownThreshold, Value: strconv.Itoa(vardef.DefOptLimitPushDownThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.LimitPushDownThreshold = TidbOptInt64(val, vardef.DefOptLimitPushDownThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(vardef.DefOptCorrelationThreshold, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationThreshold = tidbOptFloat64(val, vardef.DefOptCorrelationThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableCorrelationAdjustment, Value: BoolToOnOff(vardef.DefOptEnableCorrelationAdjustment), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableCorrelationAdjustment = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCorrelationExpFactor, Value: strconv.Itoa(vardef.DefOptCorrelationExpFactor), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationExpFactor = int(TidbOptInt64(val, vardef.DefOptCorrelationExpFactor))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRiskEqSkewRatio, Value: strconv.FormatFloat(vardef.DefOptRiskEqSkewRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.RiskEqSkewRatio = tidbOptFloat64(val, vardef.DefOptRiskEqSkewRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCPUFactor, Value: strconv.FormatFloat(vardef.DefOptCPUFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.cpuFactor = tidbOptFloat64(val, vardef.DefOptCPUFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(vardef.DefOptTiFlashConcurrencyFactor, 'f', -1, 64), skipInit: true, Type: vardef.TypeFloat, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopTiFlashConcurrencyFactor = tidbOptFloat64(val, vardef.DefOptTiFlashConcurrencyFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCopCPUFactor, Value: strconv.FormatFloat(vardef.DefOptCopCPUFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.copCPUFactor = tidbOptFloat64(val, vardef.DefOptCopCPUFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptNetworkFactor, Value: strconv.FormatFloat(vardef.DefOptNetworkFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.networkFactor = tidbOptFloat64(val, vardef.DefOptNetworkFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptScanFactor, Value: strconv.FormatFloat(vardef.DefOptScanFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.scanFactor = tidbOptFloat64(val, vardef.DefOptScanFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDescScanFactor, Value: strconv.FormatFloat(vardef.DefOptDescScanFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.descScanFactor = tidbOptFloat64(val, vardef.DefOptDescScanFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSeekFactor, Value: strconv.FormatFloat(vardef.DefOptSeekFactor, 'f', -1, 64), skipInit: true, Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.seekFactor = tidbOptFloat64(val, vardef.DefOptSeekFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMemoryFactor, Value: strconv.FormatFloat(vardef.DefOptMemoryFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.memoryFactor = tidbOptFloat64(val, vardef.DefOptMemoryFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDiskFactor, Value: strconv.FormatFloat(vardef.DefOptDiskFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.diskFactor = tidbOptFloat64(val, vardef.DefOptDiskFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexScanCostFactor = tidbOptFloat64(val, vardef.DefOptIndexScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexReaderCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexReaderCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexReaderCostFactor = tidbOptFloat64(val, vardef.DefOptIndexReaderCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableReaderCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableReaderCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableReaderCostFactor = tidbOptFloat64(val, vardef.DefOptTableReaderCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableFullScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableFullScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableFullScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableFullScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableRangeScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableRangeScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableRangeScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableRangeScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableRowIDScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableRowIDScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableRowIDScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableRowIDScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableTiFlashScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableTiFlashScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableTiFlashScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableTiFlashScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexLookupCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexLookupCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupCostFactor = tidbOptFloat64(val, vardef.DefOptIndexLookupCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexMergeCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexMergeCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexMergeCostFactor = tidbOptFloat64(val, vardef.DefOptIndexMergeCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSortCostFactor, Value: strconv.FormatFloat(vardef.DefOptSortCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.SortCostFactor = tidbOptFloat64(val, vardef.DefOptSortCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTopNCostFactor, Value: strconv.FormatFloat(vardef.DefOptTopNCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TopNCostFactor = tidbOptFloat64(val, vardef.DefOptTopNCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptLimitCostFactor, Value: strconv.FormatFloat(vardef.DefOptLimitCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.LimitCostFactor = tidbOptFloat64(val, vardef.DefOptLimitCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptStreamAggCostFactor, Value: strconv.FormatFloat(vardef.DefOptStreamAggCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.StreamAggCostFactor = tidbOptFloat64(val, vardef.DefOptStreamAggCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptHashAggCostFactor, Value: strconv.FormatFloat(vardef.DefOptHashAggCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.HashAggCostFactor = tidbOptFloat64(val, vardef.DefOptHashAggCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMergeJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptMergeJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.MergeJoinCostFactor = tidbOptFloat64(val, vardef.DefOptMergeJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptHashJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptHashJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.HashJoinCostFactor = tidbOptFloat64(val, vardef.DefOptHashJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinCostFactor = tidbOptFloat64(val, vardef.DefOptIndexJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableNewOnlyFullGroupByCheck, Value: BoolToOnOff(vardef.DefTiDBOptimizerEnableNewOFGB), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNewOnlyFullGroupByCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(vardef.DefOptConcurrencyFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.concurrencyFactor = tidbOptFloat64(val, vardef.DefOptConcurrencyFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptForceInlineCTE, Value: BoolToOnOff(vardef.DefOptForceInlineCTE), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.enableForceInlineCTE = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexJoinBatchSize, Value: strconv.Itoa(vardef.DefIndexJoinBatchSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, vardef.DefIndexJoinBatchSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexLookupSize, Value: strconv.Itoa(vardef.DefIndexLookupSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupSize = tidbOptPositiveInt32(val, vardef.DefIndexLookupSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexLookupConcurrency, Value: strconv.Itoa(vardef.DefIndexLookupConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBIndexLookupConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexLookupJoinConcurrency, Value: strconv.Itoa(vardef.DefIndexLookupJoinConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupJoinConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBIndexLookupJoinConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(vardef.DefIndexSerialScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.indexSerialScanConcurrency = tidbOptPositiveInt32(val, vardef.DefIndexSerialScanConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipUTF8Check, Value: BoolToOnOff(vardef.DefSkipUTF8Check), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipUTF8Check = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipASCIICheck, Value: BoolToOnOff(vardef.DefSkipASCIICheck), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipASCIICheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDMLBatchSize, Value: strconv.Itoa(vardef.DefDMLBatchSize), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.DMLBatchSize = int(TidbOptInt64(val, vardef.DefDMLBatchSize))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxChunkSize, Value: strconv.Itoa(vardef.DefMaxChunkSize), Type: vardef.TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.MaxChunkSize = tidbOptPositiveInt32(val, vardef.DefMaxChunkSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowBatchCop, Value: strconv.Itoa(vardef.DefTiDBAllowBatchCop), Type: vardef.TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowBatchCop = int(TidbOptInt64(val, vardef.DefTiDBAllowBatchCop))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBShardRowIDBits, Value: strconv.Itoa(vardef.DefShardRowIDBits), Type: vardef.TypeInt, MinValue: 0, MaxValue: vardef.MaxShardRowIDBits, SetSession: func(s *SessionVars, val string) error {
		s.ShardRowIDBits = TidbOptUint64(val, vardef.DefShardRowIDBits)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPreSplitRegions, Value: strconv.Itoa(vardef.DefPreSplitRegions), Type: vardef.TypeInt, MinValue: 0, MaxValue: vardef.MaxPreSplitRegions, SetSession: func(s *SessionVars, val string) error {
		s.PreSplitRegions = TidbOptUint64(val, vardef.DefPreSplitRegions)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBInitChunkSize, Value: strconv.Itoa(vardef.DefInitChunkSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound, SetSession: func(s *SessionVars, val string) error {
		s.InitChunkSize = tidbOptPositiveInt32(val, vardef.DefInitChunkSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableCascadesPlanner, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableCascadesPlanner(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableIndexMerge, Value: BoolToOnOff(vardef.DefTiDBEnableIndexMerge), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableIndexMerge(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableTablePartition, Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, "AUTO"}, Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
		if s == vardef.Off {
			vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_table_partition is always turned on. This variable has been deprecated and will be removed in the future releases"))
		}
		return vardef.On, nil
	}},
	// Keeping tidb_enable_list_partition here, to give errors if setting it to anything other than ON
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableListTablePartition, Value: vardef.On, Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue, _ string, _ vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGenByArgs(vardef.TiDBEnableListTablePartition))
		if !TiDBOptOn(normalizedValue) {
			return normalizedValue, errors.Errorf("tidb_enable_list_partition is now always on, and cannot be turned off")
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashJoinConcurrency, Value: strconv.Itoa(vardef.DefTiDBHashJoinConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashJoinConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBHashJoinConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBProjectionConcurrency, Value: strconv.Itoa(vardef.DefTiDBProjectionConcurrency), Type: vardef.TypeInt, MinValue: -1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.projectionConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBProjectionConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashAggPartialConcurrency, Value: strconv.Itoa(vardef.DefTiDBHashAggPartialConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggPartialConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBHashAggPartialConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashAggFinalConcurrency, Value: strconv.Itoa(vardef.DefTiDBHashAggFinalConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggFinalConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBHashAggFinalConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBWindowConcurrency, Value: strconv.Itoa(vardef.DefTiDBWindowConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.windowConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBWindowConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMergeJoinConcurrency, Value: strconv.Itoa(vardef.DefTiDBMergeJoinConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.mergeJoinConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBMergeJoinConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBStreamAggConcurrency, Value: strconv.Itoa(vardef.DefTiDBStreamAggConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.streamAggConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBStreamAggConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexMergeIntersectionConcurrency, Value: strconv.Itoa(vardef.DefTiDBIndexMergeIntersectionConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexMergeIntersectionConcurrency = tidbOptPositiveInt32(val, vardef.ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBIndexMergeIntersectionConcurrency, vardef.TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableParallelApply, Value: BoolToOnOff(vardef.DefTiDBEnableParallelApply), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableParallelApply = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMemQuotaApplyCache, Value: strconv.Itoa(vardef.DefTiDBMemQuotaApplyCache), Type: vardef.TypeUnsigned, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaApplyCache = TidbOptInt64(val, vardef.DefTiDBMemQuotaApplyCache)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBackoffLockFast, Value: strconv.Itoa(tikvstore.DefBackoffLockFast), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, tikvstore.DefBackoffLockFast)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBackOffWeight, Value: strconv.Itoa(tikvstore.DefBackOffWeight), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, tikvstore.DefBackOffWeight)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnEntrySizeLimit, Value: strconv.Itoa(vardef.DefTiDBTxnEntrySizeLimit), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: config.MaxTxnEntrySizeLimit, SetSession: func(s *SessionVars, val string) error {
		s.TxnEntrySizeLimit = TidbOptUint64(val, vardef.DefTiDBTxnEntrySizeLimit)
		return nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		vardef.TxnEntrySizeLimit.Store(TidbOptUint64(val, vardef.DefTiDBTxnEntrySizeLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRetryLimit, Value: strconv.Itoa(vardef.DefTiDBRetryLimit), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RetryLimit = TidbOptInt64(val, vardef.DefTiDBRetryLimit)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDisableTxnAutoRetry, Value: BoolToOnOff(vardef.DefTiDBDisableTxnAutoRetry), Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if normalizedValue == vardef.Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(vardef.Off, vardef.On))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.DisableTxnAutoRetry = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBConstraintCheckInPlace, Value: BoolToOnOff(vardef.DefTiDBConstraintCheckInPlace), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ConstraintCheckInPlace = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnMode, Value: vardef.DefTiDBTxnMode, AllowEmptyAll: true, Type: vardef.TypeEnum, PossibleValues: []string{vardef.PessimisticTxnMode, vardef.OptimisticTxnMode}, SetSession: func(s *SessionVars, val string) error {
		s.TxnMode = strings.ToUpper(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableWindowFunction, Value: BoolToOnOff(vardef.DefEnableWindowFunction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableWindowFunction = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePipelinedWindowFunction, Value: BoolToOnOff(vardef.DefEnablePipelinedWindowFunction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePipelinedWindowExec = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(vardef.DefEnableStrictDoubleTypeCheck), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStrictDoubleTypeCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableVectorizedExpression, Value: BoolToOnOff(vardef.DefEnableVectorizedExpression), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableVectorizedExpression = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableFastAnalyze, Value: BoolToOnOff(vardef.DefTiDBUseFastAnalyze), Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if TiDBOptOn(normalizedValue) {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect"))
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFastAnalyze = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(vardef.DefTiDBSkipIsolationLevelCheck), Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableRateLimitAction, Value: BoolToOnOff(vardef.DefTiDBEnableRateLimitAction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnabledRateLimitAction = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowFallbackToTiKV, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return "", nil
		}
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		storeTypes := make(map[kv.StoreType]struct{})
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			switch {
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				if _, ok := storeTypes[kv.TiFlash]; !ok {
					if i != 0 {
						formatVal += ","
					}
					formatVal += kv.TiFlash.Name()
					storeTypes[kv.TiFlash] = struct{}{}
				}
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBAllowFallbackToTiKV, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AllowFallbackToTiKV = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			if engine == kv.TiFlash.Name() {
				s.AllowFallbackToTiKV[kv.TiFlash] = struct{}{}
			}
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAutoIncrementInGenerated, Value: BoolToOnOff(vardef.DefTiDBEnableAutoIncrementInGenerated), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAutoIncrementInGenerated = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlacementMode, Value: vardef.DefTiDBPlacementMode, Type: vardef.TypeEnum, PossibleValues: []string{vardef.PlacementModeStrict, vardef.PlacementModeIgnore}, SetSession: func(s *SessionVars, val string) error {
		s.PlacementMode = val
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptJoinReorderThreshold, Value: strconv.Itoa(vardef.DefTiDBOptJoinReorderThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 63, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, vardef.DefTiDBOptJoinReorderThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNoopFuncs, Value: vardef.DefTiDBEnableNoopFuncs, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Warn}, Depended: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		// The behavior is very weird if someone can turn TiDBEnableNoopFuncs OFF, but keep any of the following on:
		// TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, serverReadOnly, SQLAutoIsNull
		// To prevent this strange position, prevent setting to OFF when any of these sysVars are ON of the same scope.
		if normalizedValue == vardef.Off {
			for _, potentialIncompatibleSysVar := range []string{vardef.TxReadOnly, vardef.TransactionReadOnly, vardef.OfflineMode, vardef.SuperReadOnly, vardef.ReadOnly, vardef.SQLAutoIsNull} {
				val, _ := vars.GetSystemVar(potentialIncompatibleSysVar) // session scope
				if scope == vardef.ScopeGlobal {                         // global scope
					var err error
					val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(potentialIncompatibleSysVar)
					if err != nil {
						return originalValue, errUnknownSystemVariable.GenWithStackByArgs(potentialIncompatibleSysVar)
					}
				}
				if TiDBOptOn(val) {
					return originalValue, errValueNotSupportedWhen.GenWithStackByArgs(vardef.TiDBEnableNoopFuncs, potentialIncompatibleSysVar)
				}
			}
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.NoopFuncsMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBReplicaRead, Value: "leader", Type: vardef.TypeEnum, PossibleValues: []string{"leader", "prefer-leader", "follower", "leader-and-follower", "closest-replicas", "closest-adaptive", "learner"}, SetSession: func(s *SessionVars, val string) error {
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(kv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(kv.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(kv.ReplicaReadLeader)
		} else if strings.EqualFold(val, "closest-replicas") {
			s.SetReplicaRead(kv.ReplicaReadClosest)
		} else if strings.EqualFold(val, "closest-adaptive") {
			s.SetReplicaRead(kv.ReplicaReadClosestAdaptive)
		} else if strings.EqualFold(val, "learner") {
			s.SetReplicaRead(kv.ReplicaReadLearner)
		} else if strings.EqualFold(val, "prefer-leader") {
			s.SetReplicaRead(kv.ReplicaReadPreferLeader)
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAdaptiveClosestReadThreshold, Value: strconv.Itoa(vardef.DefAdaptiveClosestReadThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.ReplicaClosestReadThreshold = TidbOptInt64(val, vardef.DefAdaptiveClosestReadThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBUsePlanBaselines, Value: BoolToOnOff(vardef.DefTiDBUsePlanBaselines), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.UsePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEvolvePlanBaselines, Value: BoolToOnOff(vardef.DefTiDBEvolvePlanBaselines), Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "ON" && !config.CheckTableBeforeDrop {
			return normalizedValue, errors.Errorf("Cannot enable baseline evolution feature, it is not generally available now")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EvolvePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExtendedStats, Value: BoolToOnOff(false), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExtendedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CTEMaxRecursionDepth, Value: strconv.Itoa(vardef.DefCTEMaxRecursionDepth), Type: vardef.TypeInt, MinValue: 0, MaxValue: 4294967295, SetSession: func(s *SessionVars, val string) error {
		s.CTEMaxRecursionDepth = TidbOptInt(val, vardef.DefCTEMaxRecursionDepth)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(vardef.DefTiDBAllowAutoRandExplicitInsert), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAutoRandExplicitInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableClusteredIndex, Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.IntOnly}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == vardef.IntOnly {
			vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(normalizedValue, fmt.Sprintf("'%s' or '%s'", vardef.On, vardef.Off)))
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableClusteredIndex = vardef.TiDBOptEnableClustered(val)
		return nil
	}},
	// Keeping tidb_enable_global_index here, to give error if setting it to anything other than ON
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableGlobalIndex, Type: vardef.TypeBool, Value: vardef.On, Validation: func(vars *SessionVars, normalizedValue, _ string, _ vardef.ScopeFlag) (string, error) {
		if !TiDBOptOn(normalizedValue) {
			vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_global_index is always turned on. This variable has been deprecated and will be removed in the future releases"))
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPartitionPruneMode, Value: vardef.DefTiDBPartitionPruneMode, Type: vardef.TypeEnum, PossibleValues: []string{"static", "dynamic", "static-only", "dynamic-only"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		mode := PartitionPruneMode(normalizedValue).Update()
		if !mode.Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(vardef.TiDBPartitionPruneMode)
		}
		return string(mode), nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return s.PartitionPruneMode.Load(), nil
	}, SetSession: func(s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Static {
			s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("static prune mode is deprecated and will be removed in the future release."))
		}
		if PartitionPruneMode(s.PartitionPruneMode.Load()) == Static && PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
		}
		s.PartitionPruneMode.Store(newMode)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Static {
			s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("static prune mode is deprecated and will be removed in the future release."))
		}
		if PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRedactLog, Value: vardef.DefTiDBRedactLog, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Marker}, SetSession: func(s *SessionVars, val string) error {
		s.EnableRedactLog = val
		errors.RedactLogEnabled.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBShardAllocateStep, Value: strconv.Itoa(vardef.DefTiDBShardAllocateStep), Type: vardef.TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), SetSession: func(s *SessionVars, val string) error {
		s.ShardAllocateStep = TidbOptInt64(val, vardef.DefTiDBShardAllocateStep)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAsyncCommit, Value: BoolToOnOff(vardef.DefTiDBEnableAsyncCommit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAsyncCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnable1PC, Value: BoolToOnOff(vardef.DefTiDBEnable1PC), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable1PC = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBGuaranteeLinearizability, Value: BoolToOnOff(vardef.DefTiDBGuaranteeLinearizability), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.GuaranteeLinearizability = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeVersion, Value: strconv.Itoa(vardef.DefTiDBAnalyzeVersion), Type: vardef.TypeInt, MinValue: 1, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AnalyzeVersion = tidbOptPositiveInt32(val, vardef.DefTiDBAnalyzeVersion)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashJoinVersion, Value: vardef.DefTiDBHashJoinVersion, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			lowerValue := strings.ToLower(normalizedValue)
			if lowerValue != joinversion.HashJoinVersionLegacy && lowerValue != joinversion.HashJoinVersionOptimized {
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s", originalValue, vardef.TiDBHashJoinVersion, joinversion.HashJoinVersionLegacy+", "+joinversion.HashJoinVersionOptimized)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.UseHashJoinV2 = joinversion.IsOptimizedVersion(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableHashJoin, Value: BoolToOnOff(vardef.DefTiDBOptEnableHashJoin), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.DisableHashJoin = !TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableIndexMergeJoin, Value: BoolToOnOff(vardef.DefTiDBEnableIndexMergeJoin), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableIndexMergeJoin = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(vardef.DefTiDBTrackAggregateMemoryUsage), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TrackAggregateMemoryUsage = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMultiStatementMode, Value: vardef.Off, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Warn}, SetSession: func(s *SessionVars, val string) error {
		s.MultiStatementMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExchangePartition, Value: vardef.On, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_exchange_partition is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiDBEnableExchangePartition = true
			return nil
		}},
	// It's different from tmp_table_size or max_heap_table_size. See https://github.com/pingcap/tidb/issues/28691.
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTmpTableMaxSize, Value: strconv.Itoa(vardef.DefTiDBTmpTableMaxSize), Type: vardef.TypeUnsigned, MinValue: 1 << 20, MaxValue: 1 << 37, SetSession: func(s *SessionVars, val string) error {
		s.TMPTableSize = TidbOptInt64(val, vardef.DefTiDBTmpTableMaxSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableOrderedResultMode, Value: BoolToOnOff(vardef.DefTiDBEnableOrderedResultMode), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStableResultMode = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePseudoForOutdatedStats, Value: BoolToOnOff(vardef.DefTiDBEnablePseudoForOutdatedStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePseudoForOutdatedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRegardNULLAsPoint, Value: BoolToOnOff(vardef.DefTiDBRegardNULLAsPoint), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RegardNULLAsPoint = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePaging, Value: BoolToOnOff(vardef.DefTiDBEnablePaging), Type: vardef.TypeBool, Hidden: true, SetSession: func(s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableLegacyInstanceScope, Value: BoolToOnOff(vardef.DefEnableLegacyInstanceScope), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLegacyInstanceScope = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBStatsLoadSyncWait, Value: strconv.Itoa(vardef.DefTiDBStatsLoadSyncWait), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		SetSession: func(s *SessionVars, val string) error {
			s.StatsLoadSyncWait.Store(TidbOptInt64(val, vardef.DefTiDBStatsLoadSyncWait))
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.StatsLoadSyncWait.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.StatsLoadSyncWait.Store(TidbOptInt64(val, vardef.DefTiDBStatsLoadSyncWait))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSysdateIsNow, Value: BoolToOnOff(vardef.DefSysdateIsNow), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.SysdateIsNow = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableParallelHashaggSpill, Value: BoolToOnOff(vardef.DefTiDBEnableParallelHashaggSpill), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableParallelHashaggSpill = TiDBOptOn(s)
			if !vars.EnableParallelHashaggSpill {
				vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("tidb_enable_parallel_hashagg_spill will be removed in the future and hash aggregate spill will be enabled by default."))
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableMutationChecker, Hidden: true,
		Value: BoolToOnOff(vardef.DefTiDBEnableMutationChecker), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableMutationChecker = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnAssertionLevel, Value: vardef.DefTiDBTxnAssertionLevel, PossibleValues: []string{vardef.AssertionOffStr, vardef.AssertionFastStr, vardef.AssertionStrictStr}, Hidden: true, Type: vardef.TypeEnum, SetSession: func(s *SessionVars, val string) error {
		s.AssertionLevel = tidbOptAssertionLevel(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBatchPendingTiFlashCount, Value: strconv.Itoa(vardef.DefTiDBBatchPendingTiFlashCount), MinValue: 0, MaxValue: math.MaxUint32, Hidden: false, Type: vardef.TypeUnsigned, SetSession: func(s *SessionVars, val string) error {
		b, e := strconv.Atoi(val)
		if e != nil {
			b = vardef.DefTiDBBatchPendingTiFlashCount
		}
		s.BatchPendingTiFlashCount = b
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIgnorePreparedCacheCloseStmt, Value: BoolToOnOff(vardef.DefTiDBIgnorePreparedCacheCloseStmt), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IgnorePreparedCacheCloseStmt = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNewCostInterface, Value: BoolToOnOff(true), Hidden: false, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(vardef.Off, vardef.On))
			}
			return vardef.On, nil
		},
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableNewCostInterface = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBCostModelVersion, Value: strconv.Itoa(vardef.DefTiDBCostModelVer), Hidden: false, Type: vardef.TypeInt, MinValue: 1, MaxValue: 2,
		SetSession: func(vars *SessionVars, s string) error {
			vars.CostModelVersion = int(TidbOptInt64(s, 1))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexJoinDoubleReadPenaltyCostRate, Value: strconv.Itoa(0), Hidden: false, Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IndexJoinDoubleReadPenaltyCostRate = tidbOptFloat64(s, 0)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRCWriteCheckTs, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBRcWriteCheckTs), SetSession: func(s *SessionVars, val string) error {
		s.RcWriteCheckTS = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRemoveOrderbyInSubquery, Value: BoolToOnOff(vardef.DefTiDBRemoveOrderbyInSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RemoveOrderbyInSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMemQuotaQuery, Value: strconv.Itoa(vardef.DefTiDBMemQuotaQuery), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaQuery = TidbOptInt64(val, vardef.DefTiDBMemQuotaQuery)
		s.MemTracker.SetBytesLimit(s.MemQuotaQuery)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		intVal := TidbOptInt64(normalizedValue, vardef.DefTiDBMemQuotaQuery)
		if intVal > 0 && intVal < 128 {
			vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBMemQuotaQuery, originalValue))
			normalizedValue = "128"
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBNonTransactionalIgnoreError, Value: BoolToOnOff(vardef.DefTiDBBatchDMLIgnoreError), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.NonTransactionalIgnoreError = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFineGrainedShuffleStreamCount, Value: strconv.Itoa(vardef.DefTiFlashFineGrainedShuffleStreamCount), Type: vardef.TypeInt, MinValue: -1, MaxValue: 1024,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleStreamCount = TidbOptInt64(val, vardef.DefTiFlashFineGrainedShuffleStreamCount)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFineGrainedShuffleBatchSize, Value: strconv.Itoa(vardef.DefTiFlashFineGrainedShuffleBatchSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleBatchSize = uint64(TidbOptInt64(val, vardef.DefTiFlashFineGrainedShuffleBatchSize))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSimplifiedMetrics, Value: BoolToOnOff(vardef.DefTiDBSimplifiedMetrics), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			metrics.ToggleSimplifiedMode(TiDBOptOn(s))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMinPagingSize, Value: strconv.Itoa(vardef.DefMinPagingSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MinPagingSize = tidbOptPositiveInt32(val, vardef.DefMinPagingSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxPagingSize, Value: strconv.Itoa(vardef.DefMaxPagingSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MaxPagingSize = tidbOptPositiveInt32(val, vardef.DefMaxPagingSize)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemoryDebugModeMinHeapInUse, Value: strconv.Itoa(0), Type: vardef.TypeInt, MinValue: math.MinInt64, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeMinHeapInUse = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemoryDebugModeAlarmRatio, Value: strconv.Itoa(0), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeAlarmRatio = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.SQLRequirePrimaryKey, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PrimaryKeyRequired = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAnalyzeSnapshot, Value: BoolToOnOff(vardef.DefTiDBEnableAnalyzeSnapshot), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAnalyzeSnapshot = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGenerateBinaryPlan, Value: BoolToOnOff(vardef.DefTiDBGenerateBinaryPlan), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		GenerateBinaryPlan.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDefaultStrMatchSelectivity, Value: strconv.FormatFloat(vardef.DefTiDBDefaultStrMatchSelectivity, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.DefaultStrMatchSelectivity = tidbOptFloat64(val, vardef.DefTiDBDefaultStrMatchSelectivity)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLEnableFastReorg, Value: BoolToOnOff(vardef.DefTiDBEnableFastReorg), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableFastReorg.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableFastReorg.Store(TiDBOptOn(val))
		return nil
	}},
	// This system var is set disk quota for lightning sort dir, from 100 GB to 1PB.
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLDiskQuota, Value: strconv.Itoa(vardef.DefTiDBDDLDiskQuota), Type: vardef.TypeInt, MinValue: vardef.DefTiDBDDLDiskQuota, MaxValue: 1024 * 1024 * vardef.DefTiDBDDLDiskQuota / 100, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatUint(vardef.DDLDiskQuota.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.DDLDiskQuota.Store(TidbOptUint64(val, vardef.DefTiDBDDLDiskQuota))
		return nil
	}},
	// can't assign validate function here. Because validation function will run after GetGlobal function
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCloudStorageURI, Value: "", Type: vardef.TypeStr, GetGlobal: func(ctx context.Context, sv *SessionVars) (string, error) {
		cloudStorageURI := vardef.CloudStorageURI.Load()
		if len(cloudStorageURI) > 0 {
			cloudStorageURI = ast.RedactURL(cloudStorageURI)
		}
		return cloudStorageURI, nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		if len(val) > 0 && val != vardef.CloudStorageURI.Load() {
			if err := ValidateCloudStorageURI(ctx, val); err != nil {
				// convert annotations (second-level message) to message so clientConn.writeError
				// will print friendly error.
				if goerr.As(err, new(*errors.Error)) {
					err = errors.New(err.Error())
				}
				return err
			}
		}
		vardef.CloudStorageURI.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBConstraintCheckInPlacePessimistic, Value: BoolToOnOff(config.GetGlobalConfig().PessimisticTxn.ConstraintCheckInPlacePessimistic), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.ConstraintCheckInPlacePessimistic = TiDBOptOn(val)
			if !s.ConstraintCheckInPlacePessimistic {
				metrics.LazyPessimisticUniqueCheckSetCount.Inc()
			}
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableTiFlashReadForWriteStmt, Value: vardef.On, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_tiflash_read_for_write_stmt is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableTiFlashReadForWriteStmt = true
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableUnsafeSubstitute, Value: BoolToOnOff(false), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableUnsafeSubstitute = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRangeMaxSize, Value: strconv.FormatInt(vardef.DefTiDBOptRangeMaxSize, 10), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RangeMaxSize = TidbOptInt64(val, vardef.DefTiDBOptRangeMaxSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptAdvancedJoinHint, Value: BoolToOnOff(vardef.DefTiDBOptAdvancedJoinHint), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAdvancedJoinHint = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptUseInvisibleIndexes, Value: BoolToOnOff(false), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerUseInvisibleIndexes = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:     vardef.TiDBAnalyzePartitionConcurrency,
		Value:    strconv.FormatInt(vardef.DefTiDBAnalyzePartitionConcurrency, 10),
		Type:     vardef.TypeInt,
		MinValue: 1,
		MaxValue: 128,
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionConcurrency = int(TidbOptInt64(val, vardef.DefTiDBAnalyzePartitionConcurrency))
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMergePartitionStatsConcurrency, Value: strconv.FormatInt(vardef.DefTiDBMergePartitionStatsConcurrency, 10), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency,
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionMergeConcurrency = TidbOptInt(val, vardef.DefTiDBMergePartitionStatsConcurrency)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAsyncMergeGlobalStats, Value: BoolToOnOff(vardef.DefTiDBEnableAsyncMergeGlobalStats), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableAsyncMergeGlobalStats = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptPrefixIndexSingleScan, Value: BoolToOnOff(vardef.DefTiDBOptPrefixIndexSingleScan), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptPrefixIndexSingleScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBExternalTS, Value: strconv.FormatInt(vardef.DefTiDBExternalTS, 10), SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		ts, err := parseTSFromNumberOrTime(s, val)
		if err != nil {
			return err
		}
		return SetExternalTimestamp(ctx, ts)
	}, GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
		ts, err := GetExternalTimestamp(ctx)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(ts)), err
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExternalTSRead, Value: BoolToOnOff(vardef.DefTiDBEnableExternalTSRead), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExternalTSRead = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableReusechunk, Value: BoolToOnOff(vardef.DefTiDBEnableReusechunk), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableReuseChunk = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBIgnoreInlistPlanDigest, Value: BoolToOnOff(vardef.DefTiDBIgnoreInlistPlanDigest), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.IgnoreInlistPlanDigest.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.IgnoreInlistPlanDigest.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobEnable, Value: BoolToOnOff(vardef.DefTiDBTTLJobEnable), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.EnableTTLJob.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTTLJob.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLScanBatchSize, Value: strconv.Itoa(vardef.DefTiDBTTLScanBatchSize), Type: vardef.TypeInt, MinValue: vardef.DefTiDBTTLScanBatchMinSize, MaxValue: vardef.DefTiDBTTLScanBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLScanBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLScanBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteBatchSize, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteBatchSize), Type: vardef.TypeInt, MinValue: vardef.DefTiDBTTLDeleteBatchMinSize, MaxValue: vardef.DefTiDBTTLDeleteBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLDeleteBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLDeleteBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteRateLimit, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteRateLimit), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLDeleteRateLimit.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLDeleteRateLimit.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBStoreBatchSize, Value: strconv.FormatInt(vardef.DefTiDBStoreBatchSize, 10),
		Type: vardef.TypeInt, MinValue: 0, MaxValue: 25000, SetSession: func(s *SessionVars, val string) error {
			s.StoreBatchSize = TidbOptInt(val, vardef.DefTiDBStoreBatchSize)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MppExchangeCompressionMode, Type: vardef.TypeStr, Value: vardef.DefaultExchangeCompressionMode.Name(),
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := vardef.ToExchangeCompressionMode(normalizedValue)
			if !ok {
				var msg string
				for m := vardef.ExchangeCompressionModeNONE; m <= vardef.ExchangeCompressionModeUnspecified; m += 1 {
					if m == 0 {
						msg = m.Name()
					} else {
						msg = fmt.Sprintf("%s, %s", msg, m.Name())
					}
				}
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s",
					originalValue,
					vardef.MppExchangeCompressionMode, msg)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.mppExchangeCompressionMode, _ = vardef.ToExchangeCompressionMode(val)
			if s.ChooseMppVersion() == kv.MppVersionV0 && s.mppExchangeCompressionMode != vardef.ExchangeCompressionModeUnspecified {
				s.StmtCtx.AppendWarning(fmt.Errorf("mpp exchange compression won't work under current mpp version %d", kv.MppVersionV0))
			}

			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MppVersion, Type: vardef.TypeStr, Value: kv.MppVersionUnspecifiedName,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := kv.ToMppVersion(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %d (unspecified)",
				originalValue, vardef.MppVersion, kv.MppVersionUnspecified)
			for i := kv.MppVersionV0; i <= kv.GetNewestMppVersion(); i += 1 {
				errMsg = fmt.Sprintf("%s, %d", errMsg, i)
			}

			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			version, _ := kv.ToMppVersion(val)
			s.mppVersion = version
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobScheduleWindowStartTime, Value: vardef.DefTiDBTTLJobScheduleWindowStartTime, Type: vardef.TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			startTime, err := time.ParseInLocation(vardef.FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			vardef.TTLJobScheduleWindowStartTime.Store(startTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			startTime := vardef.TTLJobScheduleWindowStartTime.Load()
			return startTime.Format(vardef.FullDayTimeFormat), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobScheduleWindowEndTime, Value: vardef.DefTiDBTTLJobScheduleWindowEndTime, Type: vardef.TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			endTime, err := time.ParseInLocation(vardef.FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			vardef.TTLJobScheduleWindowEndTime.Store(endTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			endTime := vardef.TTLJobScheduleWindowEndTime.Load()
			return endTime.Format(vardef.FullDayTimeFormat), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLScanWorkerCount, Value: strconv.Itoa(vardef.DefTiDBTTLScanWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			vardef.TTLScanWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(vardef.TTLScanWorkerCount.Load())), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteWorkerCount, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			vardef.TTLDeleteWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(vardef.TTLDeleteWorkerCount.Load())), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableResourceControl, Value: BoolToOnOff(vardef.DefTiDBEnableResourceControl), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		if TiDBOptOn(s) != vardef.EnableResourceControl.Load() {
			vardef.EnableResourceControl.Store(TiDBOptOn(s))
			(*SetGlobalResourceControl.Load())(TiDBOptOn(s))
			logutil.BgLogger().Info("set resource control", zap.Bool("enable", TiDBOptOn(s)))
		}
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableResourceControl.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBResourceControlStrictMode, Value: BoolToOnOff(vardef.DefTiDBResourceControlStrictMode), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		opOn := TiDBOptOn(s)
		if opOn != vardef.EnableResourceControlStrictMode.Load() {
			vardef.EnableResourceControlStrictMode.Store(opOn)
			logutil.BgLogger().Info("change resource control strict mode", zap.Bool("enable", TiDBOptOn(s)))
		}
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableResourceControlStrictMode.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPessimisticTransactionFairLocking, Value: BoolToOnOff(vardef.DefTiDBPessimisticTransactionFairLocking), Type: vardef.TypeBool,
		Validation: func(_ *SessionVars, val string, _ string, _ vardef.ScopeFlag) (string, error) {
			if kerneltype.IsNextGen() && TiDBOptOn(val) {
				return vardef.Off, errNotSupportedInNextGen.FastGenByArgs(vardef.TiDBPessimisticTransactionFairLocking)
			}
			return val, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.PessimisticTransactionFairLocking = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanCacheForParamLimit, Value: BoolToOnOff(vardef.DefTiDBEnablePlanCacheForParamLimit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForParamLimit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableINLJoinInnerMultiPattern, Value: BoolToOnOff(vardef.DefTiDBEnableINLJoinMultiPattern), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableINLJoinInnerMultiPattern = TiDBOptOn(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return BoolToOnOff(s.EnableINLJoinInnerMultiPattern), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashComputeDispatchPolicy, Value: string(vardef.DefTiFlashComputeDispatchPolicy), Type: vardef.TypeStr, SetSession: setTiFlashComputeDispatchPolicy,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			return setTiFlashComputeDispatchPolicy(vars, s)
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanCacheForSubquery, Value: BoolToOnOff(vardef.DefTiDBEnablePlanCacheForSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableLateMaterialization, Value: BoolToOnOff(vardef.DefTiDBOptEnableLateMaterialization), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLateMaterialization = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBLoadBasedReplicaReadThreshold, Value: vardef.DefTiDBLoadBasedReplicaReadThreshold.String(), Type: vardef.TypeDuration, MaxValue: uint64(time.Hour), SetSession: func(s *SessionVars, val string) error {
		d, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		s.LoadBasedReplicaReadThreshold = d
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLRunningTasks, Value: strconv.Itoa(vardef.DefTiDBTTLRunningTasks), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLRunningTasks.Store(int32(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(int(vardef.TTLRunningTasks.Load())), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptOrderingIdxSelThresh, Value: strconv.FormatFloat(vardef.DefTiDBOptOrderingIdxSelThresh, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelThresh = tidbOptFloat64(val, vardef.DefTiDBOptOrderingIdxSelThresh)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptOrderingIdxSelRatio, Value: strconv.FormatFloat(vardef.DefTiDBOptOrderingIdxSelRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: -1, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelRatio = tidbOptFloat64(val, vardef.DefTiDBOptOrderingIdxSelRatio)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableMPPSharedCTEExecution, Value: BoolToOnOff(vardef.DefTiDBOptEnableMPPSharedCTEExecution), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableMPPSharedCTEExecution = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptFixControl,
		Value:                   "",
		Type:                    vardef.TypeStr,
		IsHintUpdatableVerified: true,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			// validation logic for setting global
			// we don't put this in Validation to avoid repeating the checking logic for setting session.
			_, warnings, err := fixcontrol.ParseToMap(val)
			if err != nil {
				return err
			}
			for _, warning := range warnings {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError(warning))
			}
			return nil
		},
		SetSession: func(s *SessionVars, val string) error {
			newMap, warnings, err := fixcontrol.ParseToMap(val)
			if err != nil {
				return err
			}
			for _, warning := range warnings {
				s.StmtCtx.AppendWarning(errors.NewNoStackError(warning))
			}
			s.OptimizerFixControl = newMap
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeSkipColumnTypes, Value: "json,blob,mediumblob,longblob,mediumtext,longtext", Type: vardef.TypeStr,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			return ValidAnalyzeSkipColumnTypes(normalizedValue)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzeSkipColumnTypes = ParseAnalyzeSkipColumnTypes(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlanCacheInvalidationOnFreshStats, Value: BoolToOnOff(vardef.DefTiDBPlanCacheInvalidationOnFreshStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PlanCacheInvalidationOnFreshStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashReplicaRead, Value: vardef.DefTiFlashReplicaRead, Type: vardef.TypeEnum, PossibleValues: []string{vardef.DefTiFlashReplicaRead, vardef.ClosestAdaptiveStr, vardef.ClosestReplicasStr},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashReplicaRead = tiflash.GetTiFlashReplicaReadByStr(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return tiflash.GetTiFlashReplicaRead(s.TiFlashReplicaRead), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBFastCheckTable, Value: BoolToOnOff(vardef.DefTiDBEnableFastCheckTable), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.FastCheckTable = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipMissingPartitionStats, Value: BoolToOnOff(vardef.DefTiDBSkipMissingPartitionStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipMissingPartitionStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLAuthMethodName, Value: vardef.DefAuthenticationLDAPSASLAuthMethodName, Type: vardef.TypeEnum, PossibleValues: []string{ldap.SASLAuthMethodSCRAMSHA1, ldap.SASLAuthMethodSCRAMSHA256, ldap.SASLAuthMethodGSSAPI}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetSASLAuthMethod(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSASLAuthMethod(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLServerHost, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLServerPort, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLServerPort), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLTLS, Value: BoolToOnOff(vardef.DefAuthenticationLDAPSASLTLS), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSASLAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLCAPath, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSASLAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetCAPath(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLUserSearchAttr, Value: vardef.DefAuthenticationLDAPSASLUserSearchAttr, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindBaseDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindRootDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindRootPWD, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSASLAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return vardef.MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLInitPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLInitPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLMaxPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLMaxPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetMaxCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetMaxCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleAuthMethodName, Value: vardef.DefAuthenticationLDAPSimpleAuthMethodName, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		s = strings.ToUpper(s)
		// Only "SIMPLE" is supported
		if s != "SIMPLE" {
			return errors.Errorf("auth method %s is not supported", s)
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleServerHost, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleServerPort, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleServerPort), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleTLS, Value: BoolToOnOff(vardef.DefAuthenticationLDAPSimpleTLS), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSimpleAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleCAPath, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSimpleAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetCAPath(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleUserSearchAttr, Value: vardef.DefAuthenticationLDAPSimpleUserSearchAttr, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindBaseDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindRootDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindRootPWD, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSimpleAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return vardef.MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleInitPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleInitPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleMaxPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleMaxPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetMaxCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetMaxCapacity()), nil
	}},
	// runtime filter variables group
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRuntimeFilterTypeName, Value: vardef.DefRuntimeFilterType, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := ToRuntimeFilterType(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s should be sepreated by , such as %s, also we only support IN and MIN_MAX now. ",
				originalValue, vardef.TiDBRuntimeFilterTypeName, vardef.DefRuntimeFilterType)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterTypes, _ = ToRuntimeFilterType(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRuntimeFilterModeName, Value: vardef.DefRuntimeFilterMode, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := RuntimeFilterModeStringToMode(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %s ",
				originalValue, vardef.TiDBRuntimeFilterModeName, vardef.DefRuntimeFilterMode)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterMode, _ = RuntimeFilterModeStringToMode(val)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:  vardef.TiDBLockUnchangedKeys,
		Value: BoolToOnOff(vardef.DefTiDBLockUnchangedKeys),
		Type:  vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.LockUnchangedKeys = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableCheckConstraint, Value: BoolToOnOff(vardef.DefTiDBEnableCheckConstraint), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.EnableCheckConstraint.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableCheckConstraint.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSchemaCacheSize, Value: strconv.Itoa(vardef.DefTiDBSchemaCacheSize), Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			_, str, err := parseSchemaCacheSize(s, normalizedValue, originalValue)
			if err != nil {
				return "", err
			}
			return str, nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			// It does not take effect immediately, but within a ddl lease, infoschema reload would cause the v2 to be used.
			bt, str, err := parseSchemaCacheSize(vars, val, val)
			if err != nil {
				return err
			}
			if vardef.SchemaCacheSize.Load() != bt && ChangeSchemaCacheSize != nil {
				if err := ChangeSchemaCacheSize(ctx, bt); err != nil {
					return err
				}
			}
			vardef.SchemaCacheSize.Store(bt)
			vardef.SchemaCacheSizeOriginText.Store(str)
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.SchemaCacheSizeOriginText.Load(), nil
		}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSessionAlias, Value: "", Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			chars := []rune(normalizedValue)
			warningAdded := false
			if len(chars) > 64 {
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBSessionAlias, originalValue))
				warningAdded = true
				chars = chars[:64]
				normalizedValue = string(chars)
			}

			// truncate to a valid identifier
			for normalizedValue != "" && util.IsInCorrectIdentifierName(normalizedValue) {
				if !warningAdded {
					s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBSessionAlias, originalValue))
					warningAdded = true
				}
				chars = chars[:len(chars)-1]
				normalizedValue = string(chars)
			}

			return normalizedValue, nil
		},
		SetSession: func(vars *SessionVars, s string) error {
			vars.SessionAlias = s
			return nil
		}, GetSession: func(vars *SessionVars) (string, error) {
			return vars.SessionAlias, nil
		}},
	{
		Scope:          vardef.ScopeGlobal | vardef.ScopeSession,
		Name:           vardef.TiDBOptObjective,
		Value:          vardef.DefTiDBOptObjective,
		Type:           vardef.TypeEnum,
		PossibleValues: []string{vardef.OptObjectiveModerate, vardef.OptObjectiveDeterminate},
		SetSession: func(vars *SessionVars, s string) error {
			vars.OptObjective = s
			return nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBServiceScope, Value: "", Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			return normalizedValue, servicescope.CheckServiceScope(originalValue)
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			newValue := strings.ToLower(s)
			vardef.ServiceScope.Store(newValue)
			oldConfig := config.GetGlobalConfig()
			if oldConfig.Instance.TiDBServiceScope != newValue {
				newConfig := *oldConfig
				newConfig.Instance.TiDBServiceScope = newValue
				config.StoreGlobalConfig(&newConfig)
			}
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.ServiceScope.Load(), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSchemaVersionCacheLimit, Value: strconv.Itoa(vardef.DefTiDBSchemaVersionCacheLimit), Type: vardef.TypeInt, MinValue: 2, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.SchemaVersionCacheLimit.Store(TidbOptInt64(val, vardef.DefTiDBSchemaVersionCacheLimit))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIdleTransactionTimeout, Value: strconv.Itoa(vardef.DefTiDBIdleTransactionTimeout), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear,
		SetSession: func(s *SessionVars, val string) error {
			s.IdleTransactionTimeout = tidbOptPositiveInt32(val, vardef.DefTiDBIdleTransactionTimeout)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DivPrecisionIncrement, Value: strconv.Itoa(vardef.DefDivPrecisionIncrement), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 30,
		SetSession: func(s *SessionVars, val string) error {
			s.DivPrecisionIncrement = tidbOptPositiveInt32(val, vardef.DefDivPrecisionIncrement)
			return nil
		}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBDMLType, Value: vardef.DefTiDBDMLType, Type: vardef.TypeStr,
		SetSession: func(s *SessionVars, val string) error {
			lowerVal := strings.ToLower(val)
			if strings.EqualFold(lowerVal, "standard") {
				s.BulkDMLEnabled = false
				return nil
			}
			if strings.EqualFold(lowerVal, "bulk") {
				s.BulkDMLEnabled = true
				return nil
			}
			return errors.Errorf("unsupport DML type: %s", val)
		},
		IsHintUpdatableVerified: true,
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBCreateFromSelectUsingImport, Value: "0", Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.CreateFromSelectUsingImport = TiDBOptOn(val)
			return nil
		},
		IsHintUpdatableVerified: true,
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashHashAggPreAggMode, Value: vardef.DefTiFlashPreAggMode, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			if _, ok := ToTiPBTiFlashPreAggMode(normalizedValue); ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: `%s`. %s options: %s",
				originalValue, vardef.TiFlashHashAggPreAggMode, ValidTiFlashPreAggMode())
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashPreAggMode = val
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableLazyCursorFetch, Value: BoolToOnOff(vardef.DefTiDBEnableLazyCursorFetch), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLazyCursorFetch = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableSharedLockPromotion, Value: BoolToOnOff(vardef.DefTiDBEnableSharedLockPromotion), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		if s.NoopFuncsMode != OffInt && TiDBOptOn(val) {
			logutil.BgLogger().Warn("tidb_enable_shared_lock_promotion set to on would override tidb_enable_noop_functions on")
		}
		s.SharedLockPromotion = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxDistTaskNodes, Value: strconv.Itoa(vardef.DefTiDBMaxDistTaskNodes), Type: vardef.TypeInt, MinValue: -1, MaxValue: 128,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			maxNodes := TidbOptInt(normalizedValue, vardef.DefTiDBMaxDistTaskNodes)
			if maxNodes == 0 {
				return normalizedValue, errors.New("max_dist_task_nodes should be -1 or [1, 128]")
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTSOClientRPCMode, Value: vardef.DefTiDBTSOClientRPCMode, Type: vardef.TypeEnum, PossibleValues: []string{vardef.TSOClientRPCModeDefault, vardef.TSOClientRPCModeParallel, vardef.TSOClientRPCModeParallelFast},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return (*SetPDClientDynamicOption.Load())(vardef.TiDBTSOClientRPCMode, val)
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCircuitBreakerPDMetadataErrorRateThresholdPct, Value: strconv.Itoa(vardef.DefTiDBCircuitBreakerPDMetaErrorRatePct), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 100,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			if ChangePDMetadataCircuitBreakerErrorRateThresholdPct != nil {
				ChangePDMetadataCircuitBreakerErrorRateThresholdPct(uint32(tidbOptPositiveInt32(val, vardef.DefTiDBCircuitBreakerPDMetaErrorRatePct)))
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAccelerateUserCreationUpdate, Value: BoolToOnOff(vardef.DefTiDBAccelerateUserCreationUpdate), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.AccelerateUserCreationUpdate.Store(TiDBOptOn(val))
			return nil
		},
	},
	{
		Scope:      vardef.ScopeGlobal | vardef.ScopeSession,
		Name:       vardef.TiDBPipelinedDmlResourcePolicy,
		Value:      vardef.DefTiDBPipelinedDmlResourcePolicy,
		Type:       vardef.TypeStr,
		SetSession: setPipelinedDmlResourcePolicy,
		// because the special character in custom syntax cannot be correctly handled in set_var hint
		IsHintUpdatableVerified: true,
	},
}

// GlobalSystemVariableInitialValue gets the default value for a system variable including ones that are dynamically set (e.g. based on the store)
func GlobalSystemVariableInitialValue(varName, varVal string) string {
	switch varName {
	case vardef.TiDBEnableAsyncCommit, vardef.TiDBEnable1PC:
		if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
			varVal = vardef.On
		}
	case vardef.TiDBMemOOMAction:
		if intest.InTest {
			varVal = vardef.OOMActionLog
		}
	case vardef.TiDBEnableAutoAnalyze:
		if intest.InTest {
			varVal = vardef.Off
		}
	// For the following sysvars, we change the default
	// FOR NEW INSTALLS ONLY. In most cases you don't want to do this.
	// It is better to change the value in the Sysvar struct, so that
	// all installs will have the same value.
	case vardef.TiDBRowFormatVersion:
		varVal = strconv.Itoa(vardef.DefTiDBRowFormatV2)
	case vardef.TiDBTxnAssertionLevel:
		varVal = vardef.AssertionFastStr
	case vardef.TiDBEnableMutationChecker:
		varVal = vardef.On
	case vardef.TiDBPessimisticTransactionFairLocking:
		if kerneltype.IsNextGen() {
			varVal = vardef.Off
		} else {
			varVal = vardef.On
		}
	}
	return varVal
}

func setTiFlashComputeDispatchPolicy(s *SessionVars, val string) error {
	p, err := tiflashcompute.GetDispatchPolicyByStr(val)
	if err != nil {
		return err
	}
	s.TiFlashComputeDispatchPolicy = p
	return nil
}

func setPipelinedDmlResourcePolicy(s *SessionVars, val string) error {
	// ensure the value is trimmed and lowercased
	val = strings.TrimSpace(val)
	lowVal := strings.ToLower(val)
	switch lowVal {
	case vardef.StrategyStandard:
		s.PipelinedDMLConfig.PipelinedFlushConcurrency = vardef.DefaultFlushConcurrency
		s.PipelinedDMLConfig.PipelinedResolveLockConcurrency = vardef.DefaultResolveConcurrency
		s.PipelinedDMLConfig.PipelinedWriteThrottleRatio = 0
	case vardef.StrategyConservative:
		s.PipelinedDMLConfig.PipelinedFlushConcurrency = vardef.ConservativeFlushConcurrency
		s.PipelinedDMLConfig.PipelinedResolveLockConcurrency = vardef.ConservativeResolveConcurrency
		s.PipelinedDMLConfig.PipelinedWriteThrottleRatio = 0
	default:
		// Create a temporary config to hold new values to avoid partial application
		newConfig := PipelinedDMLConfig{
			PipelinedFlushConcurrency:       vardef.DefaultFlushConcurrency,
			PipelinedResolveLockConcurrency: vardef.DefaultResolveConcurrency,
			PipelinedWriteThrottleRatio:     0,
		}

		// More flexible custom format validation
		if !strings.HasPrefix(lowVal, vardef.StrategyCustom) {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Extract everything after "custom"
		remaining := strings.TrimSpace(lowVal[len(vardef.StrategyCustom):])
		if len(remaining) < 2 || !strings.HasPrefix(remaining, "{") || !strings.HasSuffix(remaining, "}") {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Extract and trim content between brackets
		content := strings.TrimSpace(remaining[1 : len(remaining)-1])
		if content == "" {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Split parameters
		rawParams := strings.Split(content, ",")
		for _, rawParam := range rawParams {
			param := strings.TrimSpace(rawParam)
			if param == "" {
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}

			// Split key-values
			parts := strings.FieldsFunc(param, func(r rune) bool {
				return r == '=' || r == ':'
			})

			if len(parts) != 2 {
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}

			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			switch key {
			case "concurrency":
				concurrency, err := strconv.ParseInt(value, 10, 64)
				if err != nil || concurrency < vardef.MinPipelinedDMLConcurrency || concurrency > vardef.MaxPipelinedDMLConcurrency {
					logutil.BgLogger().Warn(
						"invalid concurrency value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("concurrency", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedFlushConcurrency = int(concurrency)
			case "resolve_concurrency":
				concurrency, err := strconv.ParseInt(value, 10, 64)
				if err != nil || concurrency < vardef.MinPipelinedDMLConcurrency || concurrency > vardef.MaxPipelinedDMLConcurrency {
					logutil.BgLogger().Warn(
						"invalid resolve_concurrency value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("resolve_concurrency", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedResolveLockConcurrency = int(concurrency)
			case "write_throttle_ratio":
				ratio, err := strconv.ParseFloat(value, 64)
				if err != nil || ratio < 0 || ratio >= 1 {
					logutil.BgLogger().Warn(
						"invalid write_throttle_ratio value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("write_throttle_ratio", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedWriteThrottleRatio = ratio
			default:
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}
		}

		// Only apply changes after all validation passed
		s.PipelinedDMLConfig = newConfig
	}
	return nil
}
