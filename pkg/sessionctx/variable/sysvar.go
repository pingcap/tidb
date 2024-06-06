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
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
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
	{Scope: ScopeNone, Name: SystemTimeZone, Value: "CST"},
	{Scope: ScopeNone, Name: Hostname, Value: DefHostname},
	{Scope: ScopeNone, Name: Port, Value: "4000", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: ScopeNone, Name: LogBin, Value: Off, Type: TypeBool},
	{Scope: ScopeNone, Name: VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 8.0 compatible"},
	{Scope: ScopeNone, Name: Version, Value: mysql.ServerVersion},
	{Scope: ScopeNone, Name: DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: ScopeNone, Name: Socket, Value: ""},
	{Scope: ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: ScopeNone, Name: "have_ssl", Value: "DISABLED", Type: TypeBool},
	{Scope: ScopeNone, Name: "have_openssl", Value: "DISABLED", Type: TypeBool},
	{Scope: ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: ScopeNone, Name: "version_compile_os", Value: runtime.GOOS},
	{Scope: ScopeNone, Name: "version_compile_machine", Value: runtime.GOARCH},
	/* TiDB specific variables */
	{Scope: ScopeNone, Name: TiDBEnableEnhancedSecurity, Value: Off, Type: TypeBool},
	{Scope: ScopeNone, Name: TiDBAllowFunctionForExpressionIndex, ReadOnly: true, Value: collectAllowFuncName4ExpressionIndex()},

	/* The system variables below have SESSION scope  */
	{Scope: ScopeSession, Name: Timestamp, Value: DefTimestamp, MinValue: 0, MaxValue: math.MaxInt32, Type: TypeFloat, GetSession: func(s *SessionVars) (string, error) {
		if timestamp, ok := s.systems[Timestamp]; ok && timestamp != DefTimestamp {
			return timestamp, nil
		}
		timestamp := s.StmtCtx.GetOrStoreStmtCache(stmtctx.StmtNowTsCacheKey, time.Now()).(time.Time)
		return types.ToString(float64(timestamp.UnixNano()) / float64(time.Second))
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		timestamp, ok := s.systems[Timestamp]
		return timestamp, ok && timestamp != DefTimestamp, nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		val := tidbOptFloat64(originalValue, DefTimestampFloat)
		if val > math.MaxInt32 {
			return originalValue, ErrWrongValueForVar.GenWithStackByArgs(Timestamp, originalValue)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeSession, Name: WarningCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(s.SysWarningCount), nil
	}},
	{Scope: ScopeSession, Name: ErrorCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(int(s.SysErrorCount)), nil
	}},
	{Scope: ScopeSession, Name: LastInsertID, Value: "0", Type: TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	{Scope: ScopeSession, Name: Identity, Value: "0", Type: TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	/* TiDB specific variables */
	// TODO: TiDBTxnScope is hidden because local txn feature is not done.
	{Scope: ScopeSession, Name: TiDBTxnScope, skipInit: true, Hidden: true, Value: kv.GlobalTxnScope, SetSession: func(s *SessionVars, val string) error {
		switch val {
		case kv.GlobalTxnScope:
			s.TxnScope = kv.NewGlobalTxnScopeVar()
		case kv.LocalTxnScope:
			if !EnableLocalTxn.Load() {
				return ErrWrongValueForVar.GenWithStack("@@txn_scope can not be set to local when tidb_enable_local_txn is off")
			}
			txnScope := config.GetTxnScopeFromConfig()
			if txnScope == kv.GlobalTxnScope {
				return ErrWrongValueForVar.GenWithStack("@@txn_scope can not be set to local when zone label is empty or \"global\"")
			}
			s.TxnScope = kv.NewLocalTxnScopeVar(txnScope)
		default:
			return ErrWrongValueForVar.GenWithStack("@@txn_scope value should be global or local")
		}
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return s.TxnScope.GetVarValue(), nil
	}},
	{Scope: ScopeSession, Name: TiDBTxnReadTS, Value: "", Hidden: true, SetSession: func(s *SessionVars, val string) error {
		return setTxnReadTS(s, val)
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return normalizedValue, nil
	}},
	{Scope: ScopeSession, Name: TiDBReadStaleness, Value: strconv.Itoa(DefTiDBReadStaleness), Type: TypeInt, MinValue: math.MinInt32, MaxValue: 0, AllowEmpty: true, Hidden: false, SetSession: func(s *SessionVars, val string) error {
		return setReadStaleness(s, val)
	}},
	{Scope: ScopeSession, Name: TiDBEnforceMPPExecution, Type: TypeBool, Value: BoolToOnOff(config.GetGlobalConfig().Performance.EnforceMPP), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && !vars.allowMPPExecution {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("tidb_enforce_mpp", "1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first.")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.enforceMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxTiFlashThreads, Type: TypeInt, Value: strconv.Itoa(DefTiFlashMaxThreads), MinValue: -1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxThreads = TidbOptInt64(val, DefTiFlashMaxThreads)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxBytesBeforeTiFlashExternalJoin, Type: TypeInt, Value: strconv.Itoa(DefTiFlashMaxBytesBeforeExternalJoin), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalJoin = TidbOptInt64(val, DefTiFlashMaxBytesBeforeExternalJoin)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxBytesBeforeTiFlashExternalGroupBy, Type: TypeInt, Value: strconv.Itoa(DefTiFlashMaxBytesBeforeExternalGroupBy), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalGroupBy = TidbOptInt64(val, DefTiFlashMaxBytesBeforeExternalGroupBy)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxBytesBeforeTiFlashExternalSort, Type: TypeInt, Value: strconv.Itoa(DefTiFlashMaxBytesBeforeExternalSort), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalSort = TidbOptInt64(val, DefTiFlashMaxBytesBeforeExternalSort)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashMemQuotaQueryPerNode, Type: TypeInt, Value: strconv.Itoa(DefTiFlashMemQuotaQueryPerNode), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxQueryMemoryPerNode = TidbOptInt64(val, DefTiFlashMemQuotaQueryPerNode)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashQuerySpillRatio, Type: TypeFloat, Value: strconv.FormatFloat(DefTiFlashQuerySpillRatio, 'f', -1, 64), MinValue: 0, MaxValue: 1, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, flag ScopeFlag) (string, error) {
		val, err := strconv.ParseFloat(normalizedValue, 64)
		if err != nil {
			return "", err
		}
		if val > 0.85 || val < 0 {
			return "", errors.New("The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashQuerySpillRatio = tidbOptFloat64(val, DefTiFlashQuerySpillRatio)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableTiFlashPipelineMode, Type: TypeBool, Value: BoolToOnOff(DefTiDBEnableTiFlashPipelineMode), SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		TiFlashEnablePipelineMode.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(TiFlashEnablePipelineMode.Load()), nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGenByArgs(TiDBEnableTiFlashPipelineMode))
		return normalizedValue, nil
	}},
	{Scope: ScopeSession, Name: TiDBSnapshot, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptProjectionPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowProjectionPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDeriveTopN, Value: BoolToOnOff(DefOptDeriveTopN), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDeriveTopN = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptAggPushDown, Value: BoolToOnOff(DefOptAggPushDown), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptSkewDistinctAgg, Value: BoolToOnOff(DefTiDBSkewDistinctAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableSkewDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOpt3StageDistinctAgg, Value: BoolToOnOff(DefTiDB3StageDistinctAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptEnable3StageMultiDistinctAgg, Value: BoolToOnOff(DefTiDB3StageMultiDistinctAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageMultiDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptExplainNoEvaledSubQuery, Value: BoolToOnOff(DefTiDBOptExplainEvaledSubquery), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ExplainNonEvaledSubQuery = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptWriteRowID, Value: BoolToOnOff(DefOptWriteRowID), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowWriteRowID = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBChecksumTableConcurrency, Value: strconv.Itoa(DefChecksumTableConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency},
	{Scope: ScopeSession, Name: TiDBBatchInsert, Value: BoolToOnOff(DefBatchInsert), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchDelete, Value: BoolToOnOff(DefBatchDelete), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchDelete = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchCommit, Value: BoolToOnOff(DefBatchCommit), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBCurrentTS, Value: strconv.Itoa(DefCurretTS), Type: TypeInt, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxInt64, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.TxnCtx.StartTS, 10), nil
	}},
	{Scope: ScopeSession, Name: TiDBLastTxnInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return s.LastTxnInfo, nil
	}},
	{Scope: ScopeSession, Name: TiDBLastQueryInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastQueryInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableChunkRPC, Value: BoolToOnOff(config.GetGlobalConfig().TiKVClient.EnableChunkRPC), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChunkRPC = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TxnIsolationOneShot, Value: "", skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
	{Scope: ScopeSession, Name: TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(DefTiDBOptimizerSelectivityLevel), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptimizerEnableOuterJoinReorder, Value: BoolToOnOff(DefTiDBEnableOuterJoinReorder), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableOuterJoinReorder = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptimizerEnableNAAJ, Value: BoolToOnOff(DefTiDBEnableNAAJ), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNAAJ = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBDDLReorgPriority, Value: "PRIORITY_LOW", Type: TypeEnum, skipInit: true, PossibleValues: []string{"PRIORITY_LOW", "PRIORITY_NORMAL", "PRIORITY_HIGH"}, SetSession: func(s *SessionVars, val string) error {
		s.setDDLReorgPriority(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBSlowQueryFile, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.SlowQueryFile = val
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionFinish, Value: BoolToOnOff(DefTiDBWaitSplitRegionFinish), skipInit: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionFinish = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(DefWaitSplitRegionTimeout), skipInit: true, Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, DefWaitSplitRegionTimeout))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBLowResolutionTSO, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.LowResolutionTSO = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBAllowRemoveAutoInc, Value: BoolToOnOff(DefTiDBAllowRemoveAutoInc), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowRemoveAutoInc = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBIsolationReadEngines, Value: strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ","), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(TiDBIsolationReadEngines, normalizedValue)
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
	{Scope: ScopeSession, Name: TiDBMetricSchemaStep, Value: strconv.Itoa(DefTiDBMetricSchemaStep), Type: TypeUnsigned, skipInit: true, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaStep = TidbOptInt64(val, DefTiDBMetricSchemaStep)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBCDCWriteSource, Value: "0", Type: TypeInt, MinValue: 0, MaxValue: 15, SetSession: func(s *SessionVars, val string) error {
		s.CDCWriteSource = uint64(TidbOptInt(val, 0))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(DefTiDBMetricSchemaRangeDuration), skipInit: true, Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaRangeDuration = TidbOptInt64(val, DefTiDBMetricSchemaRangeDuration)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInPlanCache, Value: BoolToOnOff(DefTiDBFoundInPlanCache), Type: TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInPlanCache), nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInBinding, Value: BoolToOnOff(DefTiDBFoundInBinding), Type: TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInBinding), nil
	}},
	{Scope: ScopeSession, Name: RandSeed1, Type: TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed1(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed1()), 10), true, nil
	}},
	{Scope: ScopeSession, Name: RandSeed2, Type: TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed2(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed2()), 10), true, nil
	}},
	{Scope: ScopeSession, Name: TiDBReadConsistency, Value: string(ReadConsistencyStrict), Type: TypeStr, Hidden: true,
		Validation: func(_ *SessionVars, normalized string, _ string, _ ScopeFlag) (string, error) {
			return normalized, validateReadConsistencyLevel(normalized)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.ReadConsistency = ReadConsistencyLevel(val)
			return nil
		},
	},
	{Scope: ScopeSession, Name: TiDBLastDDLInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastDDLInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: ScopeSession, Name: TiDBLastPlanReplayerToken, Value: "", ReadOnly: true,
		GetSession: func(s *SessionVars) (string, error) {
			return s.LastPlanReplayerToken, nil
		},
	},
	{Scope: ScopeSession, Name: TiDBUseAlloc, Value: BoolToOnOff(DefTiDBUseAlloc), Type: TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.preUseChunkAlloc), nil
	}},
	{Scope: ScopeSession, Name: TiDBExplicitRequestSourceType, Value: "", Type: TypeEnum, PossibleValues: tikvcliutil.ExplicitTypeList, GetSession: func(s *SessionVars) (string, error) {
		return s.ExplicitRequestSourceType, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.ExplicitRequestSourceType = val
		return nil
	}},
	/* The system variables below have INSTANCE scope  */
	{Scope: ScopeInstance, Name: TiDBLogFileMaxDays, Value: strconv.Itoa(config.GetGlobalConfig().Log.File.MaxDays), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		maxAge, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}
		GlobalLogMaxDays.Store(int32(maxAge))
		cfg := config.GetGlobalConfig().Log.ToLogConfig()
		cfg.Config.File.MaxDays = int(maxAge)

		err = logutil.ReplaceLogger(cfg, keyspace.WrapZapcoreWithKeyspace())
		if err != nil {
			return err
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(int64(GlobalLogMaxDays.Load()), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBConfig, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetJSONConfig()
	}},
	{Scope: ScopeInstance, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		ProcessGeneralLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(ProcessGeneralLog.Load()), nil
	}},
	{Scope: ScopeSession, Name: TiDBSlowTxnLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowTxnThreshold),
		Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
			s.SlowTxnThreshold = TidbOptUint64(val, logutil.DefaultSlowTxnThreshold)
			return nil
		},
	},
	{Scope: ScopeInstance, Name: TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&config.GetGlobalConfig().Instance.SlowThreshold, uint64(TidbOptInt64(val, logutil.DefaultSlowThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog, uint32(TidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
		return BoolToOnOff(enabled), nil
	}},
	{Scope: ScopeInstance, Name: TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.EnableSlowLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBPProfSQLCPU, Value: strconv.Itoa(DefTiDBPProfSQLCPU), Type: TypeInt, MinValue: 0, MaxValue: 1, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, DefTiDBPProfSQLCPU)) > 0)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		val := "0"
		if EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, nil
	}},
	{Scope: ScopeInstance, Name: TiDBDDLSlowOprThreshold, Value: strconv.Itoa(DefTiDBDDLSlowOprThreshold), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, DefTiDBDDLSlowOprThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBForcePriority, Value: mysql.Priority2Str[DefTiDBForcePriority], Type: TypeEnum, PossibleValues: []string{"NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreInt32(&ForcePriority, int32(mysql.Str2Priority(val)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], nil
	}},
	{Scope: ScopeInstance, Name: TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveQueryTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveQueryTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&ExpensiveQueryTimeThreshold), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBExpensiveTxnTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveTxnTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveTxnTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&ExpensiveTxnTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveTxnTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&ExpensiveTxnTimeThreshold), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(DefTiDBEnableCollectExecutionInfo), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
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
	{Scope: ScopeInstance, Name: PluginLoad, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginLoad, nil
	}},
	{Scope: ScopeInstance, Name: PluginDir, Value: "/data/deploy/plugin", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginDir, nil
	}},
	{Scope: ScopeInstance, Name: MaxConnections, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), Type: TypeUnsigned, MinValue: 0, MaxValue: 100000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.MaxConnections = uint32(TidbOptInt64(val, 0))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBEnableDDL, Value: BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()), Type: TypeBool,
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
	{Scope: ScopeInstance, Name: TiDBRCReadCheckTS, Value: BoolToOnOff(DefRCReadCheckTS), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableRCReadCheckTS.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableRCReadCheckTS.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBStmtSummaryEnablePersistent, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent), nil
	}},
	{Scope: ScopeInstance, Name: TiDBStmtSummaryFilename, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.StmtSummaryFilename, nil
	}},
	{Scope: ScopeInstance, Name: TiDBStmtSummaryFileMaxDays, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxDays), nil
	}},
	{Scope: ScopeInstance, Name: TiDBStmtSummaryFileMaxSize, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxSize), nil
	}},
	{Scope: ScopeInstance, Name: TiDBStmtSummaryFileMaxBackups, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxBackups), nil
	}},

	/* The system variables below have GLOBAL scope  */
	{Scope: ScopeGlobal, Name: MaxPreparedStmtCount, Value: strconv.FormatInt(DefMaxPreparedStmtCount, 10), Type: TypeInt, MinValue: -1, MaxValue: 1048576,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			MaxPreparedStmtCountValue.Store(num)
			return nil
		}},
	{Scope: ScopeGlobal, Name: InitConnect, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		p := parser.New()
		p.SetSQLMode(vars.SQLMode)
		p.SetParserConfig(vars.BuildParserConfig())
		_, _, err := p.ParseSQL(normalizedValue)
		if err != nil {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(InitConnect)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal, Name: ValidatePasswordEnable, Value: Off, Type: TypeBool},
	{Scope: ScopeGlobal, Name: ValidatePasswordPolicy, Value: "MEDIUM", Type: TypeEnum, PossibleValues: []string{"LOW", "MEDIUM", "STRONG"}},
	{Scope: ScopeGlobal, Name: ValidatePasswordCheckUserName, Value: On, Type: TypeBool},
	{Scope: ScopeGlobal, Name: ValidatePasswordLength, Value: "8", Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			numberCount, specialCharCount, mixedCaseCount := PasswordValidtaionNumberCount.Load(), PasswordValidationSpecialCharCount.Load(), PasswordValidationMixedCaseCount.Load()
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
			PasswordValidationLength.Store(int32(TidbOptInt64(val, 8)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(PasswordValidationLength.Load()), 10), nil
		},
	},
	{Scope: ScopeGlobal, Name: ValidatePasswordMixedCaseCount, Value: "1", Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			length, numberCount, specialCharCount := PasswordValidationLength.Load(), PasswordValidtaionNumberCount.Load(), PasswordValidationSpecialCharCount.Load()
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
			PasswordValidationMixedCaseCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(PasswordValidationMixedCaseCount.Load()), 10), nil
		},
	},
	{Scope: ScopeGlobal, Name: ValidatePasswordNumberCount, Value: "1", Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			length, specialCharCount, mixedCaseCount := PasswordValidationLength.Load(), PasswordValidationSpecialCharCount.Load(), PasswordValidationMixedCaseCount.Load()
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
			PasswordValidtaionNumberCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(PasswordValidtaionNumberCount.Load()), 10), nil
		},
	},
	{Scope: ScopeGlobal, Name: ValidatePasswordSpecialCharCount, Value: "1", Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			length, numberCount, mixedCaseCount := PasswordValidationLength.Load(), PasswordValidtaionNumberCount.Load(), PasswordValidationMixedCaseCount.Load()
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
			PasswordValidationSpecialCharCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(PasswordValidationSpecialCharCount.Load()), 10), nil
		},
	},
	{Scope: ScopeGlobal, Name: ValidatePasswordDictionary, Value: "", Type: TypeStr},
	{Scope: ScopeGlobal, Name: DefaultPasswordLifetime, Value: "0", Type: TypeInt, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: ScopeGlobal, Name: DisconnectOnExpiredPassword, Value: On, Type: TypeBool, ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(!IsSandBoxModeEnabled.Load()), nil
	}},

	/* TiDB specific variables */
	{Scope: ScopeGlobal, Name: TiDBTSOClientBatchMaxWaitTime, Value: strconv.FormatFloat(DefTiDBTSOClientBatchMaxWaitTime, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 10,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatFloat(MaxTSOBatchWaitInterval.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return (*SetPDClientDynamicOption.Load())(TiDBTSOClientBatchMaxWaitTime, val)
		}},
	{Scope: ScopeGlobal, Name: TiDBEnableTSOFollowerProxy, Value: BoolToOnOff(DefTiDBEnableTSOFollowerProxy), Type: TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(EnableTSOFollowerProxy.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(TiDBEnableTSOFollowerProxy, val)
	}},
	{Scope: ScopeGlobal, Name: PDEnableFollowerHandleRegion, Value: BoolToOnOff(DefPDEnableFollowerHandleRegion), Type: TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(EnablePDFollowerHandleRegion.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(PDEnableFollowerHandleRegion, val)
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableLocalTxn, Value: BoolToOnOff(DefTiDBEnableLocalTxn), Hidden: true, Type: TypeBool, Depended: true, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(EnableLocalTxn.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		oldVal := EnableLocalTxn.Load()
		newVal := TiDBOptOn(val)
		// Make sure the TxnScope is always Global when disable the Local Txn.
		// ON -> OFF
		if oldVal && !newVal {
			s.TxnScope = kv.NewGlobalTxnScopeVar()
		}
		EnableLocalTxn.Store(newVal)
		return nil
	}},
	{
		Scope:    ScopeGlobal,
		Name:     TiDBAutoAnalyzeRatio,
		Value:    strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64),
		Type:     TypeFloat,
		MinValue: 0,
		MaxValue: math.MaxUint64,
		// The value of TiDBAutoAnalyzeRatio should be greater than 0.00001 or equal to 0.00001.
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			ratio, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				return "", err
			}
			const minRatio = 0.00001
			const tolerance = 1e-9
			if ratio < minRatio && math.Abs(ratio-minRatio) > tolerance {
				return "", errors.Errorf("the value of %s should be greater than or equal to %f", TiDBAutoAnalyzeRatio, minRatio)
			}
			return normalizedValue, nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeStartTime, Value: DefAutoAnalyzeStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeEndTime, Value: DefAutoAnalyzeEndTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBMemQuotaBindingCache, Value: strconv.FormatInt(DefTiDBMemQuotaBindingCache, 10), Type: TypeUnsigned, MaxValue: math.MaxInt32, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatInt(MemQuotaBindingCache.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		MemQuotaBindingCache.Store(TidbOptInt64(val, DefTiDBMemQuotaBindingCache))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLFlashbackConcurrency, Value: strconv.Itoa(DefTiDBDDLFlashbackConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		SetDDLFlashbackConcurrency(int32(tidbOptPositiveInt32(val, DefTiDBDDLFlashbackConcurrency)))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgWorkerCount, Value: strconv.Itoa(DefTiDBDDLReorgWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgBatchSize, Value: strconv.Itoa(DefTiDBDDLReorgBatchSize), Type: TypeUnsigned, MinValue: int64(MinDDLReorgBatchSize), MaxValue: uint64(MaxDDLReorgBatchSize), SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgBatchSize)))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLErrorCountLimit, Value: strconv.Itoa(DefTiDBDDLErrorCountLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		SetDDLErrorCountLimit(TidbOptInt64(val, DefTiDBDDLErrorCountLimit))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(DefTiDBMaxDeltaSchemaCount), Type: TypeUnsigned, MinValue: 100, MaxValue: 16384, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		// It's a global variable, but it also wants to be cached in server.
		SetMaxDeltaSchemaCount(TidbOptInt64(val, DefTiDBMaxDeltaSchemaCount))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBScatterRegion, Value: BoolToOnOff(DefTiDBScatterRegion), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableStmtSummary, Value: BoolToOnOff(DefTiDBEnableStmtSummary), Type: TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnabled(TiDBOptOn(val))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(DefTiDBStmtSummaryInternalQuery), Type: TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnableInternalQuery(TiDBOptOn(val))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(DefTiDBStmtSummaryRefreshInterval), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			// convert val to int64
			return stmtsummaryv2.SetRefreshInterval(TidbOptInt64(val, DefTiDBStmtSummaryRefreshInterval))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryHistorySize, Value: strconv.Itoa(DefTiDBStmtSummaryHistorySize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetHistorySize(TidbOptInt(val, DefTiDBStmtSummaryHistorySize))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryMaxStmtCount, Value: strconv.Itoa(DefTiDBStmtSummaryMaxStmtCount), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt16, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxStmtCount(TidbOptInt(val, DefTiDBStmtSummaryMaxStmtCount))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryMaxSQLLength, Value: strconv.Itoa(DefTiDBStmtSummaryMaxSQLLength), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxSQLLength(TidbOptInt(val, DefTiDBStmtSummaryMaxSQLLength))
		}},
	{Scope: ScopeGlobal, Name: TiDBCapturePlanBaseline, Value: DefTiDBCapturePlanBaseline, Type: TypeBool, AllowEmptyAll: true},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskStartTime, Value: DefTiDBEvolvePlanTaskStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskEndTime, Value: DefTiDBEvolvePlanTaskEndTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(tikvstore.StoreLimit.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvstore.StoreLimit.Store(TidbOptInt64(val, DefTiDBStoreLimit))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTxnCommitBatchSize, Value: strconv.FormatUint(tikvstore.DefTxnCommitBatchSize, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 1 << 30,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatUint(tikvstore.TxnCommitBatchSize.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tikvstore.TxnCommitBatchSize.Store(uint64(TidbOptInt64(val, int64(tikvstore.DefTxnCommitBatchSize))))
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBRestrictedReadOnly, Value: BoolToOnOff(DefTiDBRestrictedReadOnly), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		// For user initiated SET GLOBAL, also change the value of TiDBSuperReadOnly
		if on && s.StmtCtx.StmtType == "Set" {
			err := s.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), TiDBSuperReadOnly, "ON")
			if err != nil {
				return err
			}
		}
		RestrictedReadOnly.Store(on)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBSuperReadOnly, Value: BoolToOnOff(DefTiDBSuperReadOnly), Type: TypeBool, Validation: func(s *SessionVars, normalizedValue string, _ string, _ ScopeFlag) (string, error) {
		on := TiDBOptOn(normalizedValue)
		if !on && s.StmtCtx.StmtType == "Set" {
			result, err := s.GlobalVarsAccessor.GetGlobalSysVar(TiDBRestrictedReadOnly)
			if err != nil {
				return normalizedValue, err
			}
			if TiDBOptOn(result) {
				return normalizedValue, fmt.Errorf("can't turn off %s when %s is on", TiDBSuperReadOnly, TiDBRestrictedReadOnly)
			}
		}
		return normalizedValue, nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		VarTiDBSuperReadOnly.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableGOGCTuner, Value: BoolToOnOff(DefTiDBEnableGOGCTuner), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		gctuner.EnableGOGCTuner.Store(on)
		if !on {
			gctuner.SetDefaultGOGC()
		}
		gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBGOGCTunerMaxValue, Value: strconv.Itoa(DefTiDBGOGCMaxValue),
		Type: TypeInt, MinValue: 10, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			maxValue := TidbOptInt64(val, DefTiDBGOGCMaxValue)
			gctuner.SetMaxGCPercent(uint32(maxValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MaxGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope ScopeFlag) (string, error) {
			maxValue := TidbOptInt64(origin, DefTiDBGOGCMaxValue)
			if maxValue <= int64(gctuner.MinGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_max_value should be more than tidb_gogc_tuner_min_value")
			}
			return origin, nil
		}},
	{Scope: ScopeGlobal, Name: TiDBGOGCTunerMinValue, Value: strconv.Itoa(DefTiDBGOGCMinValue),
		Type: TypeInt, MinValue: 10, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			minValue := TidbOptInt64(val, DefTiDBGOGCMinValue)
			gctuner.SetMinGCPercent(uint32(minValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MinGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope ScopeFlag) (string, error) {
			minValue := TidbOptInt64(origin, DefTiDBGOGCMinValue)
			if minValue >= int64(gctuner.MaxGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_min_value should be less than tidb_gogc_tuner_max_value")
			}
			return origin, nil
		}},
	{Scope: ScopeGlobal, Name: TiDBEnableTelemetry, Value: BoolToOnOff(DefTiDBEnableTelemetry), Type: TypeBool, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return "OFF", nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("tidb_enable_telemetry is deprecated since Telemetry has been removed, this variable is 'OFF' always."))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableHistoricalStats, Value: Off, Type: TypeBool, Depended: true},
	/* tikv gc metrics */
	{Scope: ScopeGlobal, Name: TiDBGCEnable, Value: On, Type: TypeBool, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_enable", On)
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_enable", val, "Current GC enable status")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCRunInterval, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_run_interval", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_run_interval", val, "GC run interval, at least 10m, in Go format.")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCLifetime, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_life_time", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_life_time", val, "All versions within life time will not be collected by GC, at least 10m, in Go format.")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCConcurrency, Value: "-1", Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		autoConcurrencyVal, err := getTiDBTableValue(s, "tikv_gc_auto_concurrency", On)
		if err == nil && autoConcurrencyVal == On {
			return "-1", nil // convention for "AUTO"
		}
		return getTiDBTableValue(s, "tikv_gc_concurrency", "-1")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		autoConcurrency := Off
		if val == "-1" {
			autoConcurrency = On
		}
		// Update both autoconcurrency and concurrency.
		if err := setTiDBTableValue(s, "tikv_gc_auto_concurrency", autoConcurrency, "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used"); err != nil {
			return err
		}
		return setTiDBTableValue(s, "tikv_gc_concurrency", val, "How many goroutines used to do GC parallel, [1, 256], default 2")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCScanLockMode, Value: "LEGACY", Type: TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_scan_lock_mode", "LEGACY")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_scan_lock_mode", val, "Mode of scanning locks, \"physical\" or \"legacy\"")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCMaxWaitTime, Value: strconv.Itoa(DefTiDBGCMaxWaitTime), Type: TypeInt, MinValue: 600, MaxValue: 31536000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		GCMaxWaitTime.Store(TidbOptInt64(val, DefTiDBGCMaxWaitTime))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTableCacheLease, Value: strconv.Itoa(DefTiDBTableCacheLease), Type: TypeUnsigned, MinValue: 1, MaxValue: 10, SetGlobal: func(_ context.Context, s *SessionVars, sVal string) error {
		var val int64
		val, err := strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		TableCacheLease.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzePartitionBatchSize,
		Value: strconv.Itoa(DefTiDBAutoAnalyzePartitionBatchSize),
		Type:  TypeUnsigned, MinValue: 1, MaxValue: mysql.PartitionCountLimit,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			var val int64
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			AutoAnalyzePartitionBatchSize.Store(val)
			return nil
		}},

	// variable for top SQL feature.
	// TopSQL enable only be controlled by TopSQL pub/sub sinker.
	// This global variable only uses to update the global config which store in PD(ETCD).
	{Scope: ScopeGlobal, Name: TiDBEnableTopSQL, Value: BoolToOnOff(topsqlstate.DefTiDBTopSQLEnable), Type: TypeBool, AllowEmpty: true, GlobalConfigName: GlobalConfigEnableTopSQL},
	{Scope: ScopeGlobal, Name: TiDBSourceID, Value: "1", Type: TypeInt, MinValue: 1, MaxValue: 15, GlobalConfigName: GlobalConfigSourceID},
	{Scope: ScopeGlobal, Name: TiDBTopSQLMaxTimeSeriesCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxTimeSeriesCount), Type: TypeInt, MinValue: 1, MaxValue: 5000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxStatementCount.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxStatementCount.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTopSQLMaxMetaCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxMetaCount), Type: TypeInt, MinValue: 1, MaxValue: 10000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxCollect.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxCollect.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: SkipNameResolve, Value: Off, Type: TypeBool},
	{Scope: ScopeGlobal, Name: DefaultAuthPlugin, Value: mysql.AuthNativePassword, Type: TypeEnum, PossibleValues: []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthLDAPSASL, mysql.AuthLDAPSimple}},
	{
		Scope: ScopeGlobal,
		Name:  TiDBPersistAnalyzeOptions,
		Value: BoolToOnOff(DefTiDBPersistAnalyzeOptions),
		Type:  TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(PersistAnalyzeOptions.Load()), nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			persist := TiDBOptOn(normalizedValue)
			if !persist && EnableColumnTracking.Load() {
				return "", errors.Errorf("tidb_persist_analyze_options option cannot be set to OFF when tidb_enable_column_tracking is ON, as this will result in the loss of column tracking information")
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			persist := TiDBOptOn(val)
			PersistAnalyzeOptions.Store(persist)
			return nil
		},
	},
	{
		Scope: ScopeGlobal, Name: TiDBEnableAutoAnalyze, Value: BoolToOnOff(DefTiDBEnableAutoAnalyze), Type: TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(RunAutoAnalyze.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			RunAutoAnalyze.Store(TiDBOptOn(val))
			return nil
		},
	}, {
		Scope: ScopeGlobal, Name: TiDBEnableAutoAnalyzePriorityQueue, Value: BoolToOnOff(DefTiDBEnableAutoAnalyzePriorityQueue), Type: TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(EnableAutoAnalyzePriorityQueue.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			EnableAutoAnalyzePriorityQueue.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBGOGCTunerThreshold, Value: strconv.FormatFloat(DefTiDBGOGCTunerThreshold, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(GOGCTunerThreshold.Load(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			floatValue := tidbOptFloat64(normalizedValue, DefTiDBGOGCTunerThreshold)
			globalMemoryLimitTuner := gctuner.GlobalMemoryLimitTuner.GetPercentage()
			if floatValue < 0 && floatValue > 0.9 {
				return "", ErrWrongValueForVar.GenWithStackByArgs(TiDBGOGCTunerThreshold, normalizedValue)
			}
			// globalMemoryLimitTuner must not be 0. it will be 0 when tidb_server_memory_limit_gc_trigger is not set during startup.
			if globalMemoryLimitTuner != 0 && globalMemoryLimitTuner < floatValue+0.05 {
				return "", errors.New("tidb_gogc_tuner_threshold should be less than tidb_server_memory_limit_gc_trigger - 0.05")
			}
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) (err error) {
			factor := tidbOptFloat64(val, DefTiDBGOGCTunerThreshold)
			GOGCTunerThreshold.Store(factor)
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
	{Scope: ScopeGlobal, Name: TiDBServerMemoryLimit, Value: DefTiDBServerMemoryLimit, Type: TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitOriginText.Load(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
			threshold := float64(bt) * GOGCTunerThreshold.Load()
			gctuner.Tuning(uint64(threshold))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBServerMemoryLimitSessMinSize, Value: strconv.FormatUint(DefTiDBServerMemoryLimitSessMinSize, 10), Type: TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitSessMinSize.String(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			intVal, err := strconv.ParseUint(normalizedValue, 10, 64)
			if err != nil {
				bt, str := parseByteSize(normalizedValue)
				if str == "" {
					return "", err
				}
				intVal = bt
			}
			if intVal > 0 && intVal < 128 { // 128 Bytes
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(TiDBServerMemoryLimitSessMinSize, originalValue))
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
	{Scope: ScopeGlobal, Name: TiDBServerMemoryLimitGCTrigger, Value: strconv.FormatFloat(DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64), Type: TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(gctuner.GlobalMemoryLimitTuner.GetPercentage(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			floatValue, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				perc, str := parsePercentage(normalizedValue)
				if len(str) == 0 {
					return "", err
				}
				floatValue = float64(perc) / 100
			}
			gogcTunerThreshold := GOGCTunerThreshold.Load()
			if floatValue < 0.51 || floatValue > 1 { // 51% ~ 100%
				return "", ErrWrongValueForVar.GenWithStackByArgs(TiDBServerMemoryLimitGCTrigger, normalizedValue)
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
		Scope: ScopeGlobal, Name: TiDBEnableColumnTracking,
		Value: BoolToOnOff(DefTiDBEnableColumnTracking),
		Type:  TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(EnableColumnTracking.Load()), nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			enabled := TiDBOptOn(normalizedValue)
			persist := PersistAnalyzeOptions.Load()
			if enabled && !persist {
				return "", errors.Errorf("tidb_enable_column_tracking option cannot be set to ON when tidb_persist_analyze_options is set to OFF, as this will prevent the preservation of column tracking information")
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			enabled := TiDBOptOn(val)
			// If this is a user initiated statement,
			// we log that column tracking is disabled.
			if s.StmtCtx.StmtType == "Set" && !enabled {
				// Set the location to UTC to avoid time zone interference.
				disableTime := time.Now().UTC().Format(types.UTCTimeFormat)
				if err := setTiDBTableValue(s, TiDBDisableColumnTrackingTime, disableTime, "Record the last time tidb_enable_column_tracking is set off"); err != nil {
					return err
				}
			}
			EnableColumnTracking.Store(enabled)
			return nil
		}},
	{Scope: ScopeGlobal, Name: RequireSecureTransport, Value: BoolToOnOff(DefRequireSecureTransport), Type: TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(tls.RequireSecureTransport.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tls.RequireSecureTransport.Store(TiDBOptOn(val))
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
	{Scope: ScopeGlobal, Name: TiDBStatsLoadPseudoTimeout, Value: BoolToOnOff(DefTiDBStatsLoadPseudoTimeout), Type: TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(StatsLoadPseudoTimeout.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			StatsLoadPseudoTimeout.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableBatchDML, Value: BoolToOnOff(DefTiDBEnableBatchDML), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableBatchDML.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableBatchDML.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBStatsCacheMemQuota, Value: strconv.Itoa(DefTiDBStatsCacheMemQuota),
		MinValue: 0, MaxValue: MaxTiDBStatsCacheMemQuota, Type: TypeInt,
		GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(StatsCacheMemQuota.Load(), 10), nil
		}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			v := TidbOptInt64(s, DefTiDBStatsCacheMemQuota)
			oldv := StatsCacheMemQuota.Load()
			if v != oldv {
				StatsCacheMemQuota.Store(v)
				SetStatsCacheCapacityFunc := SetStatsCacheCapacity.Load()
				(*SetStatsCacheCapacityFunc)(v)
			}
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBQueryLogMaxLen, Value: strconv.Itoa(DefTiDBQueryLogMaxLen), Type: TypeInt, MinValue: 0, MaxValue: 1073741824, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		QueryLogMaxLen.Store(int32(TidbOptInt64(val, DefTiDBQueryLogMaxLen)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(QueryLogMaxLen.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBCommitterConcurrency, Value: strconv.Itoa(DefTiDBCommitterConcurrency), Type: TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvutil.CommitterConcurrency.Store(int32(TidbOptInt64(val, DefTiDBCommitterConcurrency)))
		cfg := config.GetGlobalConfig().GetTiKVConfig()
		tikvcfg.StoreGlobalConfig(cfg)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(tikvutil.CommitterConcurrency.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMemQuotaAnalyze, Value: strconv.Itoa(DefTiDBMemQuotaAnalyze), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(GetMemQuotaAnalyze(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			SetMemQuotaAnalyze(TidbOptInt64(val, DefTiDBMemQuotaAnalyze))
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePrepPlanCache, Value: BoolToOnOff(DefTiDBEnablePrepPlanCache), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPrepPlanCacheSize, Aliases: []string{TiDBSessionPlanCacheSize}, Value: strconv.FormatUint(uint64(DefTiDBPrepPlanCacheSize), 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBPrepPlanCacheSize, TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePrepPlanCacheMemoryMonitor, Value: BoolToOnOff(DefTiDBEnablePrepPlanCacheMemoryMonitor), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCacheMemoryMonitor = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBPrepPlanCacheMemoryGuardRatio, Value: strconv.FormatFloat(DefTiDBPrepPlanCacheMemoryGuardRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			PreparedPlanCacheMemoryGuardRatio.Store(f)
		}
		return err
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatFloat(PreparedPlanCacheMemoryGuardRatio.Load(), 'f', -1, 64), nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNonPreparedPlanCache, Value: BoolToOnOff(DefTiDBEnableNonPreparedPlanCache), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNonPreparedPlanCacheForDML, Value: BoolToOnOff(DefTiDBEnableNonPreparedPlanCacheForDML), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCacheForDML = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    TiDBOptEnableFuzzyBinding,
		Value:                   BoolToOnOff(false),
		Type:                    TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFuzzyBinding = TiDBOptOn(val)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBNonPreparedPlanCacheSize, Value: strconv.FormatUint(uint64(DefTiDBNonPreparedPlanCacheSize), 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.NonPreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBNonPreparedPlanCacheSize, TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPlanCacheMaxPlanSize, Value: strconv.FormatUint(DefTiDBPlanCacheMaxPlanSize, 10), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PlanCacheMaxPlanSize = uVal
		}
		return err
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSessionPlanCacheSize, Aliases: []string{TiDBPrepPlanCacheSize}, Value: strconv.FormatUint(uint64(DefTiDBSessionPlanCacheSize), 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.SessionPlanCacheSize = uVal
		}
		return err
	}},
	{Scope: ScopeGlobal, Name: TiDBMemOOMAction, Value: DefTiDBMemOOMAction, PossibleValues: []string{"CANCEL", "LOG"}, Type: TypeEnum,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return OOMAction.Load(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			OOMAction.Store(val)
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBMaxAutoAnalyzeTime, Value: strconv.Itoa(DefTiDBMaxAutoAnalyzeTime), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(MaxAutoAnalyzeTime.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				MaxAutoAnalyzeTime.Store(num)
			}
			return err
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableMDL, Value: BoolToOnOff(DefTiDBEnableMDL), Type: TypeBool, SetGlobal: func(_ context.Context, vars *SessionVars, val string) error {
		if EnableMDL.Load() != TiDBOptOn(val) {
			err := SwitchMDL(TiDBOptOn(val))
			if err != nil {
				return err
			}
		}
		return nil
	}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(EnableMDL.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableDistTask, Value: BoolToOnOff(DefTiDBEnableDistTask), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if EnableDistTask.Load() != TiDBOptOn(val) {
			EnableDistTask.Store(TiDBOptOn(val))
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableDistTask.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableFastCreateTable, Value: BoolToOnOff(DefTiDBEnableFastCreateTable), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if EnableFastCreateTable.Load() != TiDBOptOn(val) {
			err := SwitchFastCreateTable(TiDBOptOn(val))
			if err != nil {
				return err
			}
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableFastCreateTable.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableNoopVariables, Value: BoolToOnOff(DefTiDBEnableNoopVariables), Type: TypeEnum, PossibleValues: []string{Off, On}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableNoopVariables.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableNoopVariables.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableGCAwareMemoryTrack, Value: BoolToOnOff(DefEnableTiDBGCAwareMemoryTrack), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		memory.EnableGCAwareMemoryTrack.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(memory.EnableGCAwareMemoryTrack.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableTmpStorageOnOOM, Value: BoolToOnOff(DefTiDBEnableTmpStorageOnOOM), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableTmpStorageOnOOM.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableTmpStorageOnOOM.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBAutoBuildStatsConcurrency, Value: strconv.Itoa(DefTiDBAutoBuildStatsConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency},
	{Scope: ScopeGlobal, Name: TiDBSysProcScanConcurrency, Value: strconv.Itoa(DefTiDBSysProcScanConcurrency), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32},
	{Scope: ScopeGlobal, Name: TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(DefMemoryUsageAlarmRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		MemoryUsageAlarmRatio.Store(tidbOptFloat64(val, DefMemoryUsageAlarmRatio))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMemoryUsageAlarmKeepRecordNum, Value: strconv.Itoa(DefMemoryUsageAlarmKeepRecordNum), Type: TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		MemoryUsageAlarmKeepRecordNum.Store(TidbOptInt64(val, DefMemoryUsageAlarmKeepRecordNum))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(MemoryUsageAlarmKeepRecordNum.Load(), 10), nil
	}},
	{Scope: ScopeGlobal, Name: PasswordReuseHistory, Value: strconv.Itoa(DefPasswordReuseHistory), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(PasswordHistory.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		PasswordHistory.Store(TidbOptInt64(val, DefPasswordReuseHistory))
		return nil
	}},
	{Scope: ScopeGlobal, Name: PasswordReuseTime, Value: strconv.Itoa(DefPasswordReuseTime), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(PasswordReuseInterval.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		PasswordReuseInterval.Store(TidbOptInt64(val, DefPasswordReuseTime))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableHistoricalStatsForCapture, Value: BoolToOnOff(DefTiDBEnableHistoricalStatsForCapture), Type: TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			EnableHistoricalStatsForCapture.Store(TiDBOptOn(s))
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(EnableHistoricalStatsForCapture.Load()), nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBHistoricalStatsDuration, Value: DefTiDBHistoricalStatsDuration.String(), Type: TypeDuration, MinValue: int64(time.Second), MaxValue: uint64(time.Hour * 24 * 365),
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return HistoricalStatsDuration.Load().String(), nil
		}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			HistoricalStatsDuration.Store(d)
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBLowResolutionTSOUpdateInterval, Value: strconv.Itoa(DefTiDBLowResolutionTSOUpdateInterval), Type: TypeInt, MinValue: 10, MaxValue: 60000,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			LowResolutionTSOUpdateInterval.Store(uint32(TidbOptInt64(val, DefTiDBLowResolutionTSOUpdateInterval)))
			if SetLowResolutionTSOUpdateInterval != nil {
				interval := time.Duration(LowResolutionTSOUpdateInterval.Load()) * time.Millisecond
				return SetLowResolutionTSOUpdateInterval(interval)
			}
			return nil
		},
	},

	/* The system variables below have GLOBAL and SESSION scope  */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePlanReplayerContinuousCapture, Value: BoolToOnOff(false), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			historicalStatsEnabled, err := s.GlobalVarsAccessor.GetGlobalSysVar(TiDBEnableHistoricalStats)
			if err != nil {
				return err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(val) {
				return errors.Errorf("%v should be enabled before enabling %v", TiDBEnableHistoricalStats, TiDBEnablePlanReplayerContinuousCapture)
			}
			s.EnablePlanReplayedContinuesCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayedContinuesCapture), nil
		},
		Validation: func(vars *SessionVars, s string, s2 string, flag ScopeFlag) (string, error) {
			historicalStatsEnabled, err := vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBEnableHistoricalStats)
			if err != nil {
				return "", err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(s) {
				return "", errors.Errorf("%v should be enabled before enabling %v", TiDBEnableHistoricalStats, TiDBEnablePlanReplayerContinuousCapture)
			}
			return s, nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePlanReplayerCapture, Value: BoolToOnOff(DefTiDBEnablePlanReplayerCapture), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnablePlanReplayerCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayerCapture), nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRowFormatVersion, Value: strconv.Itoa(DefTiDBRowFormatV1), Type: TypeUnsigned, MinValue: 1, MaxValue: 2, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		SetDDLReorgRowFormat(TidbOptInt64(val, DefTiDBRowFormatV2))
		return nil
	}, SetSession: func(s *SessionVars, val string) error {
		formatVersion := TidbOptInt64(val, DefTiDBRowFormatV1)
		if formatVersion == DefTiDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableRowLevelChecksum, Value: BoolToOnOff(DefTiDBEnableRowLevelChecksum), Type: TypeBool,
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(EnableRowLevelChecksum.Load()), nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			EnableRowLevelChecksum.Store(TiDBOptOn(s))
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSelectLimit, Value: "18446744073709551615", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultWeekFormat, Value: DefDefaultWeekFormat, Type: TypeUnsigned, MinValue: 0, MaxValue: 7},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    SQLModeVar,
		Value:                   mysql.DefaultSQLMode,
		IsHintUpdatableVerified: true,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag,
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
		Scope:                   ScopeGlobal,
		Name:                    TiDBLoadBindingTimeout,
		Value:                   "200",
		Type:                    TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: false,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			timeoutMS := tidbOptPositiveInt32(s, 0)
			vars.LoadBindingTimeout = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    MaxExecutionTime,
		Value:                   "0",
		Type:                    TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.MaxExecutionTime = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    TiKVClientReadTimeout,
		Value:                   "0",
		Type:                    TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.TiKVClientReadTimeout = uint64(timeoutMS)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharacterSetServer] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultCollationForUTF8MB4, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		coll, err := checkDefaultCollationForUTF8MB4(vars, normalizedValue, originalValue, scope)
		if err == nil {
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs(DefaultCollationForUTF8MB4))
		}
		return coll, err
	}, SetSession: func(s *SessionVars, val string) error {
		s.DefaultCollationForUTF8MB4 = val
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLLogBin, Value: On, Type: TypeBool},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    TimeZone,
		Value:                   "SYSTEM",
		IsHintUpdatableVerified: true,
		Validation: func(
			varErrFunctionsNoopImpls *SessionVars, normalizedValue string, originalValue string,
			scope ScopeFlag,
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
	{Scope: ScopeGlobal | ScopeSession, Name: ForeignKeyChecks, Value: BoolToOnOff(DefTiDBForeignKeyChecks), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = true
			return On, nil
		} else if !TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = false
			return Off, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue)
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableForeignKey, Value: BoolToOnOff(true), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableForeignKey.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(EnableForeignKey.Load()), nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationDatabase, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharsetDatabase] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementIncrement, Value: strconv.FormatInt(DefAutoIncrementIncrement, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = tidbOptPositiveInt32(val, DefAutoIncrementIncrement)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementOffset, Value: strconv.FormatInt(DefAutoIncrementOffset, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = tidbOptPositiveInt32(val, DefAutoIncrementOffset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetClient, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetClient)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetResults, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return normalizedValue, nil
		}
		return checkCharacterSet(normalizedValue, "")
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TxnIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, Aliases: []string{TransactionIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// MySQL appends a warning here for tx_isolation is deprecated
		// TiDB doesn't currently, but may in future. It is still commonly used by applications
		// So it might be noisy to do so.
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TransactionIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, Aliases: []string{TxnIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationConnection, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharacterSetConnection] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoCommit, Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		isAutocommit := TiDBOptOn(val)
		// Implicitly commit the possible ongoing transaction if mode is changed from off to on.
		if !s.IsAutocommit() && isAutocommit {
			s.SetInTxn(false)
		}
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharsetDatabase, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharsetDatabase)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationDatabase] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: WaitTimeout, Value: strconv.FormatInt(DefWaitTimeout, 10), Type: TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear},
	{Scope: ScopeGlobal | ScopeSession, Name: InteractiveTimeout, Value: "28800", Type: TypeUnsigned, MinValue: 1, MaxValue: secondsPerYear},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbLockWaitTimeout, Value: strconv.FormatInt(DefInnodbLockWaitTimeout, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 3600, SetSession: func(s *SessionVars, val string) error {
		lockWaitSec := TidbOptInt64(val, DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = lockWaitSec * 1000
		return nil
	}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    GroupConcatMaxLen,
		Value:                   strconv.FormatUint(DefGroupConcatMaxLen, 10),
		IsHintUpdatableVerified: true,
		Type:                    TypeUnsigned,
		MinValue:                4,
		MaxValue:                math.MaxUint64,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag,
		) (string, error) {
			// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
			// Minimum Value 4
			// Maximum Value (64-bit platforms) 18446744073709551615
			// Maximum Value (32-bit platforms) 4294967295
			if mathutil.IntBits == 32 {
				if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
					if val > uint64(math.MaxUint32) {
						vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(GroupConcatMaxLen, originalValue))
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
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetConnection, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetConnection)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationConnection] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetServer, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetServer)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationServer] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxAllowedPacket, Value: strconv.FormatUint(DefMaxAllowedPacket, 10), Type: TypeUnsigned, MinValue: 1024, MaxValue: MaxOfMaxAllowedPacket,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && scope == ScopeSession {
				err := ErrReadOnly.GenWithStackByArgs("SESSION", MaxAllowedPacket, "GLOBAL")
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
				vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(MaxAllowedPacket, normalizedValue))
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
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    WindowingUseHighPrecision,
		Value:                   On,
		Type:                    TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.WindowingUseHighPrecision = TiDBOptOn(val)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: BlockEncryptionMode, Value: DefBlockEncryptionMode, Type: TypeEnum, PossibleValues: []string{"aes-128-ecb", "aes-192-ecb", "aes-256-ecb", "aes-128-cbc", "aes-192-cbc", "aes-256-cbc", "aes-128-ofb", "aes-192-ofb", "aes-256-ofb", "aes-128-cfb", "aes-192-cfb", "aes-256-cfb"}},
	/* TiDB specific variables */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowMPPExecution, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowMPPExecution), Depended: true, SetSession: func(s *SessionVars, val string) error {
		s.allowMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowTiFlashCop, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowTiFlashCop), SetSession: func(s *SessionVars, val string) error {
		s.allowTiFlashCop = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashFastScan, Type: TypeBool, Value: BoolToOnOff(DefTiFlashFastScan), SetSession: func(s *SessionVars, val string) error {
		s.TiFlashFastScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMPPStoreFailTTL, Type: TypeStr, Value: DefTiDBMPPStoreFailTTL, SetSession: func(s *SessionVars, val string) error {
		s.MPPStoreFailTTL = val
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashExchangeWithNewCollation, Type: TypeBool, Value: BoolToOnOff(DefTiDBHashExchangeWithNewCollation), SetSession: func(s *SessionVars, val string) error {
		s.HashExchangeWithNewCollation = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdCount, Value: strconv.Itoa(DefBroadcastJoinThresholdCount), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdCount = TidbOptInt64(val, DefBroadcastJoinThresholdCount)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdSize, Value: strconv.Itoa(DefBroadcastJoinThresholdSize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdSize = TidbOptInt64(val, DefBroadcastJoinThresholdSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPreferBCJByExchangeDataSize, Type: TypeBool, Value: BoolToOnOff(DefPreferBCJByExchangeDataSize), SetSession: func(s *SessionVars, val string) error {
		s.PreferBCJByExchangeDataSize = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildStatsConcurrency, Value: strconv.Itoa(DefBuildStatsConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildSamplingStatsConcurrency, Value: strconv.Itoa(DefBuildSamplingStatsConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCartesianBCJ, Value: strconv.Itoa(DefOptCartesianBCJ), Type: TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowCartesianBCJ = TidbOptInt(val, DefOptCartesianBCJ)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptMPPOuterJoinFixedBuildSide, Value: BoolToOnOff(DefOptMPPOuterJoinFixedBuildSide), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.MPPOuterJoinFixedBuildSide = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBExecutorConcurrency, Value: strconv.Itoa(DefExecutorConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.ExecutorConcurrency = tidbOptPositiveInt32(val, DefExecutorConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDistSQLScanConcurrency, Value: strconv.Itoa(DefDistSQLScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.distSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeDistSQLScanConcurrency, Value: strconv.Itoa(DefAnalyzeDistSQLScanConcurrency), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.analyzeDistSQLScanConcurrency = tidbOptPositiveInt32(val, DefAnalyzeDistSQLScanConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(DefOptInSubqToJoinAndAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
		return nil
	}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    TiDBOptPreferRangeScan,
		Value:                   BoolToOnOff(DefOptPreferRangeScan),
		Type:                    TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.SetAllowPreferRangeScan(TiDBOptOn(val))
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptLimitPushDownThreshold, Value: strconv.Itoa(DefOptLimitPushDownThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.LimitPushDownThreshold = TidbOptInt64(val, DefOptLimitPushDownThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationThreshold = tidbOptFloat64(val, DefOptCorrelationThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptEnableCorrelationAdjustment, Value: BoolToOnOff(DefOptEnableCorrelationAdjustment), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableCorrelationAdjustment = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationExpFactor, Value: strconv.Itoa(DefOptCorrelationExpFactor), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationExpFactor = int(TidbOptInt64(val, DefOptCorrelationExpFactor))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCPUFactor, Value: strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.cpuFactor = tidbOptFloat64(val, DefOptCPUFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), skipInit: true, Type: TypeFloat, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopTiFlashConcurrencyFactor = tidbOptFloat64(val, DefOptTiFlashConcurrencyFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCopCPUFactor, Value: strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.copCPUFactor = tidbOptFloat64(val, DefOptCopCPUFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptNetworkFactor, Value: strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.networkFactor = tidbOptFloat64(val, DefOptNetworkFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptScanFactor, Value: strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.scanFactor = tidbOptFloat64(val, DefOptScanFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDescScanFactor, Value: strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.descScanFactor = tidbOptFloat64(val, DefOptDescScanFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptSeekFactor, Value: strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64), skipInit: true, Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.seekFactor = tidbOptFloat64(val, DefOptSeekFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptMemoryFactor, Value: strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.memoryFactor = tidbOptFloat64(val, DefOptMemoryFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDiskFactor, Value: strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.diskFactor = tidbOptFloat64(val, DefOptDiskFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptimizerEnableNewOnlyFullGroupByCheck, Value: BoolToOnOff(DefTiDBOptimizerEnableNewOFGB), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNewOnlyFullGroupByCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.concurrencyFactor = tidbOptFloat64(val, DefOptConcurrencyFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptForceInlineCTE, Value: BoolToOnOff(DefOptForceInlineCTE), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.enableForceInlineCTE = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexJoinBatchSize, Value: strconv.Itoa(DefIndexJoinBatchSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, DefIndexJoinBatchSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupSize, Value: strconv.Itoa(DefIndexLookupSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupSize = tidbOptPositiveInt32(val, DefIndexLookupSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupConcurrency, Value: strconv.Itoa(DefIndexLookupConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBIndexLookupConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupJoinConcurrency, Value: strconv.Itoa(DefIndexLookupJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBIndexLookupJoinConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(DefIndexSerialScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.indexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipUTF8Check, Value: BoolToOnOff(DefSkipUTF8Check), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipUTF8Check = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipASCIICheck, Value: BoolToOnOff(DefSkipASCIICheck), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipASCIICheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDMLBatchSize, Value: strconv.Itoa(DefDMLBatchSize), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.DMLBatchSize = int(TidbOptInt64(val, DefDMLBatchSize))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxChunkSize, Value: strconv.Itoa(DefMaxChunkSize), Type: TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowBatchCop, Value: strconv.Itoa(DefTiDBAllowBatchCop), Type: TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowBatchCop = int(TidbOptInt64(val, DefTiDBAllowBatchCop))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBInitChunkSize, Value: strconv.Itoa(DefInitChunkSize), Type: TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound, SetSession: func(s *SessionVars, val string) error {
		s.InitChunkSize = tidbOptPositiveInt32(val, DefInitChunkSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableCascadesPlanner, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableCascadesPlanner(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMerge, Value: BoolToOnOff(DefTiDBEnableIndexMerge), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableIndexMerge(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTablePartition, Value: On, Type: TypeEnum, PossibleValues: []string{Off, On, "AUTO"}, SetSession: func(s *SessionVars, val string) error {
		s.EnableTablePartition = val
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableListTablePartition, Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableListTablePartition = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashJoinConcurrency, Value: strconv.Itoa(DefTiDBHashJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBHashJoinConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBProjectionConcurrency, Value: strconv.Itoa(DefTiDBProjectionConcurrency), Type: TypeInt, MinValue: -1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.projectionConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBProjectionConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggPartialConcurrency, Value: strconv.Itoa(DefTiDBHashAggPartialConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggPartialConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBHashAggPartialConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggFinalConcurrency, Value: strconv.Itoa(DefTiDBHashAggFinalConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggFinalConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBHashAggFinalConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBWindowConcurrency, Value: strconv.Itoa(DefTiDBWindowConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.windowConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBWindowConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMergeJoinConcurrency, Value: strconv.Itoa(DefTiDBMergeJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.mergeJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBMergeJoinConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStreamAggConcurrency, Value: strconv.Itoa(DefTiDBStreamAggConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.streamAggConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBStreamAggConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexMergeIntersectionConcurrency, Value: strconv.Itoa(DefTiDBIndexMergeIntersectionConcurrency), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexMergeIntersectionConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, TiDBIndexMergeIntersectionConcurrency, TiDBExecutorConcurrency)
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableParallelApply, Value: BoolToOnOff(DefTiDBEnableParallelApply), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableParallelApply = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaApplyCache, Value: strconv.Itoa(DefTiDBMemQuotaApplyCache), Type: TypeUnsigned, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaApplyCache = TidbOptInt64(val, DefTiDBMemQuotaApplyCache)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackoffLockFast, Value: strconv.Itoa(tikvstore.DefBackoffLockFast), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, tikvstore.DefBackoffLockFast)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackOffWeight, Value: strconv.Itoa(tikvstore.DefBackOffWeight), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, tikvstore.DefBackOffWeight)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnEntrySizeLimit, Value: strconv.Itoa(DefTiDBTxnEntrySizeLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: config.MaxTxnEntrySizeLimit, SetSession: func(s *SessionVars, val string) error {
		s.TxnEntrySizeLimit = TidbOptUint64(val, DefTiDBTxnEntrySizeLimit)
		return nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		TxnEntrySizeLimit.Store(TidbOptUint64(val, DefTiDBTxnEntrySizeLimit))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRetryLimit, Value: strconv.Itoa(DefTiDBRetryLimit), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RetryLimit = TidbOptInt64(val, DefTiDBRetryLimit)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDisableTxnAutoRetry, Value: BoolToOnOff(DefTiDBDisableTxnAutoRetry), Type: TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			if normalizedValue == Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(Off, On))
			}
			return On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.DisableTxnAutoRetry = TiDBOptOn(val)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBConstraintCheckInPlace, Value: BoolToOnOff(DefTiDBConstraintCheckInPlace), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ConstraintCheckInPlace = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnMode, Value: DefTiDBTxnMode, AllowEmptyAll: true, Type: TypeEnum, PossibleValues: []string{PessimisticTxnMode, OptimisticTxnMode}, SetSession: func(s *SessionVars, val string) error {
		s.TxnMode = strings.ToUpper(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableWindowFunction, Value: BoolToOnOff(DefEnableWindowFunction), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableWindowFunction = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePipelinedWindowFunction, Value: BoolToOnOff(DefEnablePipelinedWindowFunction), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePipelinedWindowExec = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(DefEnableStrictDoubleTypeCheck), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStrictDoubleTypeCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableVectorizedExpression, Value: BoolToOnOff(DefEnableVectorizedExpression), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableVectorizedExpression = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableFastAnalyze, Value: BoolToOnOff(DefTiDBUseFastAnalyze), Type: TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			if TiDBOptOn(normalizedValue) {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect"))
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFastAnalyze = TiDBOptOn(val)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(DefTiDBSkipIsolationLevelCheck), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableRateLimitAction, Value: BoolToOnOff(DefTiDBEnableRateLimitAction), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnabledRateLimitAction = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowFallbackToTiKV, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(TiDBAllowFallbackToTiKV, normalizedValue)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAutoIncrementInGenerated, Value: BoolToOnOff(DefTiDBEnableAutoIncrementInGenerated), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAutoIncrementInGenerated = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPlacementMode, Value: DefTiDBPlacementMode, Type: TypeEnum, PossibleValues: []string{PlacementModeStrict, PlacementModeIgnore}, SetSession: func(s *SessionVars, val string) error {
		s.PlacementMode = val
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptJoinReorderThreshold, Value: strconv.Itoa(DefTiDBOptJoinReorderThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: 63, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNoopFuncs, Value: DefTiDBEnableNoopFuncs, Type: TypeEnum, PossibleValues: []string{Off, On, Warn}, Depended: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// The behavior is very weird if someone can turn TiDBEnableNoopFuncs OFF, but keep any of the following on:
		// TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, serverReadOnly, SQLAutoIsNull
		// To prevent this strange position, prevent setting to OFF when any of these sysVars are ON of the same scope.
		if normalizedValue == Off {
			for _, potentialIncompatibleSysVar := range []string{TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, ReadOnly, SQLAutoIsNull} {
				val, _ := vars.GetSystemVar(potentialIncompatibleSysVar) // session scope
				if scope == ScopeGlobal {                                // global scope
					var err error
					val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(potentialIncompatibleSysVar)
					if err != nil {
						return originalValue, errUnknownSystemVariable.GenWithStackByArgs(potentialIncompatibleSysVar)
					}
				}
				if TiDBOptOn(val) {
					return originalValue, errValueNotSupportedWhen.GenWithStackByArgs(TiDBEnableNoopFuncs, potentialIncompatibleSysVar)
				}
			}
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.NoopFuncsMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBReplicaRead, Value: "leader", Type: TypeEnum, PossibleValues: []string{"leader", "prefer-leader", "follower", "leader-and-follower", "closest-replicas", "closest-adaptive", "learner"}, SetSession: func(s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAdaptiveClosestReadThreshold, Value: strconv.Itoa(DefAdaptiveClosestReadThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.ReplicaClosestReadThreshold = TidbOptInt64(val, DefAdaptiveClosestReadThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBUsePlanBaselines, Value: BoolToOnOff(DefTiDBUsePlanBaselines), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.UsePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEvolvePlanBaselines, Value: BoolToOnOff(DefTiDBEvolvePlanBaselines), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "ON" && !config.CheckTableBeforeDrop {
			return normalizedValue, errors.Errorf("Cannot enable baseline evolution feature, it is not generally available now")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EvolvePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExtendedStats, Value: BoolToOnOff(false), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExtendedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CTEMaxRecursionDepth, Value: strconv.Itoa(DefCTEMaxRecursionDepth), Type: TypeInt, MinValue: 0, MaxValue: 4294967295, SetSession: func(s *SessionVars, val string) error {
		s.CTEMaxRecursionDepth = TidbOptInt(val, DefCTEMaxRecursionDepth)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(DefTiDBAllowAutoRandExplicitInsert), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAutoRandExplicitInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableClusteredIndex, Value: On, Type: TypeEnum, PossibleValues: []string{Off, On, IntOnly}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == IntOnly {
			vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(normalizedValue, fmt.Sprintf("'%s' or '%s'", On, Off)))
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableClusteredIndex = TiDBOptEnableClustered(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableGlobalIndex, Type: TypeBool, Value: BoolToOnOff(DefTiDBEnableGlobalIndex), SetSession: func(s *SessionVars, val string) error {
		s.EnableGlobalIndex = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPartitionPruneMode, Value: DefTiDBPartitionPruneMode, Type: TypeEnum, PossibleValues: []string{"static", "dynamic", "static-only", "dynamic-only"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		mode := PartitionPruneMode(normalizedValue).Update()
		if !mode.Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(TiDBPartitionPruneMode)
		}
		return string(mode), nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return s.PartitionPruneMode.Load(), nil
	}, SetSession: func(s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(s.PartitionPruneMode.Load()) == Static && PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
		}
		s.PartitionPruneMode.Store(newMode)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRedactLog, Value: DefTiDBRedactLog, Type: TypeEnum, PossibleValues: []string{Off, On, Marker}, SetSession: func(s *SessionVars, val string) error {
		s.EnableRedactLog = val
		errors.RedactLogEnabled.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBShardAllocateStep, Value: strconv.Itoa(DefTiDBShardAllocateStep), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), SetSession: func(s *SessionVars, val string) error {
		s.ShardAllocateStep = TidbOptInt64(val, DefTiDBShardAllocateStep)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAsyncCommit, Value: BoolToOnOff(DefTiDBEnableAsyncCommit), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAsyncCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnable1PC, Value: BoolToOnOff(DefTiDBEnable1PC), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable1PC = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBGuaranteeLinearizability, Value: BoolToOnOff(DefTiDBGuaranteeLinearizability), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.GuaranteeLinearizability = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeVersion, Value: strconv.Itoa(DefTiDBAnalyzeVersion), Type: TypeInt, MinValue: 1, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AnalyzeVersion = tidbOptPositiveInt32(val, DefTiDBAnalyzeVersion)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptEnableHashJoin, Value: BoolToOnOff(DefTiDBOptEnableHashJoin), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.DisableHashJoin = !TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMergeJoin, Value: BoolToOnOff(DefTiDBEnableIndexMergeJoin), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableIndexMergeJoin = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(DefTiDBTrackAggregateMemoryUsage), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TrackAggregateMemoryUsage = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMultiStatementMode, Value: Off, Type: TypeEnum, PossibleValues: []string{Off, On, Warn}, SetSession: func(s *SessionVars, val string) error {
		s.MultiStatementMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExchangePartition, Value: On, Type: TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag ScopeFlag) (string, error) {
			if s == Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_exchange_partition is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiDBEnableExchangePartition = true
			return nil
		}},
	// It's different from tmp_table_size or max_heap_table_size. See https://github.com/pingcap/tidb/issues/28691.
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTmpTableMaxSize, Value: strconv.Itoa(DefTiDBTmpTableMaxSize), Type: TypeUnsigned, MinValue: 1 << 20, MaxValue: 1 << 37, SetSession: func(s *SessionVars, val string) error {
		s.TMPTableSize = TidbOptInt64(val, DefTiDBTmpTableMaxSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableOrderedResultMode, Value: BoolToOnOff(DefTiDBEnableOrderedResultMode), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStableResultMode = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePseudoForOutdatedStats, Value: BoolToOnOff(DefTiDBEnablePseudoForOutdatedStats), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePseudoForOutdatedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRegardNULLAsPoint, Value: BoolToOnOff(DefTiDBRegardNULLAsPoint), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RegardNULLAsPoint = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePaging, Value: BoolToOnOff(DefTiDBEnablePaging), Type: TypeBool, Hidden: true, SetSession: func(s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableLegacyInstanceScope, Value: BoolToOnOff(DefEnableLegacyInstanceScope), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLegacyInstanceScope = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStatsLoadSyncWait, Value: strconv.Itoa(DefTiDBStatsLoadSyncWait), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		SetSession: func(s *SessionVars, val string) error {
			s.StatsLoadSyncWait.Store(TidbOptInt64(val, DefTiDBStatsLoadSyncWait))
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(StatsLoadSyncWait.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			StatsLoadSyncWait.Store(TidbOptInt64(val, DefTiDBStatsLoadSyncWait))
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSysdateIsNow, Value: BoolToOnOff(DefSysdateIsNow), Type: TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.SysdateIsNow = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableParallelHashaggSpill, Value: BoolToOnOff(DefTiDBEnableParallelHashaggSpill), Type: TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableParallelHashaggSpill = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableMutationChecker, Hidden: true,
		Value: BoolToOnOff(DefTiDBEnableMutationChecker), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableMutationChecker = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnAssertionLevel, Value: DefTiDBTxnAssertionLevel, PossibleValues: []string{AssertionOffStr, AssertionFastStr, AssertionStrictStr}, Hidden: true, Type: TypeEnum, SetSession: func(s *SessionVars, val string) error {
		s.AssertionLevel = tidbOptAssertionLevel(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBatchPendingTiFlashCount, Value: strconv.Itoa(DefTiDBBatchPendingTiFlashCount), MinValue: 0, MaxValue: math.MaxUint32, Hidden: false, Type: TypeUnsigned, SetSession: func(s *SessionVars, val string) error {
		b, e := strconv.Atoi(val)
		if e != nil {
			b = DefTiDBBatchPendingTiFlashCount
		}
		s.BatchPendingTiFlashCount = b
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIgnorePreparedCacheCloseStmt, Value: BoolToOnOff(DefTiDBIgnorePreparedCacheCloseStmt), Type: TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IgnorePreparedCacheCloseStmt = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNewCostInterface, Value: BoolToOnOff(true), Hidden: false, Type: TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag ScopeFlag) (string, error) {
			if s == Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(Off, On))
			}
			return On, nil
		},
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableNewCostInterface = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBCostModelVersion, Value: strconv.Itoa(DefTiDBCostModelVer), Hidden: false, Type: TypeInt, MinValue: 1, MaxValue: 2,
		SetSession: func(vars *SessionVars, s string) error {
			vars.CostModelVersion = int(TidbOptInt64(s, 1))
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexJoinDoubleReadPenaltyCostRate, Value: strconv.Itoa(0), Hidden: false, Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IndexJoinDoubleReadPenaltyCostRate = tidbOptFloat64(s, 0)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRCWriteCheckTs, Type: TypeBool, Value: BoolToOnOff(DefTiDBRcWriteCheckTs), SetSession: func(s *SessionVars, val string) error {
		s.RcWriteCheckTS = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRemoveOrderbyInSubquery, Value: BoolToOnOff(DefTiDBRemoveOrderbyInSubquery), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RemoveOrderbyInSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaQuery, Value: strconv.Itoa(DefTiDBMemQuotaQuery), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaQuery = TidbOptInt64(val, DefTiDBMemQuotaQuery)
		s.MemTracker.SetBytesLimit(s.MemQuotaQuery)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		intVal := TidbOptInt64(normalizedValue, DefTiDBMemQuotaQuery)
		if intVal > 0 && intVal < 128 {
			vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(TiDBMemQuotaQuery, originalValue))
			normalizedValue = "128"
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBNonTransactionalIgnoreError, Value: BoolToOnOff(DefTiDBBatchDMLIgnoreError), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.NonTransactionalIgnoreError = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashFineGrainedShuffleStreamCount, Value: strconv.Itoa(DefTiFlashFineGrainedShuffleStreamCount), Type: TypeInt, MinValue: -1, MaxValue: 1024,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleStreamCount = TidbOptInt64(val, DefTiFlashFineGrainedShuffleStreamCount)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashFineGrainedShuffleBatchSize, Value: strconv.Itoa(DefTiFlashFineGrainedShuffleBatchSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleBatchSize = uint64(TidbOptInt64(val, DefTiFlashFineGrainedShuffleBatchSize))
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBSimplifiedMetrics, Value: BoolToOnOff(DefTiDBSimplifiedMetrics), Type: TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			metrics.ToggleSimplifiedMode(TiDBOptOn(s))
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMinPagingSize, Value: strconv.Itoa(DefMinPagingSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MinPagingSize = tidbOptPositiveInt32(val, DefMinPagingSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxPagingSize, Value: strconv.Itoa(DefMaxPagingSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MaxPagingSize = tidbOptPositiveInt32(val, DefMaxPagingSize)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMemoryDebugModeMinHeapInUse, Value: strconv.Itoa(0), Type: TypeInt, MinValue: math.MinInt64, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeMinHeapInUse = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMemoryDebugModeAlarmRatio, Value: strconv.Itoa(0), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeAlarmRatio = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLRequirePrimaryKey, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PrimaryKeyRequired = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAnalyzeSnapshot, Value: BoolToOnOff(DefTiDBEnableAnalyzeSnapshot), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAnalyzeSnapshot = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBGenerateBinaryPlan, Value: BoolToOnOff(DefTiDBGenerateBinaryPlan), Type: TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		GenerateBinaryPlan.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDefaultStrMatchSelectivity, Value: strconv.FormatFloat(DefTiDBDefaultStrMatchSelectivity, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.DefaultStrMatchSelectivity = tidbOptFloat64(val, DefTiDBDefaultStrMatchSelectivity)
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBDDLEnableFastReorg, Value: BoolToOnOff(DefTiDBEnableFastReorg), Type: TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(EnableFastReorg.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		EnableFastReorg.Store(TiDBOptOn(val))
		return nil
	}},
	// This system var is set disk quota for lightning sort dir, from 100 GB to 1PB.
	{Scope: ScopeGlobal, Name: TiDBDDLDiskQuota, Value: strconv.Itoa(DefTiDBDDLDiskQuota), Type: TypeInt, MinValue: DefTiDBDDLDiskQuota, MaxValue: 1024 * 1024 * DefTiDBDDLDiskQuota / 100, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatUint(DDLDiskQuota.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		DDLDiskQuota.Store(TidbOptUint64(val, DefTiDBDDLDiskQuota))
		return nil
	}},
	// can't assign validate function here. Because validation function will run after GetGlobal function
	{Scope: ScopeGlobal, Name: TiDBCloudStorageURI, Value: "", Type: TypeStr, GetGlobal: func(ctx context.Context, sv *SessionVars) (string, error) {
		cloudStorageURI := CloudStorageURI.Load()
		if len(cloudStorageURI) > 0 {
			cloudStorageURI = ast.RedactURL(cloudStorageURI)
		}
		return cloudStorageURI, nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		if len(val) > 0 && val != CloudStorageURI.Load() {
			if err := ValidateCloudStorageURI(ctx, val); err != nil {
				return err
			}
		}
		CloudStorageURI.Store(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBConstraintCheckInPlacePessimistic, Value: BoolToOnOff(config.GetGlobalConfig().PessimisticTxn.ConstraintCheckInPlacePessimistic), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.ConstraintCheckInPlacePessimistic = TiDBOptOn(val)
			if !s.ConstraintCheckInPlacePessimistic {
				metrics.LazyPessimisticUniqueCheckSetCount.Inc()
			}
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTiFlashReadForWriteStmt, Value: On, Type: TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag ScopeFlag) (string, error) {
			if s == Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_tiflash_read_for_write_stmt is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableTiFlashReadForWriteStmt = true
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableUnsafeSubstitute, Value: BoolToOnOff(false), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableUnsafeSubstitute = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptRangeMaxSize, Value: strconv.FormatInt(DefTiDBOptRangeMaxSize, 10), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RangeMaxSize = TidbOptInt64(val, DefTiDBOptRangeMaxSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptAdvancedJoinHint, Value: BoolToOnOff(DefTiDBOptAdvancedJoinHint), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAdvancedJoinHint = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptUseInvisibleIndexes, Value: BoolToOnOff(false), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerUseInvisibleIndexes = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzePartitionConcurrency, Value: strconv.FormatInt(DefTiDBAnalyzePartitionConcurrency, 10),
		MinValue: 1, MaxValue: uint64(config.GetGlobalConfig().Performance.AnalyzePartitionConcurrencyQuota), SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionConcurrency = int(TidbOptInt64(val, DefTiDBAnalyzePartitionConcurrency))
			return nil
		}},
	{
		Scope: ScopeGlobal | ScopeSession, Name: TiDBMergePartitionStatsConcurrency, Value: strconv.FormatInt(DefTiDBMergePartitionStatsConcurrency, 10), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency,
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionMergeConcurrency = TidbOptInt(val, DefTiDBMergePartitionStatsConcurrency)
			return nil
		},
	},
	{
		Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAsyncMergeGlobalStats, Value: BoolToOnOff(DefTiDBEnableAsyncMergeGlobalStats), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableAsyncMergeGlobalStats = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptPrefixIndexSingleScan, Value: BoolToOnOff(DefTiDBOptPrefixIndexSingleScan), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptPrefixIndexSingleScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBExternalTS, Value: strconv.FormatInt(DefTiDBExternalTS, 10), SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExternalTSRead, Value: BoolToOnOff(DefTiDBEnableExternalTSRead), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExternalTSRead = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableReusechunk, Value: BoolToOnOff(DefTiDBEnableReusechunk), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableReuseChunk = TiDBOptOn(val)
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBIgnoreInlistPlanDigest, Value: BoolToOnOff(DefTiDBIgnoreInlistPlanDigest), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		IgnoreInlistPlanDigest.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(IgnoreInlistPlanDigest.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTTLJobEnable, Value: BoolToOnOff(DefTiDBTTLJobEnable), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		EnableTTLJob.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(EnableTTLJob.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTTLScanBatchSize, Value: strconv.Itoa(DefTiDBTTLScanBatchSize), Type: TypeInt, MinValue: DefTiDBTTLScanBatchMinSize, MaxValue: DefTiDBTTLScanBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		TTLScanBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := TTLScanBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTTLDeleteBatchSize, Value: strconv.Itoa(DefTiDBTTLDeleteBatchSize), Type: TypeInt, MinValue: DefTiDBTTLDeleteBatchMinSize, MaxValue: DefTiDBTTLDeleteBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		TTLDeleteBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := TTLDeleteBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTTLDeleteRateLimit, Value: strconv.Itoa(DefTiDBTTLDeleteRateLimit), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		TTLDeleteRateLimit.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := TTLDeleteRateLimit.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{
		Scope: ScopeGlobal | ScopeSession, Name: TiDBStoreBatchSize, Value: strconv.FormatInt(DefTiDBStoreBatchSize, 10),
		Type: TypeInt, MinValue: 0, MaxValue: 25000, SetSession: func(s *SessionVars, val string) error {
			s.StoreBatchSize = TidbOptInt(val, DefTiDBStoreBatchSize)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: MppExchangeCompressionMode, Type: TypeStr, Value: DefaultExchangeCompressionMode.Name(),
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			_, ok := kv.ToExchangeCompressionMode(normalizedValue)
			if !ok {
				var msg string
				for m := kv.ExchangeCompressionModeNONE; m <= kv.ExchangeCompressionModeUnspecified; m += 1 {
					if m == 0 {
						msg = m.Name()
					} else {
						msg = fmt.Sprintf("%s, %s", msg, m.Name())
					}
				}
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s",
					originalValue,
					MppExchangeCompressionMode, msg)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.mppExchangeCompressionMode, _ = kv.ToExchangeCompressionMode(val)
			if s.ChooseMppVersion() == kv.MppVersionV0 && s.mppExchangeCompressionMode != kv.ExchangeCompressionModeUnspecified {
				s.StmtCtx.AppendWarning(fmt.Errorf("mpp exchange compression won't work under current mpp version %d", kv.MppVersionV0))
			}

			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: MppVersion, Type: TypeStr, Value: kv.MppVersionUnspecifiedName,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			_, ok := kv.ToMppVersion(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %d (unspecified)",
				originalValue, MppVersion, kv.MppVersionUnspecified)
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
		Scope: ScopeGlobal, Name: TiDBTTLJobScheduleWindowStartTime, Value: DefTiDBTTLJobScheduleWindowStartTime, Type: TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			startTime, err := time.ParseInLocation(FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			TTLJobScheduleWindowStartTime.Store(startTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			startTime := TTLJobScheduleWindowStartTime.Load()
			return startTime.Format(FullDayTimeFormat), nil
		},
	},
	{
		Scope: ScopeGlobal, Name: TiDBTTLJobScheduleWindowEndTime, Value: DefTiDBTTLJobScheduleWindowEndTime, Type: TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			endTime, err := time.ParseInLocation(FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			TTLJobScheduleWindowEndTime.Store(endTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			endTime := TTLJobScheduleWindowEndTime.Load()
			return endTime.Format(FullDayTimeFormat), nil
		},
	},
	{
		Scope: ScopeGlobal, Name: TiDBTTLScanWorkerCount, Value: strconv.Itoa(DefTiDBTTLScanWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			TTLScanWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(TTLScanWorkerCount.Load())), nil
		},
	},
	{
		Scope: ScopeGlobal, Name: TiDBTTLDeleteWorkerCount, Value: strconv.Itoa(DefTiDBTTLDeleteWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			TTLDeleteWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(TTLDeleteWorkerCount.Load())), nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableResourceControl, Value: BoolToOnOff(DefTiDBEnableResourceControl), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		if TiDBOptOn(s) != EnableResourceControl.Load() {
			EnableResourceControl.Store(TiDBOptOn(s))
			(*SetGlobalResourceControl.Load())(TiDBOptOn(s))
			logutil.BgLogger().Info("set resource control", zap.Bool("enable", TiDBOptOn(s)))
		}
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(EnableResourceControl.Load()), nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPessimisticTransactionFairLocking, Value: BoolToOnOff(DefTiDBPessimisticTransactionFairLocking), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PessimisticTransactionFairLocking = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePlanCacheForParamLimit, Value: BoolToOnOff(DefTiDBEnablePlanCacheForParamLimit), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForParamLimit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableINLJoinInnerMultiPattern, Value: BoolToOnOff(false), Type: TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableINLJoinInnerMultiPattern = TiDBOptOn(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return BoolToOnOff(s.EnableINLJoinInnerMultiPattern), nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashComputeDispatchPolicy, Value: string(DefTiFlashComputeDispatchPolicy), Type: TypeStr, SetSession: setTiFlashComputeDispatchPolicy,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			return setTiFlashComputeDispatchPolicy(vars, s)
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePlanCacheForSubquery, Value: BoolToOnOff(DefTiDBEnablePlanCacheForSubquery), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptEnableLateMaterialization, Value: BoolToOnOff(DefTiDBOptEnableLateMaterialization), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLateMaterialization = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBLoadBasedReplicaReadThreshold, Value: DefTiDBLoadBasedReplicaReadThreshold.String(), Type: TypeDuration, MaxValue: uint64(time.Hour), SetSession: func(s *SessionVars, val string) error {
		d, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		s.LoadBasedReplicaReadThreshold = d
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTTLRunningTasks, Value: strconv.Itoa(DefTiDBTTLRunningTasks), Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		TTLRunningTasks.Store(int32(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(int(TTLRunningTasks.Load())), nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptOrderingIdxSelThresh, Value: strconv.FormatFloat(DefTiDBOptOrderingIdxSelThresh, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelThresh = tidbOptFloat64(val, DefTiDBOptOrderingIdxSelThresh)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptOrderingIdxSelRatio, Value: strconv.FormatFloat(DefTiDBOptOrderingIdxSelRatio, 'f', -1, 64), Type: TypeFloat, MinValue: -1, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelRatio = tidbOptFloat64(val, DefTiDBOptOrderingIdxSelRatio)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptEnableMPPSharedCTEExecution, Value: BoolToOnOff(DefTiDBOptEnableMPPSharedCTEExecution), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableMPPSharedCTEExecution = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   ScopeGlobal | ScopeSession,
		Name:                    TiDBOptFixControl,
		Value:                   "",
		Type:                    TypeStr,
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeSkipColumnTypes, Value: "json,blob,mediumblob,longblob,mediumtext,longtext", Type: TypeStr,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			return ValidAnalyzeSkipColumnTypes(normalizedValue)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzeSkipColumnTypes = ParseAnalyzeSkipColumnTypes(val)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPlanCacheInvalidationOnFreshStats, Value: BoolToOnOff(DefTiDBPlanCacheInvalidationOnFreshStats), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PlanCacheInvalidationOnFreshStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiFlashReplicaRead, Value: DefTiFlashReplicaRead, Type: TypeEnum, PossibleValues: []string{DefTiFlashReplicaRead, tiflash.ClosestAdaptiveStr, tiflash.ClosestReplicasStr},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashReplicaRead = tiflash.GetTiFlashReplicaReadByStr(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return tiflash.GetTiFlashReplicaRead(s.TiFlashReplicaRead), nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBFastCheckTable, Value: BoolToOnOff(DefTiDBEnableFastCheckTable), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.FastCheckTable = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipMissingPartitionStats, Value: BoolToOnOff(DefTiDBSkipMissingPartitionStats), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipMissingPartitionStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLAuthMethodName, Value: DefAuthenticationLDAPSASLAuthMethodName, Type: TypeEnum, PossibleValues: []string{ldap.SASLAuthMethodSCRAMSHA1, ldap.SASLAuthMethodSCRAMSHA256, ldap.SASLAuthMethodGSSAPI}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetSASLAuthMethod(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSASLAuthMethod(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLServerHost, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLServerPort, Value: strconv.Itoa(DefAuthenticationLDAPSASLServerPort), Type: TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLTLS, Value: BoolToOnOff(DefAuthenticationLDAPSASLTLS), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSASLAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLCAPath, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSASLAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetCAPath(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLUserSearchAttr, Value: DefAuthenticationLDAPSASLUserSearchAttr, Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLBindBaseDN, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLBindRootDN, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLBindRootPWD, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSASLAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLInitPoolSize, Value: strconv.Itoa(DefAuthenticationLDAPSASLInitPoolSize), Type: TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSASLMaxPoolSize, Value: strconv.Itoa(DefAuthenticationLDAPSASLMaxPoolSize), Type: TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetMaxCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetMaxCapacity()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleAuthMethodName, Value: DefAuthenticationLDAPSimpleAuthMethodName, Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		s = strings.ToUpper(s)
		// Only "SIMPLE" is supported
		if s != "SIMPLE" {
			return errors.Errorf("auth method %s is not supported", s)
		}
		return nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleServerHost, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleServerPort, Value: strconv.Itoa(DefAuthenticationLDAPSimpleServerPort), Type: TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleTLS, Value: BoolToOnOff(DefAuthenticationLDAPSimpleTLS), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSimpleAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleCAPath, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSimpleAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetCAPath(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleUserSearchAttr, Value: DefAuthenticationLDAPSimpleUserSearchAttr, Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleBindBaseDN, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleBindRootDN, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleBindRootPWD, Value: "", Type: TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSimpleAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleInitPoolSize, Value: strconv.Itoa(DefAuthenticationLDAPSimpleInitPoolSize), Type: TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: ScopeGlobal, Name: AuthenticationLDAPSimpleMaxPoolSize, Value: strconv.Itoa(DefAuthenticationLDAPSimpleMaxPoolSize), Type: TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRuntimeFilterTypeName, Value: DefRuntimeFilterType, Type: TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			_, ok := ToRuntimeFilterType(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s should be sepreated by , such as %s, also we only support IN and MIN_MAX now. ",
				originalValue, TiDBRuntimeFilterTypeName, DefRuntimeFilterType)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterTypes, _ = ToRuntimeFilterType(val)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRuntimeFilterModeName, Value: DefRuntimeFilterMode, Type: TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			_, ok := RuntimeFilterModeStringToMode(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %s ",
				originalValue, TiDBRuntimeFilterModeName, DefRuntimeFilterMode)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterMode, _ = RuntimeFilterModeStringToMode(val)
			return nil
		},
	},
	{
		Scope: ScopeGlobal | ScopeSession,
		Name:  TiDBLockUnchangedKeys,
		Value: BoolToOnOff(DefTiDBLockUnchangedKeys),
		Type:  TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.LockUnchangedKeys = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableCheckConstraint, Value: BoolToOnOff(DefTiDBEnableCheckConstraint), Type: TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		EnableCheckConstraint.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(EnableCheckConstraint.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBSchemaCacheSize, Value: strconv.Itoa(DefTiDBSchemaCacheSize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
		// It does not take effect immediately, but within a ddl lease, infoschema reload would cause the v2 to be used.
		SchemaCacheSize.Store(TidbOptInt64(val, DefTiDBSchemaCacheSize))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := SchemaCacheSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: ScopeSession, Name: TiDBSessionAlias, Value: "", Type: TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			chars := []rune(normalizedValue)
			warningAdded := false
			if len(chars) > 64 {
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(TiDBSessionAlias, originalValue))
				warningAdded = true
				chars = chars[:64]
				normalizedValue = string(chars)
			}

			// truncate to a valid identifier
			for normalizedValue != "" && util.IsInCorrectIdentifierName(normalizedValue) {
				if !warningAdded {
					s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(TiDBSessionAlias, originalValue))
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
		Scope:          ScopeGlobal | ScopeSession,
		Name:           TiDBOptObjective,
		Value:          DefTiDBOptObjective,
		Type:           TypeEnum,
		PossibleValues: []string{OptObjectiveModerate, OptObjectiveDeterminate},
		SetSession: func(vars *SessionVars, s string) error {
			vars.OptObjective = s
			return nil
		},
	},
	{Scope: ScopeInstance, Name: TiDBServiceScope, Value: "", Type: TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ ScopeFlag) (string, error) {
			return normalizedValue, servicescope.CheckServiceScope(originalValue)
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			newValue := strings.ToLower(s)
			ServiceScope.Store(newValue)
			oldConfig := config.GetGlobalConfig()
			if oldConfig.Instance.TiDBServiceScope != newValue {
				newConfig := *oldConfig
				newConfig.Instance.TiDBServiceScope = newValue
				config.StoreGlobalConfig(&newConfig)
			}
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return ServiceScope.Load(), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBSchemaVersionCacheLimit, Value: strconv.Itoa(DefTiDBSchemaVersionCacheLimit), Type: TypeInt, MinValue: 2, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			SchemaVersionCacheLimit.Store(TidbOptInt64(val, DefTiDBSchemaVersionCacheLimit))
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIdleTransactionTimeout, Value: strconv.Itoa(DefTiDBIdleTransactionTimeout), Type: TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear,
		SetSession: func(s *SessionVars, val string) error {
			s.IdleTransactionTimeout = tidbOptPositiveInt32(val, DefTiDBIdleTransactionTimeout)
			return nil
		}},
	{Scope: ScopeGlobal | ScopeSession, Name: DivPrecisionIncrement, Value: strconv.Itoa(DefDivPrecisionIncrement), Type: TypeUnsigned, MinValue: 0, MaxValue: 30,
		SetSession: func(s *SessionVars, val string) error {
			s.DivPrecisionIncrement = tidbOptPositiveInt32(val, DefDivPrecisionIncrement)
			return nil
		}},
	{Scope: ScopeSession, Name: TiDBDMLType, Value: DefTiDBDMLType, Type: TypeStr,
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
}

// GlobalSystemVariableInitialValue gets the default value for a system variable including ones that are dynamically set (e.g. based on the store)
func GlobalSystemVariableInitialValue(varName, varVal string) string {
	switch varName {
	case TiDBEnableAsyncCommit, TiDBEnable1PC:
		if config.GetGlobalConfig().Store == "tikv" {
			varVal = On
		}
	case TiDBMemOOMAction:
		if intest.InTest {
			varVal = OOMActionLog
		}
	case TiDBEnableAutoAnalyze:
		if intest.InTest {
			varVal = Off
		}
	// For the following sysvars, we change the default
	// FOR NEW INSTALLS ONLY. In most cases you don't want to do this.
	// It is better to change the value in the Sysvar struct, so that
	// all installs will have the same value.
	case TiDBRowFormatVersion:
		varVal = strconv.Itoa(DefTiDBRowFormatV2)
	case TiDBTxnAssertionLevel:
		varVal = AssertionFastStr
	case TiDBEnableMutationChecker:
		varVal = On
	case TiDBPessimisticTransactionFairLocking:
		varVal = On
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

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	CharacterSetClient,
	CharacterSetConnection,
	CharacterSetResults,
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	CharacterSetClient,
	CharacterSetResults,
}

const (
	// MaskPwd is the mask of password for LDAP variables.
	MaskPwd = "******"

	// PessimisticTxnMode is the name for tidb_txn_mode system variable.
	PessimisticTxnMode = "pessimistic"
	// OptimisticTxnMode is the name for tidb_txn_mode system variable.
	OptimisticTxnMode = "optimistic"
)

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// CollationConnection is the name for collation_connection system variable.
	CollationConnection = "collation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// CollationDatabase is the name for collation_database system variable.
	CollationDatabase = "collation_database"
	// CharacterSetFilesystem is the name for character_set_filesystem system variable.
	CharacterSetFilesystem = "character_set_filesystem"
	// CharacterSetClient is the name for character_set_client system variable.
	CharacterSetClient = "character_set_client"
	// CharacterSetSystem is the name for character_set_system system variable.
	CharacterSetSystem = "character_set_system"
	// GeneralLog is the name for 'general_log' system variable.
	GeneralLog = "general_log"
	// AvoidTemporalUpgrade is the name for 'avoid_temporal_upgrade' system variable.
	AvoidTemporalUpgrade = "avoid_temporal_upgrade"
	// MaxPreparedStmtCount is the name for 'max_prepared_stmt_count' system variable.
	MaxPreparedStmtCount = "max_prepared_stmt_count"
	// BigTables is the name for 'big_tables' system variable.
	BigTables = "big_tables"
	// CheckProxyUsers is the name for 'check_proxy_users' system variable.
	CheckProxyUsers = "check_proxy_users"
	// CoreFile is the name for 'core_file' system variable.
	CoreFile = "core_file"
	// DefaultWeekFormat is the name for 'default_week_format' system variable.
	DefaultWeekFormat = "default_week_format"
	// GroupConcatMaxLen is the name for 'group_concat_max_len' system variable.
	GroupConcatMaxLen = "group_concat_max_len"
	// DelayKeyWrite is the name for 'delay_key_write' system variable.
	DelayKeyWrite = "delay_key_write"
	// EndMarkersInJSON is the name for 'end_markers_in_json' system variable.
	EndMarkersInJSON = "end_markers_in_json"
	// Hostname is the name for 'hostname' system variable.
	Hostname = "hostname"
	// InnodbCommitConcurrency is the name for 'innodb_commit_concurrency' system variable.
	InnodbCommitConcurrency = "innodb_commit_concurrency"
	// InnodbFastShutdown is the name for 'innodb_fast_shutdown' system variable.
	InnodbFastShutdown = "innodb_fast_shutdown"
	// InnodbLockWaitTimeout is the name for 'innodb_lock_wait_timeout' system variable.
	InnodbLockWaitTimeout = "innodb_lock_wait_timeout"
	// SQLLogBin is the name for 'sql_log_bin' system variable.
	SQLLogBin = "sql_log_bin"
	// LogBin is the name for 'log_bin' system variable.
	LogBin = "log_bin"
	// MaxSortLength is the name for 'max_sort_length' system variable.
	MaxSortLength = "max_sort_length"
	// MaxSpRecursionDepth is the name for 'max_sp_recursion_depth' system variable.
	MaxSpRecursionDepth = "max_sp_recursion_depth"
	// MaxUserConnections is the name for 'max_user_connections' system variable.
	MaxUserConnections = "max_user_connections"
	// OfflineMode is the name for 'offline_mode' system variable.
	OfflineMode = "offline_mode"
	// InteractiveTimeout is the name for 'interactive_timeout' system variable.
	InteractiveTimeout = "interactive_timeout"
	// FlushTime is the name for 'flush_time' system variable.
	FlushTime = "flush_time"
	// PseudoSlaveMode is the name for 'pseudo_slave_mode' system variable.
	PseudoSlaveMode = "pseudo_slave_mode"
	// LowPriorityUpdates is the name for 'low_priority_updates' system variable.
	LowPriorityUpdates = "low_priority_updates"
	// LowerCaseTableNames is the name for 'lower_case_table_names' system variable.
	LowerCaseTableNames = "lower_case_table_names"
	// SessionTrackGtids is the name for 'session_track_gtids' system variable.
	SessionTrackGtids = "session_track_gtids"
	// OldPasswords is the name for 'old_passwords' system variable.
	OldPasswords = "old_passwords"
	// MaxConnections is the name for 'max_connections' system variable.
	MaxConnections = "max_connections"
	// SkipNameResolve is the name for 'skip_name_resolve' system variable.
	SkipNameResolve = "skip_name_resolve"
	// ForeignKeyChecks is the name for 'foreign_key_checks' system variable.
	ForeignKeyChecks = "foreign_key_checks"
	// SQLSafeUpdates is the name for 'sql_safe_updates' system variable.
	SQLSafeUpdates = "sql_safe_updates"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// DefaultPasswordLifetime is the name for 'default_password_lifetime' system variable.
	DefaultPasswordLifetime = "default_password_lifetime"
	// DisconnectOnExpiredPassword is the name for 'disconnect_on_expired_password' system variable.
	DisconnectOnExpiredPassword = "disconnect_on_expired_password"
	// SQLSelectLimit is the name for 'sql_select_limit' system variable.
	SQLSelectLimit = "sql_select_limit"
	// MaxConnectErrors is the name for 'max_connect_errors' system variable.
	MaxConnectErrors = "max_connect_errors"
	// TableDefinitionCache is the name for 'table_definition_cache' system variable.
	TableDefinitionCache = "table_definition_cache"
	// Timestamp is the name for 'timestamp' system variable.
	Timestamp = "timestamp"
	// ConnectTimeout is the name for 'connect_timeout' system variable.
	ConnectTimeout = "connect_timeout"
	// SyncBinlog is the name for 'sync_binlog' system variable.
	SyncBinlog = "sync_binlog"
	// BlockEncryptionMode is the name for 'block_encryption_mode' system variable.
	BlockEncryptionMode = "block_encryption_mode"
	// WaitTimeout is the name for 'wait_timeout' system variable.
	WaitTimeout = "wait_timeout"
	// Version is the name of 'version' system variable.
	Version = "version"
	// VersionComment is the name of 'version_comment' system variable.
	VersionComment = "version_comment"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
	// TiDBEnableDDL indicates whether the tidb-server campaigns the DDL owner,
	TiDBEnableDDL = "tidb_enable_ddl"
	// Port is the name for 'port' system variable.
	Port = "port"
	// DataDir is the name for 'datadir' system variable.
	DataDir = "datadir"
	// Profiling is the name for 'Profiling' system variable.
	Profiling = "profiling"
	// Socket is the name for 'socket' system variable.
	Socket = "socket"
	// BinlogOrderCommits is the name for 'binlog_order_commits' system variable.
	BinlogOrderCommits = "binlog_order_commits"
	// MasterVerifyChecksum is the name for 'master_verify_checksum' system variable.
	MasterVerifyChecksum = "master_verify_checksum"
	// SuperReadOnly is the name for 'super_read_only' system variable.
	SuperReadOnly = "super_read_only"
	// SQLNotes is the name for 'sql_notes' system variable.
	SQLNotes = "sql_notes"
	// QueryCacheType is the name for 'query_cache_type' system variable.
	QueryCacheType = "query_cache_type"
	// SlaveCompressedProtocol is the name for 'slave_compressed_protocol' system variable.
	SlaveCompressedProtocol = "slave_compressed_protocol"
	// BinlogRowQueryLogEvents is the name for 'binlog_rows_query_log_events' system variable.
	BinlogRowQueryLogEvents = "binlog_rows_query_log_events"
	// LogSlowSlaveStatements is the name for 'log_slow_slave_statements' system variable.
	LogSlowSlaveStatements = "log_slow_slave_statements"
	// LogSlowAdminStatements is the name for 'log_slow_admin_statements' system variable.
	LogSlowAdminStatements = "log_slow_admin_statements"
	// LogQueriesNotUsingIndexes is the name for 'log_queries_not_using_indexes' system variable.
	LogQueriesNotUsingIndexes = "log_queries_not_using_indexes"
	// QueryCacheWlockInvalidate is the name for 'query_cache_wlock_invalidate' system variable.
	QueryCacheWlockInvalidate = "query_cache_wlock_invalidate"
	// SQLAutoIsNull is the name for 'sql_auto_is_null' system variable.
	SQLAutoIsNull = "sql_auto_is_null"
	// RelayLogPurge is the name for 'relay_log_purge' system variable.
	RelayLogPurge = "relay_log_purge"
	// AutomaticSpPrivileges is the name for 'automatic_sp_privileges' system variable.
	AutomaticSpPrivileges = "automatic_sp_privileges"
	// SQLQuoteShowCreate is the name for 'sql_quote_show_create' system variable.
	SQLQuoteShowCreate = "sql_quote_show_create"
	// SlowQueryLog is the name for 'slow_query_log' system variable.
	SlowQueryLog = "slow_query_log"
	// BinlogDirectNonTransactionalUpdates is the name for 'binlog_direct_non_transactional_updates' system variable.
	BinlogDirectNonTransactionalUpdates = "binlog_direct_non_transactional_updates"
	// SQLBigSelects is the name for 'sql_big_selects' system variable.
	SQLBigSelects = "sql_big_selects"
	// LogBinTrustFunctionCreators is the name for 'log_bin_trust_function_creators' system variable.
	LogBinTrustFunctionCreators = "log_bin_trust_function_creators"
	// OldAlterTable is the name for 'old_alter_table' system variable.
	OldAlterTable = "old_alter_table"
	// EnforceGtidConsistency is the name for 'enforce_gtid_consistency' system variable.
	EnforceGtidConsistency = "enforce_gtid_consistency"
	// SecureAuth is the name for 'secure_auth' system variable.
	SecureAuth = "secure_auth"
	// UniqueChecks is the name for 'unique_checks' system variable.
	UniqueChecks = "unique_checks"
	// SQLWarnings is the name for 'sql_warnings' system variable.
	SQLWarnings = "sql_warnings"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// KeepFilesOnCreate is the name for 'keep_files_on_create' system variable.
	KeepFilesOnCreate = "keep_files_on_create"
	// ShowOldTemporals is the name for 'show_old_temporals' system variable.
	ShowOldTemporals = "show_old_temporals"
	// LocalInFile is the name for 'local_infile' system variable.
	LocalInFile = "local_infile"
	// PerformanceSchema is the name for 'performance_schema' system variable.
	PerformanceSchema = "performance_schema"
	// Flush is the name for 'flush' system variable.
	Flush = "flush"
	// SlaveAllowBatching is the name for 'slave_allow_batching' system variable.
	SlaveAllowBatching = "slave_allow_batching"
	// MyISAMUseMmap is the name for 'myisam_use_mmap' system variable.
	MyISAMUseMmap = "myisam_use_mmap"
	// InnodbFilePerTable is the name for 'innodb_file_per_table' system variable.
	InnodbFilePerTable = "innodb_file_per_table"
	// InnodbLogCompressedPages is the name for 'innodb_log_compressed_pages' system variable.
	InnodbLogCompressedPages = "innodb_log_compressed_pages"
	// InnodbPrintAllDeadlocks is the name for 'innodb_print_all_deadlocks' system variable.
	InnodbPrintAllDeadlocks = "innodb_print_all_deadlocks"
	// InnodbStrictMode is the name for 'innodb_strict_mode' system variable.
	InnodbStrictMode = "innodb_strict_mode"
	// InnodbCmpPerIndexEnabled is the name for 'innodb_cmp_per_index_enabled' system variable.
	InnodbCmpPerIndexEnabled = "innodb_cmp_per_index_enabled"
	// InnodbBufferPoolDumpAtShutdown is the name for 'innodb_buffer_pool_dump_at_shutdown' system variable.
	InnodbBufferPoolDumpAtShutdown = "innodb_buffer_pool_dump_at_shutdown"
	// InnodbAdaptiveHashIndex is the name for 'innodb_adaptive_hash_index' system variable.
	InnodbAdaptiveHashIndex = "innodb_adaptive_hash_index"
	// InnodbFtEnableStopword is the name for 'innodb_ft_enable_stopword' system variable.
	InnodbFtEnableStopword = "innodb_ft_enable_stopword" // #nosec G101
	// InnodbSupportXA is the name for 'innodb_support_xa' system variable.
	InnodbSupportXA = "innodb_support_xa"
	// InnodbOptimizeFullTextOnly is the name for 'innodb_optimize_fulltext_only' system variable.
	InnodbOptimizeFullTextOnly = "innodb_optimize_fulltext_only"
	// InnodbStatusOutputLocks is the name for 'innodb_status_output_locks' system variable.
	InnodbStatusOutputLocks = "innodb_status_output_locks"
	// InnodbBufferPoolDumpNow is the name for 'innodb_buffer_pool_dump_now' system variable.
	InnodbBufferPoolDumpNow = "innodb_buffer_pool_dump_now"
	// InnodbBufferPoolLoadNow is the name for 'innodb_buffer_pool_load_now' system variable.
	InnodbBufferPoolLoadNow = "innodb_buffer_pool_load_now"
	// InnodbStatsOnMetadata is the name for 'innodb_stats_on_metadata' system variable.
	InnodbStatsOnMetadata = "innodb_stats_on_metadata"
	// InnodbDisableSortFileCache is the name for 'innodb_disable_sort_file_cache' system variable.
	InnodbDisableSortFileCache = "innodb_disable_sort_file_cache"
	// InnodbStatsAutoRecalc is the name for 'innodb_stats_auto_recalc' system variable.
	InnodbStatsAutoRecalc = "innodb_stats_auto_recalc"
	// InnodbBufferPoolLoadAbort is the name for 'innodb_buffer_pool_load_abort' system variable.
	InnodbBufferPoolLoadAbort = "innodb_buffer_pool_load_abort"
	// InnodbStatsPersistent is the name for 'innodb_stats_persistent' system variable.
	InnodbStatsPersistent = "innodb_stats_persistent"
	// InnodbRandomReadAhead is the name for 'innodb_random_read_ahead' system variable.
	InnodbRandomReadAhead = "innodb_random_read_ahead"
	// InnodbAdaptiveFlushing is the name for 'innodb_adaptive_flushing' system variable.
	InnodbAdaptiveFlushing = "innodb_adaptive_flushing"
	// InnodbTableLocks is the name for 'innodb_table_locks' system variable.
	InnodbTableLocks = "innodb_table_locks"
	// InnodbStatusOutput is the name for 'innodb_status_output' system variable.
	InnodbStatusOutput = "innodb_status_output"
	// NetBufferLength is the name for 'net_buffer_length' system variable.
	NetBufferLength = "net_buffer_length"
	// QueryCacheSize is the name of 'query_cache_size' system variable.
	QueryCacheSize = "query_cache_size"
	// TxReadOnly is the name of 'tx_read_only' system variable.
	TxReadOnly = "tx_read_only"
	// TransactionReadOnly is the name of 'transaction_read_only' system variable.
	TransactionReadOnly = "transaction_read_only"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// InitConnect is the name of 'init_connect' system variable.
	InitConnect = "init_connect"
	// CollationServer is the name of 'collation_server' variable.
	CollationServer = "collation_server"
	// DefaultCollationForUTF8MB4 is the name of 'default_collation_for_utf8mb4' variable.
	DefaultCollationForUTF8MB4 = "default_collation_for_utf8mb4"
	// NetWriteTimeout is the name of 'net_write_timeout' variable.
	NetWriteTimeout = "net_write_timeout"
	// ThreadPoolSize is the name of 'thread_pool_size' variable.
	ThreadPoolSize = "thread_pool_size"
	// WindowingUseHighPrecision is the name of 'windowing_use_high_precision' system variable.
	WindowingUseHighPrecision = "windowing_use_high_precision"
	// OptimizerSwitch is the name of 'optimizer_switch' system variable.
	OptimizerSwitch = "optimizer_switch"
	// SystemTimeZone is the name of 'system_time_zone' system variable.
	SystemTimeZone = "system_time_zone"
	// CTEMaxRecursionDepth is the name of 'cte_max_recursion_depth' system variable.
	CTEMaxRecursionDepth = "cte_max_recursion_depth"
	// SQLModeVar is the name of the 'sql_mode' system variable.
	SQLModeVar = "sql_mode"
	// CharacterSetResults is the name of the 'character_set_results' system variable.
	CharacterSetResults = "character_set_results"
	// MaxAllowedPacket is the name of the 'max_allowed_packet' system variable.
	MaxAllowedPacket = "max_allowed_packet"
	// TimeZone is the name of the 'time_zone' system variable.
	TimeZone = "time_zone"
	// TxnIsolation is the name of the 'tx_isolation' system variable.
	TxnIsolation = "tx_isolation"
	// TransactionIsolation is the name of the 'transaction_isolation' system variable.
	TransactionIsolation = "transaction_isolation"
	// TxnIsolationOneShot is the name of the 'tx_isolation_one_shot' system variable.
	TxnIsolationOneShot = "tx_isolation_one_shot"
	// MaxExecutionTime is the name of the 'max_execution_time' system variable.
	MaxExecutionTime = "max_execution_time"
	// TiKVClientReadTimeout is the name of the 'tikv_client_read_timeout' system variable.
	TiKVClientReadTimeout = "tikv_client_read_timeout"
	// TiDBLoadBindingTimeout is the name of the 'tidb_load_binding_timeout' system variable.
	TiDBLoadBindingTimeout = "tidb_load_binding_timeout"
	// ReadOnly is the name of the 'read_only' system variable.
	ReadOnly = "read_only"
	// DefaultAuthPlugin is the name of 'default_authentication_plugin' system variable.
	DefaultAuthPlugin = "default_authentication_plugin"
	// LastInsertID is the name of 'last_insert_id' system variable.
	LastInsertID = "last_insert_id"
	// Identity is the name of 'identity' system variable.
	Identity = "identity"
	// TiDBAllowFunctionForExpressionIndex is the name of `TiDBAllowFunctionForExpressionIndex` system variable.
	TiDBAllowFunctionForExpressionIndex = "tidb_allow_function_for_expression_index"
	// RandSeed1 is the name of 'rand_seed1' system variable.
	RandSeed1 = "rand_seed1"
	// RandSeed2 is the name of 'rand_seed2' system variable.
	RandSeed2 = "rand_seed2"
	// SQLRequirePrimaryKey is the name of `sql_require_primary_key` system variable.
	SQLRequirePrimaryKey = "sql_require_primary_key"
	// ValidatePasswordEnable turns on/off the validation of password.
	ValidatePasswordEnable = "validate_password.enable"
	// ValidatePasswordPolicy specifies the password policy enforced by validate_password.
	ValidatePasswordPolicy = "validate_password.policy"
	// ValidatePasswordCheckUserName controls whether validate_password compares passwords to the user name part of
	// the effective user account for the current session
	ValidatePasswordCheckUserName = "validate_password.check_user_name"
	// ValidatePasswordLength specified the minimum number of characters that validate_password requires passwords to have
	ValidatePasswordLength = "validate_password.length"
	// ValidatePasswordMixedCaseCount specified the minimum number of lowercase and uppercase characters that validate_password requires
	ValidatePasswordMixedCaseCount = "validate_password.mixed_case_count"
	// ValidatePasswordNumberCount specified the minimum number of numeric (digit) characters that validate_password requires
	ValidatePasswordNumberCount = "validate_password.number_count"
	// ValidatePasswordSpecialCharCount specified the minimum number of nonalphanumeric characters that validate_password requires
	ValidatePasswordSpecialCharCount = "validate_password.special_char_count"
	// ValidatePasswordDictionary specified the dictionary that validate_password uses for checking passwords. Each word is separated by semicolon (;).
	ValidatePasswordDictionary = "validate_password.dictionary"
)
