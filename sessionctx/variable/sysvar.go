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
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/tikvutil"
	"github.com/pingcap/tidb/util/tls"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/versioninfo"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tikvstore "github.com/tikv/client-go/v2/kv"
	atomic2 "go.uber.org/atomic"
)

// All system variables declared here are ordered by their scopes, which follow the order of scopes below:
// 		[NONE, SESSION, INSTANCE, GLOBAL, GLOBAL & SESSION]
// If you are adding a new system variable, please put it in the corresponding area.
var defaultSysVars = []*SysVar{
	/* The system variables below have NONE scope  */
	{Scope: ScopeNone, Name: SystemTimeZone, Value: "CST"},
	{Scope: ScopeNone, Name: Hostname, Value: DefHostname},
	{Scope: ScopeNone, Name: Port, Value: "4000", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: ScopeNone, Name: LogBin, Value: Off, Type: TypeBool},
	{Scope: ScopeNone, Name: VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible"},
	{Scope: ScopeNone, Name: Version, Value: mysql.ServerVersion},
	{Scope: ScopeNone, Name: DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: ScopeNone, Name: Socket, Value: ""},
	{Scope: ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: ScopeNone, Name: "have_ssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "have_openssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: ScopeNone, Name: "version_compile_os", Value: runtime.GOOS},
	{Scope: ScopeNone, Name: "version_compile_machine", Value: runtime.GOARCH},
	/* TiDB specific variables */
	{Scope: ScopeNone, Name: TiDBEnableEnhancedSecurity, Value: Off, Type: TypeBool},
	{Scope: ScopeNone, Name: TiDBAllowFunctionForExpressionIndex, ReadOnly: true, Value: collectAllowFuncName4ExpressionIndex()},

	/* The system variables below have SESSION scope  */
	{Scope: ScopeSession, Name: Timestamp, Value: DefTimestamp, skipInit: true, MinValue: 0, MaxValue: 2147483647, Type: TypeFloat, GetSession: func(s *SessionVars) (string, error) {
		if timestamp, ok := s.systems[Timestamp]; ok && timestamp != DefTimestamp {
			return timestamp, nil
		}
		timestamp := s.StmtCtx.GetOrStoreStmtCache(stmtctx.StmtNowTsCacheKey, time.Now()).(time.Time)
		return types.ToString(float64(timestamp.UnixNano()) / float64(time.Second))
	}},
	{Scope: ScopeSession, Name: WarningCount, Value: "0", ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(s.SysWarningCount), nil
	}},
	{Scope: ScopeSession, Name: ErrorCount, Value: "0", ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(int(s.SysErrorCount)), nil
	}},
	{Scope: ScopeSession, Name: LastInsertID, Value: "", skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}},
	{Scope: ScopeSession, Name: Identity, Value: "", skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
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
	{Scope: ScopeSession, Name: TiDBSnapshot, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptProjectionPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown), skipInit: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowProjectionPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptAggPushDown, Value: BoolToOnOff(DefOptAggPushDown), Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.AllowAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), skipInit: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptWriteRowID, Value: BoolToOnOff(DefOptWriteRowID), skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.AllowWriteRowID = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBChecksumTableConcurrency, skipInit: true, Value: strconv.Itoa(DefChecksumTableConcurrency)},
	{Scope: ScopeSession, Name: TiDBBatchInsert, Value: BoolToOnOff(DefBatchInsert), Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.BatchInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchDelete, Value: BoolToOnOff(DefBatchDelete), Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.BatchDelete = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchCommit, Value: BoolToOnOff(DefBatchCommit), Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.BatchCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBCurrentTS, Value: strconv.Itoa(DefCurretTS), ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.TxnCtx.StartTS, 10), nil
	}},
	{Scope: ScopeSession, Name: TiDBLastTxnInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		return s.LastTxnInfo, nil
	}},
	{Scope: ScopeSession, Name: TiDBLastQueryInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastQueryInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableChunkRPC, Value: On, Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.EnableChunkRPC = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TxnIsolationOneShot, Value: "", skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(DefTiDBOptimizerSelectivityLevel), skipInit: true, Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptimizerEnableOuterJoinReorder, Value: BoolToOnOff(DefTiDBEnableOuterJoinReorder), skipInit: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableOuterJoinReorder = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBLogFileMaxDays, Value: strconv.Itoa(config.GetGlobalConfig().Log.File.MaxDays), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		maxAge, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}

		GlobalLogMaxDays.Store(int32(maxAge))

		cfg := config.GetGlobalConfig().Log.ToLogConfig()
		cfg.Config.File.MaxDays = int(maxAge)

		err = logutil.ReplaceLogger(cfg)
		if err != nil {
			return err
		}

		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatInt(int64(GlobalLogMaxDays.Load()), 10), nil
	}},
	{Scope: ScopeSession, Name: TiDBConfig, Value: "", ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		conf := config.GetGlobalConfig()
		j, err := json.MarshalIndent(conf, "", "\t")
		if err != nil {
			return "", err
		}
		return config.HideConfig(string(j)), nil
	}},
	{Scope: ScopeSession, Name: TiDBDDLReorgPriority, Value: "PRIORITY_LOW", skipInit: true, SetSession: func(s *SessionVars, val string) error {
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
	{Scope: ScopeSession, Name: TiDBLowResolutionTSO, Value: Off, Type: TypeBool, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.LowResolutionTSO = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBAllowRemoveAutoInc, Value: BoolToOnOff(DefTiDBAllowRemoveAutoInc), skipInit: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
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
				s.IsolationReadEngines[kv.TiFlash] = struct{}{}
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
	{Scope: ScopeSession, Name: TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(DefTiDBMetricSchemaRangeDuration), skipInit: true, Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaRangeDuration = TidbOptInt64(val, DefTiDBMetricSchemaRangeDuration)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInPlanCache, Value: BoolToOnOff(DefTiDBFoundInPlanCache), Type: TypeBool, ReadOnly: true, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.FoundInPlanCache = TiDBOptOn(val)
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInPlanCache), nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInBinding, Value: BoolToOnOff(DefTiDBFoundInBinding), Type: TypeBool, ReadOnly: true, skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.FoundInBinding = TiDBOptOn(val)
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInBinding), nil
	}},
	{Scope: ScopeSession, Name: RandSeed1, Type: TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed1(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}},
	{Scope: ScopeSession, Name: RandSeed2, Type: TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed2(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
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
	{Scope: ScopeSession, Name: TiDBLastDDLInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true, skipInit: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastDDLInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},

	/* The system variables below have INSTANCE scope  */
	{Scope: ScopeInstance, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool, skipInit: true, SetGlobal: func(s *SessionVars, val string) error {
		ProcessGeneralLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(ProcessGeneralLog.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), skipInit: true, Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetGlobal: func(s *SessionVars, val string) error {
		atomic.StoreUint64(&config.GetGlobalConfig().Instance.SlowThreshold, uint64(TidbOptInt64(val, logutil.DefaultSlowThreshold)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), skipInit: true, Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		atomic.StoreUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog, uint32(TidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
		return BoolToOnOff(enabled), nil
	}},
	{Scope: ScopeInstance, Name: TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: TypeBool, skipInit: true, SetGlobal: func(s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.EnableSlowLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), skipInit: true, Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBPProfSQLCPU, Value: strconv.Itoa(DefTiDBPProfSQLCPU), Type: TypeInt, skipInit: true, MinValue: 0, MaxValue: 1, SetGlobal: func(s *SessionVars, val string) error {
		EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, DefTiDBPProfSQLCPU)) > 0)
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		val := "0"
		if EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, nil
	}},
	{Scope: ScopeInstance, Name: TiDBDDLSlowOprThreshold, Value: strconv.Itoa(DefTiDBDDLSlowOprThreshold), skipInit: true, SetGlobal: func(s *SessionVars, val string) error {
		atomic.StoreUint32(&DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, DefTiDBDDLSlowOprThreshold)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBForcePriority, skipInit: true, Value: mysql.Priority2Str[DefTiDBForcePriority], SetGlobal: func(s *SessionVars, val string) error {
		atomic.StoreInt32(&ForcePriority, int32(mysql.Str2Priority(val)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], nil
	}},
	{Scope: ScopeInstance, Name: TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveQueryTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(s *SessionVars, val string) error {
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveQueryTimeThreshold)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&ExpensiveQueryTimeThreshold), 10), nil
	}},
	{Scope: ScopeInstance, Name: TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(config.GetGlobalConfig().Instance.MemoryUsageAlarmRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0, skipInit: true, SetGlobal: func(s *SessionVars, val string) error {
		MemoryUsageAlarmRatio.Store(tidbOptFloat64(val, 0.8))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), nil
	}},
	{Scope: ScopeInstance, Name: TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(DefTiDBEnableCollectExecutionInfo), skipInit: true, Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		oldConfig := config.GetGlobalConfig()
		newValue := TiDBOptOn(val)
		if oldConfig.Instance.EnableCollectExecutionInfo != newValue {
			newConfig := *oldConfig
			newConfig.Instance.EnableCollectExecutionInfo = newValue
			config.StoreGlobalConfig(&newConfig)
		}
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableCollectExecutionInfo), nil
	}},
	{Scope: ScopeInstance, Name: PluginLoad, Value: "", ReadOnly: true, GetGlobal: func(s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginLoad, nil
	}},
	{Scope: ScopeInstance, Name: PluginDir, Value: "/data/deploy/plugin", ReadOnly: true, GetGlobal: func(s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginDir, nil
	}},

	/* The system variables below have GLOBAL scope  */
	{Scope: ScopeGlobal, Name: MaxPreparedStmtCount, Value: strconv.FormatInt(DefMaxPreparedStmtCount, 10), Type: TypeInt, MinValue: -1, MaxValue: 1048576},
	{Scope: ScopeGlobal, Name: InitConnect, Value: ""},
	/* TiDB specific variables */
	{Scope: ScopeGlobal, Name: TiDBTSOClientBatchMaxWaitTime, Value: strconv.FormatFloat(DefTiDBTSOClientBatchMaxWaitTime, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 10,
		GetGlobal: func(sv *SessionVars) (string, error) {
			return strconv.FormatFloat(MaxTSOBatchWaitInterval.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			MaxTSOBatchWaitInterval.Store(tidbOptFloat64(val, DefTiDBTSOClientBatchMaxWaitTime))
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBEnableTSOFollowerProxy, Value: BoolToOnOff(DefTiDBEnableTSOFollowerProxy), Type: TypeBool, GetGlobal: func(sv *SessionVars) (string, error) {
		return BoolToOnOff(EnableTSOFollowerProxy.Load()), nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		EnableTSOFollowerProxy.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableLocalTxn, Value: BoolToOnOff(DefTiDBEnableLocalTxn), Hidden: true, Type: TypeBool, GetGlobal: func(sv *SessionVars) (string, error) {
		return BoolToOnOff(EnableLocalTxn.Load()), nil
	}, SetGlobal: func(s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeRatio, Value: strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeStartTime, Value: DefAutoAnalyzeStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeEndTime, Value: DefAutoAnalyzeEndTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBMemQuotaBindingCache, Value: strconv.FormatInt(DefTiDBMemQuotaBindingCache, 10), Type: TypeUnsigned, MaxValue: math.MaxInt32, GetGlobal: func(sv *SessionVars) (string, error) {
		return strconv.FormatInt(MemQuotaBindingCache.Load(), 10), nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		MemQuotaBindingCache.Store(TidbOptInt64(val, DefTiDBMemQuotaBindingCache))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBRowFormatVersion, Value: strconv.Itoa(DefTiDBRowFormatV1), Type: TypeUnsigned, MinValue: 1, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		formatVersion := int(TidbOptInt64(val, DefTiDBRowFormatV1))
		if formatVersion == DefTiDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
		SetDDLReorgRowFormat(TidbOptInt64(val, DefTiDBRowFormatV2))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgWorkerCount, Value: strconv.Itoa(DefTiDBDDLReorgWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgBatchSize, Value: strconv.Itoa(DefTiDBDDLReorgBatchSize), Type: TypeUnsigned, MinValue: int64(MinDDLReorgBatchSize), MaxValue: uint64(MaxDDLReorgBatchSize), SetSession: func(s *SessionVars, val string) error {
		SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgBatchSize)))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBDDLErrorCountLimit, Value: strconv.Itoa(DefTiDBDDLErrorCountLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		SetDDLErrorCountLimit(TidbOptInt64(val, DefTiDBDDLErrorCountLimit))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(DefTiDBMaxDeltaSchemaCount), Type: TypeUnsigned, MinValue: 100, MaxValue: 16384, SetSession: func(s *SessionVars, val string) error {
		// It's a global variable, but it also wants to be cached in server.
		SetMaxDeltaSchemaCount(TidbOptInt64(val, DefTiDBMaxDeltaSchemaCount))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableChangeMultiSchema, Value: BoolToOnOff(DefTiDBChangeMultiSchema), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChangeMultiSchema = TiDBOptOn(val)
		return nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		s.EnableChangeMultiSchema = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnablePointGetCache, Value: BoolToOnOff(DefTiDBPointGetCache), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePointGetCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBScatterRegion, Value: BoolToOnOff(DefTiDBScatterRegion), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableStmtSummary, Value: BoolToOnOff(DefTiDBEnableStmtSummary), Type: TypeBool, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			return stmtsummary.StmtSummaryByDigestMap.SetEnabled(TiDBOptOn(val))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(DefTiDBStmtSummaryInternalQuery), Type: TypeBool, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			return stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(TiDBOptOn(val))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(DefTiDBStmtSummaryRefreshInterval), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			// convert val to int64
			return stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(TidbOptInt64(val, DefTiDBStmtSummaryRefreshInterval))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryHistorySize, Value: strconv.Itoa(DefTiDBStmtSummaryHistorySize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			return stmtsummary.StmtSummaryByDigestMap.SetHistorySize(TidbOptInt(val, DefTiDBStmtSummaryHistorySize))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryMaxStmtCount, Value: strconv.Itoa(DefTiDBStmtSummaryMaxStmtCount), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt16, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			return stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(uint(TidbOptInt(val, DefTiDBStmtSummaryMaxStmtCount)))
		}},
	{Scope: ScopeGlobal, Name: TiDBStmtSummaryMaxSQLLength, Value: strconv.Itoa(DefTiDBStmtSummaryMaxSQLLength), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(s *SessionVars, val string) error {
			return stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(TidbOptInt(val, DefTiDBStmtSummaryMaxSQLLength))
		}},
	{Scope: ScopeGlobal, Name: TiDBCapturePlanBaseline, Value: DefTiDBCapturePlanBaseline, Type: TypeBool, AllowEmptyAll: true},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskStartTime, Value: DefTiDBEvolvePlanTaskStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskEndTime, Value: DefTiDBEvolvePlanTaskEndTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatInt(tikvstore.StoreLimit.Load(), 10), nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		tikvstore.StoreLimit.Store(TidbOptInt64(val, DefTiDBStoreLimit))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTxnCommitBatchSize, Value: strconv.FormatUint(tikvstore.DefTxnCommitBatchSize, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 1 << 30,
		GetGlobal: func(sv *SessionVars) (string, error) {
			return strconv.FormatUint(tikvstore.TxnCommitBatchSize.Load(), 10), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			tikvstore.TxnCommitBatchSize.Store(uint64(TidbOptInt64(val, int64(tikvstore.DefTxnCommitBatchSize))))
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBRestrictedReadOnly, Value: BoolToOnOff(DefTiDBRestrictedReadOnly), Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		// For user initiated SET GLOBAL, also change the value of TiDBSuperReadOnly
		if on && s.StmtCtx.StmtType == "Set" {
			err := s.GlobalVarsAccessor.SetGlobalSysVar(TiDBSuperReadOnly, "ON")
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
	}, SetGlobal: func(s *SessionVars, val string) error {
		VarTiDBSuperReadOnly.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableTelemetry, Value: BoolToOnOff(DefTiDBEnableTelemetry), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableHistoricalStats, Value: Off, Type: TypeBool},
	/* tikv gc metrics */
	{Scope: ScopeGlobal, Name: TiDBGCEnable, Value: On, Type: TypeBool, GetGlobal: func(s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_enable", On)
	}, SetGlobal: func(s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_enable", val, "Current GC enable status")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCRunInterval, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_run_interval", "10m0s")
	}, SetGlobal: func(s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_run_interval", val, "GC run interval, at least 10m, in Go format.")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCLifetime, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_life_time", "10m0s")
	}, SetGlobal: func(s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_life_time", val, "All versions within life time will not be collected by GC, at least 10m, in Go format.")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCConcurrency, Value: "-1", Type: TypeInt, MinValue: 1, MaxValue: MaxConfigurableConcurrency, AllowAutoValue: true, GetGlobal: func(s *SessionVars) (string, error) {
		autoConcurrencyVal, err := getTiDBTableValue(s, "tikv_gc_auto_concurrency", On)
		if err == nil && autoConcurrencyVal == On {
			return "-1", nil // convention for "AUTO"
		}
		return getTiDBTableValue(s, "tikv_gc_concurrency", "-1")
	}, SetGlobal: func(s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal, Name: TiDBGCScanLockMode, Value: "LEGACY", Type: TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}, GetGlobal: func(s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_scan_lock_mode", "LEGACY")
	}, SetGlobal: func(s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_scan_lock_mode", val, "Mode of scanning locks, \"physical\" or \"legacy\"")
	}},
	{Scope: ScopeGlobal, Name: TiDBGCMaxWaitTime, Value: strconv.Itoa(DefTiDBGCMaxWaitTime), Type: TypeInt, MinValue: 600, MaxValue: 31536000, SetGlobal: func(s *SessionVars, val string) error {
		GCMaxWaitTime.Store(TidbOptInt64(val, DefTiDBGCMaxWaitTime))
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTableCacheLease, Value: strconv.Itoa(DefTiDBTableCacheLease), Type: TypeUnsigned, MinValue: 1, MaxValue: 10, SetGlobal: func(s *SessionVars, sVal string) error {
		var val int64
		val, err := strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		TableCacheLease.Store(val)
		return nil
	}},
	// variable for top SQL feature.
	// TopSQL enable only be controlled by TopSQL pub/sub sinker.
	// This global variable only uses to update the global config which store in PD(ETCD).
	{Scope: ScopeGlobal, Name: TiDBEnableTopSQL, Value: BoolToOnOff(topsqlstate.DefTiDBTopSQLEnable), Type: TypeBool, AllowEmpty: true, GlobalConfigName: GlobalConfigEnableTopSQL},
	{Scope: ScopeGlobal, Name: TiDBTopSQLMaxTimeSeriesCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxTimeSeriesCount), Type: TypeInt, MinValue: 1, MaxValue: 5000, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxStatementCount.Load(), 10), nil
	}, SetGlobal: func(vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxStatementCount.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBTopSQLMaxMetaCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxMetaCount), Type: TypeInt, MinValue: 1, MaxValue: 10000, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxCollect.Load(), 10), nil
	}, SetGlobal: func(vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxCollect.Store(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: SkipNameResolve, Value: Off, Type: TypeBool},
	{Scope: ScopeGlobal, Name: DefaultAuthPlugin, Value: mysql.AuthNativePassword, Type: TypeEnum, PossibleValues: []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password}},
	{Scope: ScopeGlobal, Name: TiDBPersistAnalyzeOptions, Value: BoolToOnOff(DefTiDBPersistAnalyzeOptions), skipInit: true, Type: TypeBool,
		GetGlobal: func(s *SessionVars) (string, error) {
			return BoolToOnOff(PersistAnalyzeOptions.Load()), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			PersistAnalyzeOptions.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableAutoAnalyze, Value: BoolToOnOff(DefTiDBEnableAutoAnalyze), Type: TypeBool,
		GetGlobal: func(s *SessionVars) (string, error) {
			return BoolToOnOff(RunAutoAnalyze.Load()), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			RunAutoAnalyze.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableColumnTracking, Value: BoolToOnOff(DefTiDBEnableColumnTracking), skipInit: true, Type: TypeBool, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(EnableColumnTracking.Load()), nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		v := TiDBOptOn(val)
		// If this is a user initiated statement,
		// we log that column tracking is disabled.
		if s.StmtCtx.StmtType == "Set" && !v {
			// Set the location to UTC to avoid time zone interference.
			disableTime := time.Now().UTC().Format(types.UTCTimeFormat)
			if err := setTiDBTableValue(s, TiDBDisableColumnTrackingTime, disableTime, "Record the last time tidb_enable_column_tracking is set off"); err != nil {
				return err
			}
		}
		EnableColumnTracking.Store(v)
		return nil
	}},
	{Scope: ScopeGlobal, Name: RequireSecureTransport, Value: BoolToOnOff(DefRequireSecureTransport), Type: TypeBool,
		GetGlobal: func(s *SessionVars) (string, error) {
			return BoolToOnOff(tls.RequireSecureTransport.Load()), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			tls.RequireSecureTransport.Store(TiDBOptOn(val))
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && TiDBOptOn(normalizedValue) {
				// Refuse to set RequireSecureTransport to ON if the connection
				// issuing the change is not secure. This helps reduce the chance of users being locked out.
				if vars.TLSConnectionState == nil {
					return "", errors.New("require_secure_transport can only be set to ON if the connection issuing the change is secure")
				}
			}
			return normalizedValue, nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBStatsLoadPseudoTimeout, Value: BoolToOnOff(DefTiDBStatsLoadPseudoTimeout), skipInit: true, Type: TypeBool,
		GetGlobal: func(s *SessionVars) (string, error) {
			return strconv.FormatBool(StatsLoadPseudoTimeout.Load()), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			StatsLoadPseudoTimeout.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnableBatchDML, Value: BoolToOnOff(DefTiDBEnableBatchDML), Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		EnableBatchDML.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(EnableBatchDML.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBStatsCacheMemQuota, Value: strconv.Itoa(DefTiDBStatsCacheMemQuota),
		MinValue: 0, MaxValue: MaxTiDBStatsCacheMemQuota, Type: TypeInt,
		GetGlobal: func(vars *SessionVars) (string, error) {
			return strconv.FormatInt(StatsCacheMemQuota.Load(), 10), nil
		}, SetGlobal: func(vars *SessionVars, s string) error {
			v := TidbOptInt64(s, DefTiDBStatsCacheMemQuota)
			oldv := StatsCacheMemQuota.Load()
			if v != oldv {
				StatsCacheMemQuota.Store(v)
				SetStatsCacheCapacity.Load().(func(int64))(v)
			}
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBQueryLogMaxLen, Value: strconv.Itoa(DefTiDBQueryLogMaxLen), Type: TypeInt, MinValue: 0, MaxValue: 1073741824, SetGlobal: func(s *SessionVars, val string) error {
		QueryLogMaxLen.Store(int32(TidbOptInt64(val, DefTiDBQueryLogMaxLen)))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return fmt.Sprint(QueryLogMaxLen.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBCommitterConcurrency, Value: strconv.Itoa(DefTiDBCommitterConcurrency), Type: TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(s *SessionVars, val string) error {
		tikvutil.CommitterConcurrency.Store(int32(TidbOptInt64(val, DefTiDBCommitterConcurrency)))
		cfg := config.GetGlobalConfig().GetTiKVConfig()
		tikvcfg.StoreGlobalConfig(cfg)
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return fmt.Sprint(tikvutil.CommitterConcurrency.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMemQuotaAnalyze, Value: strconv.Itoa(DefTiDBMemQuotaAnalyze), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64,
		GetGlobal: func(s *SessionVars) (string, error) {
			return strconv.FormatInt(GetMemQuotaAnalyze(), 10), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			SetMemQuotaAnalyze(TidbOptInt64(val, DefTiDBMemQuotaAnalyze))
			return nil
		},
	},
	{Scope: ScopeGlobal, Name: TiDBEnablePrepPlanCache, Value: BoolToOnOff(DefTiDBEnablePrepPlanCache), Type: TypeBool, SetGlobal: func(s *SessionVars, val string) error {
		EnablePreparedPlanCache.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return BoolToOnOff(EnablePreparedPlanCache.Load()), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBPrepPlanCacheSize, Value: strconv.FormatUint(uint64(DefTiDBPrepPlanCacheSize), 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, SetGlobal: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			PreparedPlanCacheSize.Store(uVal)
		}
		return err
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(PreparedPlanCacheSize.Load(), 10), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBPrepPlanCacheMemoryGuardRatio, Value: strconv.FormatFloat(DefTiDBPrepPlanCacheMemoryGuardRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(s *SessionVars, val string) error {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			PreparedPlanCacheMemoryGuardRatio.Store(f)
		}
		return err
	}, GetGlobal: func(s *SessionVars) (string, error) {
		return strconv.FormatFloat(PreparedPlanCacheMemoryGuardRatio.Load(), 'f', -1, 64), nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMemOOMAction, Value: DefTiDBMemOOMAction, PossibleValues: []string{"CANCEL", "LOG"}, Type: TypeEnum,
		GetGlobal: func(s *SessionVars) (string, error) {
			return OOMAction.Load(), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			OOMAction.Store(val)
			return nil
		}},
	{Scope: ScopeGlobal, Name: TiDBMaxAutoAnalyzeTime, Value: strconv.Itoa(DefTiDBMaxAutoAnalyzeTime), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(s *SessionVars) (string, error) {
			return strconv.FormatInt(MaxAutoAnalyzeTime.Load(), 10), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				MaxAutoAnalyzeTime.Store(num)
			}
			return err
		},
	},

	/* The system variables below have GLOBAL and SESSION scope  */
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSelectLimit, Value: "18446744073709551615", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultWeekFormat, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 7},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLModeVar, Value: mysql.DefaultSQLMode, IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxExecutionTime, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
		timeoutMS := tidbOptPositiveInt32(val, 0)
		s.MaxExecutionTime = uint64(timeoutMS)
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
	{Scope: ScopeGlobal | ScopeSession, Name: SQLLogBin, Value: On, Type: TypeBool, skipInit: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TimeZone, Value: "SYSTEM", IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if strings.EqualFold(normalizedValue, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(normalizedValue)
		return normalizedValue, err
	}, SetSession: func(s *SessionVars, val string) error {
		tz, err := parseTimeZone(val)
		if err != nil {
			return err
		}
		s.TimeZone = tz
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: ForeignKeyChecks, Value: Off, Type: TypeBool, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			// TiDB does not yet support foreign keys.
			// Return the original value in the warning, so that users are not confused.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue))
			return Off, nil
		} else if !TiDBOptOn(normalizedValue) {
			return Off, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue)
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
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetClient, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetClient)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetResults, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetInTxn(false)
		}
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
	{Scope: ScopeGlobal | ScopeSession, Name: GroupConcatMaxLen, Value: "1024", IsHintUpdatable: true, skipInit: true, Type: TypeUnsigned, MinValue: 4, MaxValue: math.MaxUint64, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
		// Minimum Value 4
		// Maximum Value (64-bit platforms) 18446744073709551615
		// Maximum Value (32-bit platforms) 4294967295
		if mathutil.IntBits == 32 {
			if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
				if val > uint64(math.MaxUint32) {
					vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(GroupConcatMaxLen, originalValue))
					return strconv.FormatInt(int64(math.MaxUint32), 10), nil
				}
			}
		}
		return normalizedValue, nil
	}},
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
				vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(MaxAllowedPacket, normalizedValue))
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
	{Scope: ScopeGlobal | ScopeSession, Name: WindowingUseHighPrecision, Value: On, Type: TypeBool, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
		s.WindowingUseHighPrecision = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: BlockEncryptionMode, Value: "aes-128-ecb"},
	/* TiDB specific variables */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowMPPExecution, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowMPPExecution), SetSession: func(s *SessionVars, val string) error {
		s.allowMPPExecution = TiDBOptOn(val)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildStatsConcurrency, skipInit: true, Value: strconv.Itoa(DefBuildStatsConcurrency)},
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(DefOptInSubqToJoinAndAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptPreferRangeScan, Value: BoolToOnOff(DefOptPreferRangeScan), Type: TypeBool, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
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
		s.CPUFactor = tidbOptFloat64(val, DefOptCPUFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), skipInit: true, Type: TypeFloat, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopTiFlashConcurrencyFactor = tidbOptFloat64(val, DefOptTiFlashConcurrencyFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCopCPUFactor, Value: strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopCPUFactor = tidbOptFloat64(val, DefOptCopCPUFactor)
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
		s.MemoryFactor = tidbOptFloat64(val, DefOptMemoryFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDiskFactor, Value: strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.DiskFactor = tidbOptFloat64(val, DefOptDiskFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptimizerEnableNewOnlyFullGroupByCheck, Value: BoolToOnOff(DefTiDBOptimizerEnableNewOFGB), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNewOnlyFullGroupByCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.ConcurrencyFactor = tidbOptFloat64(val, DefOptConcurrencyFactor)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRetryLimit, Value: strconv.Itoa(DefTiDBRetryLimit), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RetryLimit = TidbOptInt64(val, DefTiDBRetryLimit)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDisableTxnAutoRetry, Value: BoolToOnOff(DefTiDBDisableTxnAutoRetry), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.DisableTxnAutoRetry = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBConstraintCheckInPlace, Value: BoolToOnOff(DefTiDBConstraintCheckInPlace), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ConstraintCheckInPlace = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnMode, Value: DefTiDBTxnMode, AllowEmptyAll: true, Type: TypeEnum, PossibleValues: []string{"pessimistic", "optimistic"}, SetSession: func(s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableFastAnalyze, Value: BoolToOnOff(DefTiDBUseFastAnalyze), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
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
			switch engine {
			case kv.TiFlash.Name():
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptJoinReorderThreshold, Value: strconv.Itoa(DefTiDBOptJoinReorderThreshold), skipInit: true, Type: TypeUnsigned, MinValue: 0, MaxValue: 63, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNoopFuncs, Value: DefTiDBEnableNoopFuncs, Type: TypeEnum, PossibleValues: []string{Off, On, Warn}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {

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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBReplicaRead, Value: "leader", Type: TypeEnum, PossibleValues: []string{"leader", "follower", "leader-and-follower", "closest-replicas"}, SetSession: func(s *SessionVars, val string) error {
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(kv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(kv.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(kv.ReplicaReadLeader)
		} else if strings.EqualFold(val, "closest-replicas") {
			s.SetReplicaRead(kv.ReplicaReadClosest)
		}
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableClusteredIndex, Value: IntOnly, Type: TypeEnum, PossibleValues: []string{Off, On, IntOnly}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == IntOnly {
			vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(normalizedValue, fmt.Sprintf("'%s' or '%s'", On, Off)))
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableClusteredIndex = TiDBOptEnableClustered(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPartitionPruneMode, Value: DefTiDBPartitionPruneMode, Type: TypeStr, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
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
			s.StmtCtx.AppendWarning(errors.New("Please analyze all partition tables again for consistency between partition and global stats"))
			s.StmtCtx.AppendWarning(errors.New("Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
		}
		s.PartitionPruneMode.Store(newMode)
		return nil
	}, SetGlobal: func(s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.New("Please analyze all partition tables again for consistency between partition and global stats"))
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRedactLog, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableRedactLog = TiDBOptOn(val)
		errors.RedactLogEnabled.Store(s.EnableRedactLog)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBShardAllocateStep, Value: strconv.Itoa(DefTiDBShardAllocateStep), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), SetSession: func(s *SessionVars, val string) error {
		s.ShardAllocateStep = TidbOptInt64(val, DefTiDBShardAllocateStep)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAmendPessimisticTxn, Value: BoolToOnOff(DefTiDBEnableAmendPessimisticTxn), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAmendPessimisticTxn = TiDBOptOn(val)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeVersion, Value: strconv.Itoa(DefTiDBAnalyzeVersion), Type: TypeInt, MinValue: 1, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "2" && FeedbackProbability != nil && FeedbackProbability.Load() > 0 {
			var original string
			var err error
			if scope == ScopeGlobal {
				original, err = vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBAnalyzeVersion)
				if err != nil {
					return normalizedValue, nil
				}
			} else {
				original = strconv.Itoa(vars.AnalyzeVersion)
			}
			vars.StmtCtx.AppendError(errors.New("variable tidb_analyze_version not updated because analyze version 2 is incompatible with query feedback. Please consider setting feedback-probability to 0.0 in config file to disable query feedback"))
			return original, nil
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AnalyzeVersion = tidbOptPositiveInt32(val, DefTiDBAnalyzeVersion)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExchangePartition, Value: BoolToOnOff(DefTiDBEnableExchangePartition), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TiDBEnableExchangePartition = TiDBOptOn(val)
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnablePaging, Value: Off, Type: TypeBool, Hidden: true, SetSession: func(s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableLegacyInstanceScope, Value: BoolToOnOff(DefEnableLegacyInstanceScope), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLegacyInstanceScope = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStatsLoadSyncWait, Value: strconv.Itoa(DefTiDBStatsLoadSyncWait), skipInit: true, Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		SetSession: func(s *SessionVars, val string) error {
			s.StatsLoadSyncWait = TidbOptInt64(val, DefTiDBStatsLoadSyncWait)
			return nil
		},
		GetGlobal: func(s *SessionVars) (string, error) {
			return strconv.FormatInt(StatsLoadSyncWait.Load(), 10), nil
		},
		SetGlobal: func(s *SessionVars, val string) error {
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
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNewCostInterface, Value: BoolToOnOff(false), Hidden: true, Type: TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableNewCostInterface = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRCReadCheckTS, Type: TypeBool, Value: BoolToOnOff(DefRCReadCheckTS), SetSession: func(s *SessionVars, val string) error {
		s.RcReadCheckTS = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRemoveOrderbyInSubquery, Value: BoolToOnOff(DefTiDBRemoveOrderbyInSubquery), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RemoveOrderbyInSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaQuery, Value: strconv.Itoa(DefTiDBMemQuotaQuery), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaQuery = TidbOptInt64(val, DefTiDBMemQuotaQuery)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		intVal := TidbOptInt64(normalizedValue, DefTiDBMemQuotaQuery)
		if intVal > 0 && intVal < 128 {
			vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(TiDBMemQuotaQuery, originalValue))
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
}

// FeedbackProbability points to the FeedbackProbability in statistics package.
// It's initialized in init() in feedback.go to solve import cycle.
var FeedbackProbability *atomic2.Float64

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
	// ValidatePasswordNumberCount is the name of 'validate_password_number_count' system variable.
	ValidatePasswordNumberCount = "validate_password_number_count"
	// ValidatePasswordLength is the name of 'validate_password_length' system variable.
	ValidatePasswordLength = "validate_password_length"
	// Version is the name of 'version' system variable.
	Version = "version"
	// VersionComment is the name of 'version_comment' system variable.
	VersionComment = "version_comment"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
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
	// ValidatePasswordCheckUserName is the name for 'validate_password_check_user_name' system variable.
	ValidatePasswordCheckUserName = "validate_password_check_user_name"
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
)
