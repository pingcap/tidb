// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package varsutil

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
)

// GetSessionSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionSystemVar(s *variable.SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetSessionOnlySysVars(s, key)
	if err != nil || ok {
		return gVal, errors.Trace(err)
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	s.Systems[key] = gVal
	return gVal, nil
}

// GetSessionOnlySysVars get the default value defined in code for session only variable.
// The return bool value indicates whether it's a session only variable.
func GetSessionOnlySysVars(s *variable.SessionVars, key string) (string, bool, error) {
	sysVar := variable.SysVars[key]
	if sysVar == nil {
		return "", false, variable.UnknownSystemVar.GenByArgs(key)
	}
	// For virtual system variables:
	switch sysVar.Name {
	case variable.TiDBCurrentTS:
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	}
	sVal, ok := s.Systems[key]
	if ok {
		return sVal, true, nil
	}
	if sysVar.Scope&variable.ScopeGlobal == 0 {
		// None-Global variable can use pre-defined default value.
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *variable.SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetScopeNoneSystemVar(key)
	if err != nil || ok {
		return gVal, errors.Trace(err)
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	return gVal, nil
}

// GetScopeNoneSystemVar checks the validation of `key`,
// and return the default value if its scope is `ScopeNone`.
func GetScopeNoneSystemVar(key string) (string, bool, error) {
	sysVar := variable.SysVars[key]
	if sysVar == nil {
		return "", false, variable.UnknownSystemVar.GenByArgs(key)
	}
	if sysVar.Scope == variable.ScopeNone {
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// SetSessionSystemVar sets system variable and updates SessionVars states.
func SetSessionSystemVar(vars *variable.SessionVars, name string, value types.Datum) error {
	name = strings.ToLower(name)
	sysVar := variable.SysVars[name]
	if sysVar == nil {
		return variable.UnknownSystemVar
	}
	if value.IsNull() {
		if name != variable.CharacterSetResults {
			return variable.ErrCantSetToNull
		}
		delete(vars.Systems, name)
		return nil
	}
	sVal, err := value.ToString()
	if err != nil {
		return errors.Trace(err)
	}
	switch name {
	case variable.TimeZone:
		vars.TimeZone, err = parseTimeZone(sVal)
		if err != nil {
			return errors.Trace(err)
		}
	case variable.SQLModeVar:
		sVal = mysql.FormatSQLModeStr(sVal)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := mysql.GetSQLMode(sVal)
		if err2 != nil {
			return errors.Trace(err2)
		}
		vars.StrictSQLMode = sqlMode.HasStrictMode()
		vars.SQLMode = sqlMode
		vars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case variable.TiDBSnapshot:
		err = setSnapshotTS(vars, sVal)
		if err != nil {
			return errors.Trace(err)
		}
	case variable.AutocommitVar:
		isAutocommit := tidbOptOn(sVal)
		vars.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			vars.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case variable.TiDBSkipConstraintCheck:
		vars.SkipConstraintCheck = tidbOptOn(sVal)
	case variable.TiDBSkipUTF8Check:
		vars.SkipUTF8Check = tidbOptOn(sVal)
	case variable.TiDBOptAggPushDown:
		vars.AllowAggPushDown = tidbOptOn(sVal)
	case variable.TiDBOptInSubqUnFolding:
		vars.AllowInSubqueryUnFolding = tidbOptOn(sVal)
	case variable.TiDBIndexLookupConcurrency:
		vars.IndexLookupConcurrency = tidbOptPositiveInt(sVal, variable.DefIndexLookupConcurrency)
	case variable.TiDBIndexJoinBatchSize:
		vars.IndexJoinBatchSize = tidbOptPositiveInt(sVal, variable.DefIndexJoinBatchSize)
	case variable.TiDBIndexLookupSize:
		vars.IndexLookupSize = tidbOptPositiveInt(sVal, variable.DefIndexLookupSize)
	case variable.TiDBDistSQLScanConcurrency:
		vars.DistSQLScanConcurrency = tidbOptPositiveInt(sVal, variable.DefDistSQLScanConcurrency)
	case variable.TiDBIndexSerialScanConcurrency:
		vars.IndexSerialScanConcurrency = tidbOptPositiveInt(sVal, variable.DefIndexSerialScanConcurrency)
	case variable.TiDBBatchInsert:
		vars.BatchInsert = tidbOptOn(sVal)
	case variable.TiDBBatchDelete:
		vars.BatchDelete = tidbOptOn(sVal)
	case variable.TiDBDMLBatchSize:
		vars.DMLBatchSize = tidbOptPositiveInt(sVal, variable.DefDMLBatchSize)
	case variable.TiDBCurrentTS:
		return variable.ErrReadOnly
	case variable.TiDBMaxChunkSize:
		vars.MaxChunkSize = tidbOptPositiveInt(sVal, variable.DefMaxChunkSize)
	}
	vars.Systems[name] = sVal
	return nil
}

// tidbOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func tidbOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

func tidbOptPositiveInt(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

func parseTimeZone(s string) (*time.Location, error) {
	if s == "SYSTEM" {
		// TODO: Support global time_zone variable, it should be set to global time_zone value.
		return time.Local, nil
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := types.ParseDuration(s[1:], 0)
		if err == nil {
			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			return time.FixedZone("UTC", ofst), nil
		}
	}

	return nil, variable.ErrUnknownTimeZone.GenByArgs(s)
}

func setSnapshotTS(s *variable.SessionVars, sVal string) error {
	if sVal == "" {
		s.SnapshotTS = 0
		return nil
	}
	t, err := types.ParseTime(s.StmtCtx, sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: Consider time_zone variable.
	t1, err := t.Time.GoTime(time.Local)
	s.SnapshotTS = GoTimeToTS(t1)
	return errors.Trace(err)
}

// GoTimeToTS converts a Go time to uint64 timestamp.
func GoTimeToTS(t time.Time) uint64 {
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	return uint64(ts)
}
