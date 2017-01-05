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
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// GetSessionSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionSystemVar(s *variable.SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	sysVar := variable.SysVars[key]
	if sysVar == nil {
		return "", variable.UnknownSystemVar
	}
	sVal, ok := s.Systems[key]
	if ok {
		return sVal, nil
	}
	if sysVar.Scope&variable.ScopeGlobal == 0 {
		// None-Global variable can use pre-defined default value.
		return sysVar.Value, nil
	}
	gVal, err := s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	s.Systems[key] = gVal
	return gVal, nil
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *variable.SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	sysVar := variable.SysVars[key]
	if sysVar == nil {
		return "", variable.UnknownSystemVar
	}
	if sysVar.Scope == variable.ScopeSession {
		return "", variable.ErrIncorrectScope
	} else if sysVar.Scope == variable.ScopeNone {
		return sysVar.Value, nil
	}
	return s.GlobalVarsAccessor.GetGlobalSysVar(key)
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
		vars.TimeZone = parseTimeZone(sVal)
	case variable.SQLModeVar:
		sVal = strings.ToUpper(sVal)
		if strings.Contains(sVal, "STRICT_TRANS_TABLES") || strings.Contains(sVal, "STRICT_ALL_TABLES") {
			vars.StrictSQLMode = true
		} else {
			vars.StrictSQLMode = false
		}
	case variable.TiDBSnapshot:
		err = setSnapshotTS(vars, sVal)
		if err != nil {
			return errors.Trace(err)
		}
	case variable.AutocommitVar:
		isAutocommit := strings.EqualFold(sVal, "ON") || sVal == "1"
		vars.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			vars.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case variable.TiDBSkipConstraintCheck:
		vars.SkipConstraintCheck = (sVal == "1")
	case variable.TiDBSkipDDLWait:
		vars.SkipDDLWait = (sVal == "1")
	}
	vars.Systems[name] = sVal
	return nil
}

func parseTimeZone(s string) *time.Location {
	if s == "SYSTEM" {
		// TODO: Support global time_zone variable, it should be set to global time_zone value.
		return time.Local
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := types.ParseDuration(s[1:], 0)
		if err == nil {
			return time.FixedZone("UTC", int(d.Duration/time.Second))
		}
	}

	return nil
}

func setSnapshotTS(s *variable.SessionVars, sVal string) error {
	if sVal == "" {
		s.SnapshotTS = 0
		return nil
	}
	t, err := types.ParseTime(sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: Consider time_zone variable.
	t1, err := t.Time.GoTime(time.Local)
	ts := (t1.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	s.SnapshotTS = uint64(ts)
	return errors.Trace(err)
}
