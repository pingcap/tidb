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

// GetSystemVar gets a system variable.
func GetSystemVar(s *variable.SessionVars, key string) types.Datum {
	var d types.Datum
	key = strings.ToLower(key)
	sVal, ok := s.Systems[key]
	if ok {
		d.SetString(sVal)
	} else {
		// TiDBSkipConstraintCheck is a session scope vars. We do not store it in the global table.
		if key == variable.TiDBSkipConstraintCheck {
			d.SetString(variable.SysVars[variable.TiDBSkipConstraintCheck].Value)
		}
	}
	return d
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// SetSystemVar sets system variable and updates SessionVars states.
func SetSystemVar(vars *variable.SessionVars, name string, value types.Datum) error {
	name = strings.ToLower(name)
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
	case variable.TiDBSkipConstraintCheck:
		vars.SkipConstraintCheck = (sVal == "1")
	}
	vars.Systems[name] = sVal
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
	ts := (t.Time.GoTime().UnixNano() / int64(time.Millisecond)) << epochShiftBits
	s.SnapshotTS = uint64(ts)
	return nil
}
