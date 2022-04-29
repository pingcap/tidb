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

package gcutil

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
)

const (
	selectVariableValueSQL = `SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?`
)

// CheckGCEnable is use to check whether GC is enable.
func CheckGCEnable(ctx sessionctx.Context) (enable bool, err error) {
	val, err := ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBGCEnable)
	if err != nil {
		return false, errors.Trace(err)
	}
	return variable.TiDBOptOn(val), nil
}

// DisableGC will disable GC enable variable.
func DisableGC(ctx sessionctx.Context) error {
	return ctx.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(variable.TiDBGCEnable, variable.Off)
}

// EnableGC will enable GC enable variable.
func EnableGC(ctx sessionctx.Context) error {
	return ctx.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(variable.TiDBGCEnable, variable.On)
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx sessionctx.Context, snapshotTS uint64) error {
	safePointTS, err := GetGCSafePoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// ValidateSnapshotWithGCSafePoint checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshotWithGCSafePoint(snapshotTS, safePointTS uint64) error {
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// GetGCSafePoint loads GC safe point time from mysql.tidb.
func GetGCSafePoint(ctx sessionctx.Context) (uint64, error) {
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.Background(), true, selectVariableValueSQL, "tikv_gc_safe_point")
	if err != nil {
		return 0, errors.Trace(err)
	}
	rows, _, err := exec.ExecRestrictedStmt(context.Background(), stmt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.New("can not get 'tikv_gc_safe_point'")
	}
	safePointString := rows[0].GetString(0)
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := oracle.GoTimeToTS(safePointTime)
	return ts, nil
}
