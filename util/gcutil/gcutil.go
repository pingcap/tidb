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
// See the License for the specific language governing permissions and
// limitations under the License.

package gcutil

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	insertVariableValueSQL = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES (%?, %?, %?)
                              ON DUPLICATE KEY UPDATE variable_value = %?, comment = %?`
	selectVariableValueSQL = `SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?`
)

// CheckGCEnable is use to check whether GC is enable.
func CheckGCEnable(ctx sessionctx.Context) (enable bool, err error) {
	stmt, err1 := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), selectVariableValueSQL, "tikv_gc_enable")
	if err1 != nil {
		return false, errors.Trace(err1)
	}
	rows, _, err2 := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
	if err1 != nil {
		return false, errors.Trace(err2)
	}
	if len(rows) != 1 {
		return false, errors.New("can not get 'tikv_gc_enable'")
	}
	return rows[0].GetString(0) == "true", nil
}

// DisableGC will disable GC enable variable.
func DisableGC(ctx sessionctx.Context) error {
	stmt, err := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), insertVariableValueSQL, "tikv_gc_enable", "false", "Current GC enable status", "false", "Current GC enable status")
	if err == nil {
		_, _, err = ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
	}
	return errors.Trace(err)
}

// EnableGC will enable GC enable variable.
func EnableGC(ctx sessionctx.Context) error {
	stmt, err := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), insertVariableValueSQL, "tikv_gc_enable", "true", "Current GC enable status", "true", "Current GC enable status")
	if err == nil {
		_, _, err = ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
	}
	return errors.Trace(err)
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
	stmt, err := exec.ParseWithParams(context.Background(), selectVariableValueSQL, "tikv_gc_safe_point")
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
	ts := variable.GoTimeToTS(safePointTime)
	return ts, nil
}
