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
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

// CheckGCEnable is use to check whether gc is enable.
func CheckGCEnable(ctx sessionctx.Context) (enable bool, err error) {
	sql := fmt.Sprintf(`SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, "tikv_gc_enable")
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) != 1 {
		return false, errors.New("can not get 'tikv_gc_enable'")
	}
	return rows[0].GetString(0) == "true", nil
}

// DisableGC will disable gc enable variable.
func DisableGC(ctx sessionctx.Context) error {
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		"tikv_gc_enable", "false", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// EnableGC will enable gc enable variable.
func EnableGC(ctx sessionctx.Context) error {
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		"tikv_gc_enable", "true", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx sessionctx.Context, snapshotTS uint64) error {
	safePointString, err := GetGCSafePoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		return errors.Trace(err)
	}
	safePointTS := variable.GoTimeToTS(safePointTime)
	if err != nil {
		return errors.Trace(err)
	}
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(safePointString)
	}
	return nil
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshotWithGCSafePoint(snapshotTS uint64, safePointString string) error {
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		return errors.Trace(err)
	}
	gcSafePointTS := variable.GoTimeToTS(safePointTime)
	if err != nil {
		return errors.Trace(err)
	}
	if gcSafePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(safePointString)
	}
	return nil
}

func GetGCSafePoint(ctx sessionctx.Context) (string, error) {
	sql := "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_safe_point'"
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(rows) != 1 {
		return "", errors.New("can not get 'tikv_gc_safe_point'")
	}
	return rows[0].GetString(0), nil
}
