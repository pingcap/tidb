// Copyright 2023 PingCAP, Inc.
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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func addHistoricalData(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo) error {
	if passwordReuse.passwordHistory <= 0 && passwordReuse.passwordReuseInterval <= 0 {
		return nil
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `INSERT INTO %n.%n (Host, User, Password) VALUES (%?, %?, %?) `, mysql.SystemDB, mysql.PasswordHistoryTable, strings.ToLower(userDetail.host), userDetail.user, userDetail.pwd)
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// checkPasswordsMatch used to compare whether the password encrypted with mysql.AuthCachingSha2Password or mysql.AuthTiDBSM3Password is repeated.
func checkPasswordsMatch(rows []chunk.Row, oldPwd, authPlugin string) (bool, error) {
	for _, row := range rows {
		if !row.IsNull(0) {
			pwd := row.GetString(0)
			authok, err := auth.CheckHashingPassword([]byte(pwd), oldPwd, authPlugin)
			if err != nil {
				logutil.BgLogger().Error("Failed to check caching_sha2_password", zap.Error(err))
				return false, err
			}
			if authok {
				return false, nil
			}
		}
	}
	return true, nil
}

func getUserPasswordNum(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo) (deleteNum int64, err error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return 0, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		err := fmt.Errorf("`%s`@`%s` is not unique, please confirm the mysql.password_history table structure", userDetail.user, strings.ToLower(userDetail.host))
		return 0, err
	}

	return rows[0].GetInt64(0), nil
}

func fullRecordCheck(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, authPlugin string) (canUse bool, err error) {
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User= %? AND Host= %? AND Password = %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) == 0 {
			return true, nil
		}
		return false, nil
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT Password FROM %n.%n WHERE User= %? AND Host= %? ;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, vardef.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
}

func checkPasswordHistoryRule(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo, authPlugin string) (canUse bool, err error) {
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		// Exceeded the maximum number of saved items, only check the ones within the limit.
		checkRows := `SELECT count(*) FROM (SELECT Password FROM %n.%n WHERE User=%? AND Host=%? ORDER BY Password_timestamp DESC LIMIT `
		checkRows = checkRows + strconv.FormatInt(passwordReuse.passwordHistory, 10)
		checkRows = checkRows + ` ) as t where t.Password = %? `
		sqlescape.MustFormatSQL(sql, checkRows, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) != 0 {
			return false, nil
		}
		return true, nil
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		checkRows := `SELECT Password FROM %n.%n WHERE User=%? AND Host=%? ORDER BY Password_timestamp DESC LIMIT `
		checkRows = checkRows + strconv.FormatInt(passwordReuse.passwordHistory, 10)
		sqlescape.MustFormatSQL(sql, checkRows, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host))
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, vardef.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
}

func checkPasswordTimeRule(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo,
	sctx sessionctx.Context, authPlugin string) (canUse bool, err error) {
	beforeDate := getValidTime(sctx, passwordReuse)
	switch authPlugin {
	case mysql.AuthNativePassword, "":
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT count(*) FROM %n.%n WHERE User=%? AND Host=%? AND Password = %? AND Password_timestamp >= %?;`,
			mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), userDetail.pwd, beforeDate)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
		if err != nil {
			return false, err
		}
		if rows[0].GetInt64(0) == 0 {
			return true, nil
		}
	case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, `SELECT Password FROM %n.%n WHERE User=%? AND Host=%? AND Password_timestamp >= %?;`, mysql.SystemDB, mysql.PasswordHistoryTable, userDetail.user, strings.ToLower(userDetail.host), beforeDate)
		recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
		if err != nil {
			return false, err
		}
		rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, vardef.DefMaxChunkSize)
		if err != nil {
			return false, err
		}
		return checkPasswordsMatch(rows, userDetail.authString, authPlugin)
	default:
		return false, exeerrors.ErrPluginIsNotLoaded.GenWithStackByArgs(authPlugin)
	}
	return false, nil
}

func passwordVerification(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, passwordReuse *passwordReuseInfo, sctx sessionctx.Context, authPlugin string) (bool, int64, error) {
	passwordNum, err := getUserPasswordNum(ctx, sqlExecutor, userDetail)
	if err != nil {
		return false, 0, err
	}

	// the maximum number of records that can be deleted.
	canDeleteNum := max(passwordNum-passwordReuse.passwordHistory+1, 0)

	if passwordReuse.passwordHistory <= 0 && passwordReuse.passwordReuseInterval <= 0 {
		return true, canDeleteNum, nil
	}

	// The maximum number of saves has not been exceeded.
	// There are too many retention days, and it is impossible to time out in one's lifetime.
	if (passwordNum <= passwordReuse.passwordHistory) || (passwordReuse.passwordReuseInterval > math.MaxInt32) {
		passChecking, err := fullRecordCheck(ctx, sqlExecutor, userDetail, authPlugin)
		return passChecking, canDeleteNum, err
	}

	if passwordReuse.passwordHistory > 0 {
		passChecking, err := checkPasswordHistoryRule(ctx, sqlExecutor, userDetail, passwordReuse, authPlugin)
		if err != nil || !passChecking {
			return false, 0, err
		}
	}
	if passwordReuse.passwordReuseInterval > 0 {
		passChecking, err := checkPasswordTimeRule(ctx, sqlExecutor, userDetail, passwordReuse, sctx, authPlugin)
		if err != nil || !passChecking {
			return false, 0, err
		}
	}
	return true, canDeleteNum, nil
}

func checkPasswordReusePolicy(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, userDetail *userInfo, sctx sessionctx.Context, authPlugin string, authPlugins map[string]*extension.AuthPlugin) error {
	if strings.EqualFold(authPlugin, mysql.AuthTiDBAuthToken) || strings.EqualFold(authPlugin, mysql.AuthLDAPSASL) || strings.EqualFold(authPlugin, mysql.AuthLDAPSimple) {
		// AuthTiDBAuthToken is the token login method on the cloud,
		// and the Password Reuse Policy does not take effect.
		return nil
	}
	// Skip password reuse checks for extension auth plugins
	if _, ok := authPlugins[authPlugin]; ok {
		return nil
	}
	// read password reuse info from mysql.user and global variables.
	passwdReuseInfo, err := getUserPasswordLimit(ctx, sqlExecutor, userDetail.user, userDetail.host, userDetail.pLI)
	if err != nil {
		return err
	}
	// check whether password can be used.
	res, maxDelNum, err := passwordVerification(ctx, sqlExecutor, userDetail, passwdReuseInfo, sctx, authPlugin)
	if err != nil {
		return err
	}
	if !res {
		return exeerrors.ErrExistsInHistoryPassword.GenWithStackByArgs(userDetail.user, userDetail.host)
	}
	err = deleteHistoricalData(ctx, sqlExecutor, userDetail, maxDelNum, passwdReuseInfo, sctx)
	if err != nil {
		return err
	}
	// insert password history.
	err = addHistoricalData(ctx, sqlExecutor, userDetail, passwdReuseInfo)
	if err != nil {
		return err
	}
	return nil
}
