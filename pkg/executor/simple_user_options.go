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

package executor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (info *resourceOptionsInfo) loadResourceOptions(userResource []*ast.ResourceOption) error {
	for _, option := range userResource {
		switch option.Type {
		case ast.MaxQueriesPerHour:
			info.maxQueriesPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxUpdatesPerHour:
			info.maxUpdatesPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxConnectionsPerHour:
			info.maxConnectionsPerHour = min(option.Count, math.MaxInt16)
		case ast.MaxUserConnections:
			info.maxUserConnections = min(option.Count, math.MaxInt16)
		}
	}
	return nil
}

func whetherSavePasswordHistory(plOptions *passwordOrLockOptionsInfo) bool {
	var passwdSaveNum, passwdSaveTime int64
	// If the user specifies a default, read the global variable.
	if plOptions.passwordHistoryChange && plOptions.passwordHistory != notSpecified {
		passwdSaveNum = plOptions.passwordHistory
	} else {
		passwdSaveNum = vardef.PasswordHistory.Load()
	}
	if plOptions.passwordReuseIntervalChange && plOptions.passwordReuseInterval != notSpecified {
		passwdSaveTime = plOptions.passwordReuseInterval
	} else {
		passwdSaveTime = vardef.PasswordReuseInterval.Load()
	}
	return passwdSaveTime > 0 || passwdSaveNum > 0
}

type alterUserPasswordLocking struct {
	failedLoginAttempts            int64
	passwordLockTime               int64
	failedLoginAttemptsNotFound    bool
	passwordLockTimeChangeNotFound bool
	// containsNoOthers indicates whether User_attributes only contains one "Password_locking" element.
	containsNoOthers bool
}

func (info *passwordOrLockOptionsInfo) loadOptions(plOption []*ast.PasswordOrLockOption) error {
	if length := len(plOption); length > 0 {
		// If "PASSWORD EXPIRE ..." appears many times,
		// only the last declaration takes effect.
	Loop:
		for i := length - 1; i >= 0; i-- {
			switch plOption[i].Type {
			case ast.PasswordExpire:
				info.passwordExpired = "Y"
				break Loop
			case ast.PasswordExpireDefault:
				info.passwordLifetime = nil
				break Loop
			case ast.PasswordExpireNever:
				info.passwordLifetime = 0
				break Loop
			case ast.PasswordExpireInterval:
				if plOption[i].Count == 0 || plOption[i].Count > math.MaxUint16 {
					return types.ErrWrongValue2.GenWithStackByArgs("DAY", fmt.Sprintf("%v", plOption[i].Count))
				}
				info.passwordLifetime = plOption[i].Count
				break Loop
			}
		}
	}
	// only the last declaration takes effect.
	for _, option := range plOption {
		switch option.Type {
		case ast.Lock:
			info.lockAccount = "Y"
		case ast.Unlock:
			info.lockAccount = "N"
		case ast.FailedLoginAttempts:
			info.failedLoginAttempts = min(option.Count, math.MaxInt16)
			info.failedLoginAttemptsChange = true
		case ast.PasswordLockTime:
			info.passwordLockTime = min(option.Count, math.MaxInt16)
			info.passwordLockTimeChange = true
		case ast.PasswordLockTimeUnbounded:
			info.passwordLockTime = -1
			info.passwordLockTimeChange = true
		case ast.PasswordHistory:
			info.passwordHistory = min(option.Count, math.MaxUint16)
			info.passwordHistoryChange = true
		case ast.PasswordHistoryDefault:
			info.passwordHistory = notSpecified
			info.passwordHistoryChange = true
		case ast.PasswordReuseInterval:
			info.passwordReuseInterval = min(option.Count, math.MaxUint16)
			info.passwordReuseIntervalChange = true
		case ast.PasswordReuseDefault:
			info.passwordReuseInterval = notSpecified
			info.passwordReuseIntervalChange = true
		}
	}
	return nil
}

func createUserFailedLoginJSON(info *passwordOrLockOptionsInfo) string {
	// Record only when either failedLoginAttempts and passwordLockTime is not 0
	if (info.failedLoginAttemptsChange && info.failedLoginAttempts != 0) || (info.passwordLockTimeChange && info.passwordLockTime != 0) {
		return fmt.Sprintf("\"Password_locking\": {\"failed_login_attempts\": %d,\"password_lock_time_days\": %d}",
			info.failedLoginAttempts, info.passwordLockTime)
	}
	return ""
}

func alterUserFailedLoginJSON(info *alterUserPasswordLocking, lockAccount string) string {
	// alterUserPasswordLocking is the user's actual configuration.
	var passwordLockingArray []string
	if info.failedLoginAttempts != 0 || info.passwordLockTime != 0 {
		if lockAccount == "N" {
			passwordLockingArray = append(passwordLockingArray,
				fmt.Sprintf("\"auto_account_locked\": \"%s\"", lockAccount),
				fmt.Sprintf("\"auto_locked_last_changed\": \"%s\"", time.Now().Format(time.UnixDate)),
				fmt.Sprintf("\"failed_login_count\": %d", 0))
		}
		passwordLockingArray = append(passwordLockingArray,
			fmt.Sprintf("\"failed_login_attempts\": %d", info.failedLoginAttempts),
			fmt.Sprintf("\"password_lock_time_days\": %d", info.passwordLockTime))
	}
	if len(passwordLockingArray) > 0 {
		return fmt.Sprintf("\"Password_locking\": {%s}", strings.Join(passwordLockingArray, ","))
	}
	return ""
}

func readPasswordLockingInfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, pLO *passwordOrLockOptionsInfo) (aUPL *alterUserPasswordLocking, err error) {
	alterUserInfo := &alterUserPasswordLocking{
		failedLoginAttempts:            0,
		passwordLockTime:               0,
		failedLoginAttemptsNotFound:    false,
		passwordLockTimeChangeNotFound: false,
		containsNoOthers:               false,
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, `SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.failed_login_attempts')),
        JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.Password_locking.password_lock_time_days')),
	    JSON_LENGTH(JSON_REMOVE(user_attributes, '$.Password_locking')) FROM %n.%n WHERE User=%? AND Host=%?;`,
		mysql.SystemDB, mysql.UserTable, name, strings.ToLower(host))
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}

	// Configuration priority is User Changes > User History
	if pLO.failedLoginAttemptsChange {
		alterUserInfo.failedLoginAttempts = pLO.failedLoginAttempts
	} else if !rows[0].IsNull(0) {
		str := rows[0].GetString(0)
		alterUserInfo.failedLoginAttempts, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.failedLoginAttempts = max(alterUserInfo.failedLoginAttempts, 0)
		alterUserInfo.failedLoginAttempts = min(alterUserInfo.failedLoginAttempts, math.MaxInt16)
	} else {
		alterUserInfo.failedLoginAttemptsNotFound = true
	}

	if pLO.passwordLockTimeChange {
		alterUserInfo.passwordLockTime = pLO.passwordLockTime
	} else if !rows[0].IsNull(1) {
		str := rows[0].GetString(1)
		alterUserInfo.passwordLockTime, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		alterUserInfo.passwordLockTime = max(alterUserInfo.passwordLockTime, -1)
		alterUserInfo.passwordLockTime = min(alterUserInfo.passwordLockTime, math.MaxInt16)
	} else {
		alterUserInfo.passwordLockTimeChangeNotFound = true
	}

	alterUserInfo.containsNoOthers = rows[0].IsNull(2) || rows[0].GetInt64(2) == 0
	return alterUserInfo, nil
}

// deletePasswordLockingAttribute deletes "$.Password_locking" in "User_attributes" when failedLoginAttempts and passwordLockTime both 0.
func deletePasswordLockingAttribute(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, host string, alterUser *alterUserPasswordLocking) error {
	// No password_locking information.
	if alterUser.failedLoginAttemptsNotFound && alterUser.passwordLockTimeChangeNotFound {
		return nil
	}
	// Password_locking information is still in used.
	if alterUser.failedLoginAttempts != 0 || alterUser.passwordLockTime != 0 {
		return nil
	}
	sql := new(strings.Builder)
	if alterUser.containsNoOthers {
		// If we use JSON_REMOVE(user_attributes, '$.Password_locking') directly here, the result is not compatible with MySQL.
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=NULL`, mysql.SystemDB, mysql.UserTable)
	} else {
		sqlescape.MustFormatSQL(sql, `UPDATE %n.%n SET user_attributes=JSON_REMOVE(user_attributes, '$.Password_locking') `, mysql.SystemDB, mysql.UserTable)
	}
	sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", host, name)
	_, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	return err
}
