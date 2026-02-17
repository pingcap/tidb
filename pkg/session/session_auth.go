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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

func (s *session) AuthPluginForUser(ctx context.Context, user *auth.UserIdentity) (string, error) {
	pm := privilege.GetPrivilegeManager(s)
	authplugin, err := pm.GetAuthPluginForConnection(ctx, user.Username, user.Hostname)
	if err != nil {
		return "", err
	}
	return authplugin, nil
}

// Auth validates a user using an authentication string and salt.
// If the password fails, it will keep trying other users until exhausted.
// This means it can not be refactored to use MatchIdentity yet.
func (s *session) Auth(user *auth.UserIdentity, authentication, salt []byte, authConn conn.AuthConn) error {
	hasPassword := "YES"
	if len(authentication) == 0 {
		hasPassword = "NO"
	}
	pm := privilege.GetPrivilegeManager(s)
	authUser, err := s.MatchIdentity(context.Background(), user.Username, user.Hostname)
	if err != nil {
		return privileges.ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
	}
	// Check whether continuous login failure is enabled to lock the account.
	// If enabled, determine whether to unlock the account and notify TiDB to update the cache.
	enableAutoLock := pm.IsAccountAutoLockEnabled(authUser.Username, authUser.Hostname)
	if enableAutoLock {
		err = failedLoginTrackingBegin(s)
		if err != nil {
			return err
		}
		lockStatusChanged, err := verifyAccountAutoLock(s, authUser.Username, authUser.Hostname)
		if err != nil {
			rollbackErr := failedLoginTrackingRollback(s)
			if rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		err = failedLoginTrackingCommit(s)
		if err != nil {
			rollbackErr := failedLoginTrackingRollback(s)
			if rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		if lockStatusChanged {
			// Notification auto unlock.
			err = domain.GetDomain(s).NotifyUpdatePrivilege([]string{authUser.Username})
			if err != nil {
				return err
			}
		}
	}

	info, err := pm.ConnectionVerification(user, authUser.Username, authUser.Hostname, authentication, salt, s.sessionVars, authConn)
	if err != nil {
		if info.FailedDueToWrongPassword {
			// when user enables the account locking function for consecutive login failures,
			// the system updates the login failure count and determines whether to lock the account when authentication fails.
			if enableAutoLock {
				err := failedLoginTrackingBegin(s)
				if err != nil {
					return err
				}
				lockStatusChanged, passwordLocking, trackingErr := authFailedTracking(s, authUser.Username, authUser.Hostname)
				if trackingErr != nil {
					if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
						return rollBackErr
					}
					return trackingErr
				}
				if err := failedLoginTrackingCommit(s); err != nil {
					if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
						return rollBackErr
					}
					return err
				}
				if lockStatusChanged {
					// Notification auto lock.
					err := autolockAction(s, passwordLocking, authUser.Username, authUser.Hostname)
					if err != nil {
						return err
					}
				}
			}
		}
		return err
	}

	if vardef.EnableResourceControl.Load() && info.ResourceGroupName != "" {
		s.sessionVars.SetResourceGroupName(info.ResourceGroupName)
	}

	if info.InSandBoxMode {
		// Enter sandbox mode, only execute statement for resetting password.
		s.EnableSandBoxMode()
	}
	if enableAutoLock {
		err := failedLoginTrackingBegin(s)
		if err != nil {
			return err
		}
		// The password is correct. If the account is not locked, the number of login failure statistics will be cleared.
		err = authSuccessClearCount(s, authUser.Username, authUser.Hostname)
		if err != nil {
			if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
				return rollBackErr
			}
			return err
		}
		err = failedLoginTrackingCommit(s)
		if err != nil {
			if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
				return rollBackErr
			}
			return err
		}
	}
	pm.AuthSuccess(authUser.Username, authUser.Hostname)
	user.AuthUsername = authUser.Username
	user.AuthHostname = authUser.Hostname
	s.sessionVars.User = user
	s.sessionVars.ActiveRoles = pm.GetDefaultRoles(context.Background(), user.AuthUsername, user.AuthHostname)
	return nil
}

func authSuccessClearCount(s *session, user string, host string) error {
	// Obtain accurate lock status and failure count information.
	passwordLocking, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return err
	}
	// If the account is locked, it may be caused by the untimely update of the cache,
	// directly report the account lock.
	if passwordLocking.AutoAccountLocked {
		if passwordLocking.PasswordLockTimeDays == -1 {
			return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host,
				"unlimited", "unlimited")
		}

		lds := strconv.FormatInt(passwordLocking.PasswordLockTimeDays, 10)
		return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host, lds, lds)
	}
	if passwordLocking.FailedLoginCount != 0 {
		// If the number of account login failures is not zero, it will be updated to 0.
		passwordLockingJSON := privileges.BuildSuccessPasswordLockingJSON(passwordLocking.FailedLoginAttempts,
			passwordLocking.PasswordLockTimeDays)
		if passwordLockingJSON != "" {
			if err := s.passwordLocking(user, host, passwordLockingJSON); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyAccountAutoLock(s *session, user, host string) (bool, error) {
	pm := privilege.GetPrivilegeManager(s)
	// Use the cache to determine whether to unlock the account.
	// If the account needs to be unlocked, read the database information to determine whether
	// the account needs to be unlocked. Otherwise, an error message is displayed.
	lockStatusInMemory, err := pm.VerifyAccountAutoLockInMemory(user, host)
	if err != nil {
		return false, err
	}
	// If the lock status in the cache is Unlock, the automatic unlock is skipped.
	// If memory synchronization is slow and there is a lock in the database, it will be processed upon successful login.
	if !lockStatusInMemory {
		return false, nil
	}
	lockStatusChanged := false
	var plJSON string
	// After checking the cache, obtain the latest data from the database and determine
	// whether to automatically unlock the database to prevent repeated unlock errors.
	pl, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return false, err
	}
	if pl.AutoAccountLocked {
		// If it is locked, need to check whether it can be automatically unlocked.
		lockTimeDay := pl.PasswordLockTimeDays
		if lockTimeDay == -1 {
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, "unlimited", "unlimited")
		}
		lastChanged := pl.AutoLockedLastChanged
		d := time.Now().Unix() - lastChanged
		if d <= lockTimeDay*24*60*60 {
			lds := strconv.FormatInt(lockTimeDay, 10)
			rds := strconv.FormatInt(int64(math.Ceil(float64(lockTimeDay)-float64(d)/(24*60*60))), 10)
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, lds, rds)
		}
		// Generate unlock json string.
		plJSON = privileges.BuildPasswordLockingJSON(pl.FailedLoginAttempts,
			pl.PasswordLockTimeDays, "N", 0, time.Now().Format(time.UnixDate))
	}
	if plJSON != "" {
		lockStatusChanged = true
		if err = s.passwordLocking(user, host, plJSON); err != nil {
			return false, err
		}
	}
	return lockStatusChanged, nil
}

func authFailedTracking(s *session, user string, host string) (bool, *privileges.PasswordLocking, error) {
	// Obtain the number of consecutive password login failures.
	passwordLocking, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return false, nil, err
	}
	// Consecutive wrong password login failure times +1,
	// If the lock condition is satisfied, the lock status is updated and the update cache is notified.
	lockStatusChanged, err := userAutoAccountLocked(s, user, host, passwordLocking)
	if err != nil {
		return false, nil, err
	}
	return lockStatusChanged, passwordLocking, nil
}

func autolockAction(s *session, passwordLocking *privileges.PasswordLocking, user, host string) error {
	// Don't want to update the cache frequently, and only trigger the update cache when the lock status is updated.
	err := domain.GetDomain(s).NotifyUpdatePrivilege([]string{user})
	if err != nil {
		return err
	}
	// The number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS.
	// An error message is displayed indicating permission denial and account lock.
	if passwordLocking.PasswordLockTimeDays == -1 {
		return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host,
			"unlimited", "unlimited")
	}
	lds := strconv.FormatInt(passwordLocking.PasswordLockTimeDays, 10)
	return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host, lds, lds)
}

func (s *session) passwordLocking(user string, host string, newAttributesStr string) error {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
	sqlescape.MustFormatSQL(sql, "user_attributes=json_merge_patch(coalesce(user_attributes, '{}'), %?)", newAttributesStr)
	sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", host, user)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, sql.String())
	return err
}

func failedLoginTrackingBegin(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "BEGIN PESSIMISTIC")
	return err
}

func failedLoginTrackingCommit(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
	if err != nil {
		_, rollBackErr := s.ExecuteInternal(ctx, "ROLLBACK")
		if rollBackErr != nil {
			return rollBackErr
		}
	}
	return err
}

func failedLoginTrackingRollback(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "ROLLBACK")
	return err
}

// getFailedLoginUserAttributes queries the exact number of consecutive password login failures (concurrency is not allowed).
func getFailedLoginUserAttributes(s *session, user string, host string) (*privileges.PasswordLocking, error) {
	passwordLocking := &privileges.PasswordLocking{}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	rs, err := s.ExecuteInternal(ctx, `SELECT user_attributes from mysql.user WHERE USER = %? AND HOST = %? for update`, user, host)
	if err != nil {
		return passwordLocking, err
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	req := rs.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	if err != nil {
		return passwordLocking, err
	}
	if req.NumRows() == 0 {
		return passwordLocking, fmt.Errorf("user_attributes by `%s`@`%s` not found", user, host)
	}
	row := iter.Begin()
	if !row.IsNull(0) {
		passwordLockingJSON := row.GetJSON(0)
		return passwordLocking, passwordLocking.ParseJSON(passwordLockingJSON)
	}
	return passwordLocking, fmt.Errorf("user_attributes by `%s`@`%s` not found", user, host)
}

func userAutoAccountLocked(s *session, user string, host string, pl *privileges.PasswordLocking) (bool, error) {
	// Indicates whether the user needs to update the lock status change.
	lockStatusChanged := false
	// The number of consecutive login failures is stored in the database.
	// If the current login fails, one is added to the number of consecutive login failures
	// stored in the database to determine whether the user needs to be locked and the number of update failures.
	failedLoginCount := pl.FailedLoginCount + 1
	// If the cache is not updated, but it is already locked, it will report that the account is locked.
	if pl.AutoAccountLocked {
		if pl.PasswordLockTimeDays == -1 {
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host,
				"unlimited", "unlimited")
		}
		lds := strconv.FormatInt(pl.PasswordLockTimeDays, 10)
		return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, lds, lds)
	}

	autoAccountLocked := "N"
	autoLockedLastChanged := ""
	if pl.FailedLoginAttempts == 0 || pl.PasswordLockTimeDays == 0 {
		return false, nil
	}

	if failedLoginCount >= pl.FailedLoginAttempts {
		autoLockedLastChanged = time.Now().Format(time.UnixDate)
		autoAccountLocked = "Y"
		lockStatusChanged = true
	}

	newAttributesStr := privileges.BuildPasswordLockingJSON(pl.FailedLoginAttempts,
		pl.PasswordLockTimeDays, autoAccountLocked, failedLoginCount, autoLockedLastChanged)
	if newAttributesStr != "" {
		return lockStatusChanged, s.passwordLocking(user, host, newAttributesStr)
	}
	return lockStatusChanged, nil
}

// MatchIdentity finds the matching username + password in the MySQL privilege tables
// for a username + hostname, since MySQL can have wildcards.
func (s *session) MatchIdentity(ctx context.Context, username, remoteHost string) (*auth.UserIdentity, error) {
	pm := privilege.GetPrivilegeManager(s)
	var success bool
	var skipNameResolve bool
	var user = &auth.UserIdentity{}
	varVal, err := s.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.SkipNameResolve)
	if err == nil && variable.TiDBOptOn(varVal) {
		skipNameResolve = true
	}
	user.Username, user.Hostname, success = pm.MatchIdentity(ctx, username, remoteHost, skipNameResolve)
	if success {
		return user, nil
	}
	// This error will not be returned to the user, access denied will be instead
	return nil, fmt.Errorf("could not find matching user in MatchIdentity: %s, %s", username, remoteHost)
}

// AuthWithoutVerification is required by the ResetConnection RPC
func (s *session) AuthWithoutVerification(ctx context.Context, user *auth.UserIdentity) bool {
	pm := privilege.GetPrivilegeManager(s)
	authUser, err := s.MatchIdentity(ctx, user.Username, user.Hostname)
	if err != nil {
		return false
	}
	if pm.GetAuthWithoutVerification(authUser.Username, authUser.Hostname) {
		user.AuthUsername = authUser.Username
		user.AuthHostname = authUser.Hostname
		s.sessionVars.User = user
		s.sessionVars.ActiveRoles = pm.GetDefaultRoles(ctx, user.AuthUsername, user.AuthHostname)
		return true
	}
	return false
}
