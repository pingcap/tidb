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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/meta"
)

func onLockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	arg := &lockTablesArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Unlock table first.
	if arg.IndexOfUnlock < len(arg.UnlockTables) {
		return unlockTables(t, job, arg)
	}

	// Check table locked by other, this can be only checked at the first time.
	if arg.IndexOfLock == 0 {
		for i, tl := range arg.LockTables {
			job.SchemaID = tl.SchemaID
			job.TableID = tl.TableID
			tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
			if err != nil {
				return ver, err
			}
			err = checkTableLocked(tbInfo, arg.LockTables[i].Tp, arg.SessionInfo)
			if err != nil {
				// If any request table was locked by other session, just cancel this job.
				// No need to rolling back the unlocked tables, MySQL will release the lock first
				// and block if the request table was locked by other.
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
	}

	// Lock tables.
	if arg.IndexOfLock < len(arg.LockTables) {
		job.SchemaID = arg.LockTables[arg.IndexOfLock].SchemaID
		job.TableID = arg.LockTables[arg.IndexOfLock].TableID
		var tbInfo *model.TableInfo
		tbInfo, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		if err != nil {
			return ver, err
		}
		err = lockTable(tbInfo, arg.IndexOfLock, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		switch tbInfo.Lock.State {
		case model.TableLockStateNone:
			// none -> pre_lock
			tbInfo.Lock.State = model.TableLockStatePreLock
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
		// If the state of the lock is public, it means the lock is a read lock and already locked by other session,
		// so this request of lock table doesn't need pre-lock state, just update the TS and table info is ok.
		case model.TableLockStatePreLock, model.TableLockStatePublic:
			tbInfo.Lock.State = model.TableLockStatePublic
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			arg.IndexOfLock++
			job.Args = []interface{}{arg}
			if arg.IndexOfLock == len(arg.LockTables) {
				// Finish this job.
				job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
			}
		default:
			job.State = model.JobStateCancelled
			return ver, ErrInvalidDDLState.GenWithStackByArgs("table lock", tbInfo.Lock.State)
		}
	}

	return ver, err
}

// findSessionInfoIndex gets the index of sessionInfo in the sessions. return -1 if sessions doesn't contain the sessionInfo.
func findSessionInfoIndex(sessions []model.SessionInfo, sessionInfo model.SessionInfo) int {
	for i := range sessions {
		if sessions[i].ServerID == sessionInfo.ServerID && sessions[i].SessionID == sessionInfo.SessionID {
			return i
		}
	}
	return -1
}

// lockTable uses to check table locked and acquire the table lock for the request session.
func lockTable(tbInfo *model.TableInfo, idx int, arg *lockTablesArg) error {
	if !tbInfo.IsLocked() {
		tbInfo.Lock = &model.TableLockInfo{
			Tp: arg.LockTables[idx].Tp,
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, arg.SessionInfo)
		return nil
	}
	// If the state of the lock is in pre-lock, then the lock must be locked by the current request. So we can just return here.
	// Because the lock/unlock job must be serial execution in DDL owner now.
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == model.TableLockRead && arg.LockTables[idx].Tp == model.TableLockRead {
		sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, arg.SessionInfo)
		// repeat lock.
		if sessionIndex >= 0 {
			return nil
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, arg.SessionInfo)
		return nil
	}

	// Unlock tables should execute before lock tables.
	// Normally execute to here is impossible.
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

// checkTableLocked uses to check whether table was locked.
func checkTableLocked(tbInfo *model.TableInfo, lockTp model.TableLockType, sessionInfo model.SessionInfo) error {
	if !tbInfo.IsLocked() {
		return nil
	}
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == model.TableLockRead && lockTp == model.TableLockRead {
		return nil
	}
	sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, sessionInfo)
	// If the request session already locked the table before, In other words, repeat lock.
	if sessionIndex >= 0 {
		if tbInfo.Lock.Tp == lockTp {
			return nil
		}
		// If no other session locked this table.
		if len(tbInfo.Lock.Sessions) == 1 {
			return nil
		}
	}
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

// unlockTables uses unlock a batch of table lock one by one.
func unlockTables(t *meta.Meta, job *model.Job, arg *lockTablesArg) (ver int64, err error) {
	if arg.IndexOfUnlock >= len(arg.UnlockTables) {
		return ver, nil
	}
	job.SchemaID = arg.UnlockTables[arg.IndexOfUnlock].SchemaID
	job.TableID = arg.UnlockTables[arg.IndexOfUnlock].TableID
	tbInfo, err := getTableInfo(t, job.TableID, job.SchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			// The table maybe has been dropped. just ignore this err and go on.
			arg.IndexOfUnlock++
			job.Args = []interface{}{arg}
			return ver, nil
		}
		return ver, err
	}

	needUpdateTableInfo := unlockTable(tbInfo, arg)
	if needUpdateTableInfo {
		ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	arg.IndexOfUnlock++
	job.Args = []interface{}{arg}
	return ver, nil
}

// unlockTable uses to unlock table lock that hold by the session.
func unlockTable(tbInfo *model.TableInfo, arg *lockTablesArg) (needUpdateTableInfo bool) {
	if !tbInfo.IsLocked() {
		return false
	}
	if arg.IsCleanup {
		tbInfo.Lock = nil
		return true
	}

	sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, arg.SessionInfo)
	if sessionIndex < 0 {
		// When session clean table lock, session maybe send unlock table even the table lock maybe not hold by the session.
		// so just ignore and return here.
		return false
	}
	oldSessionInfo := tbInfo.Lock.Sessions
	tbInfo.Lock.Sessions = oldSessionInfo[:sessionIndex]
	tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, oldSessionInfo[sessionIndex+1:]...)
	if len(tbInfo.Lock.Sessions) == 0 {
		tbInfo.Lock = nil
	}
	return true
}

func onUnlockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	arg := &lockTablesArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = unlockTables(t, job, arg)
	if arg.IndexOfUnlock == len(arg.UnlockTables) {
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, nil)
	}
	return ver, err
}
