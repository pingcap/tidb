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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func onLockTables(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, err error) {
	args, err := model.GetLockTablesArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Unlock table first.
	if args.IndexOfUnlock < len(args.UnlockTables) {
		return unlockTables(jobCtx, t, job, args)
	}

	// Check table locked by other, this can be only checked at the first time.
	if args.IndexOfLock == 0 {
		for i, tl := range args.LockTables {
			job.SchemaID = tl.SchemaID
			job.TableID = tl.TableID
			tbInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
			if err != nil {
				return ver, err
			}
			err = checkTableLocked(tbInfo, args.LockTables[i].Tp, args.SessionInfo)
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
	if args.IndexOfLock < len(args.LockTables) {
		job.SchemaID = args.LockTables[args.IndexOfLock].SchemaID
		job.TableID = args.LockTables[args.IndexOfLock].TableID
		var tbInfo *model.TableInfo
		tbInfo, err = GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		if err != nil {
			return ver, err
		}
		err = lockTable(tbInfo, args.IndexOfLock, args)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		switch tbInfo.Lock.State {
		case model.TableLockStateNone:
			// none -> pre_lock
			tbInfo.Lock.State = model.TableLockStatePreLock
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(jobCtx, t, job, tbInfo, true)
		// If the state of the lock is public, it means the lock is a read lock and already locked by other session,
		// so this request of lock table doesn't need pre-lock state, just update the TS and table info is ok.
		case model.TableLockStatePreLock, model.TableLockStatePublic:
			tbInfo.Lock.State = model.TableLockStatePublic
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(jobCtx, t, job, tbInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			args.IndexOfLock++
			job.FillArgs(args)
			if args.IndexOfLock == len(args.LockTables) {
				// Finish this job.
				job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
			}
		default:
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table lock", tbInfo.Lock.State)
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
func lockTable(tbInfo *model.TableInfo, idx int, args *model.LockTablesArgs) error {
	if !tbInfo.IsLocked() {
		tbInfo.Lock = &model.TableLockInfo{
			Tp: args.LockTables[idx].Tp,
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, args.SessionInfo)
		return nil
	}
	// If the state of the lock is in pre-lock, then the lock must be locked by the current request. So we can just return here.
	// Because the lock/unlock job must be serial execution in DDL owner now.
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if (tbInfo.Lock.Tp == pmodel.TableLockRead && args.LockTables[idx].Tp == pmodel.TableLockRead) ||
		(tbInfo.Lock.Tp == pmodel.TableLockReadOnly && args.LockTables[idx].Tp == pmodel.TableLockReadOnly) {
		sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, args.SessionInfo)
		// repeat lock.
		if sessionIndex >= 0 {
			return nil
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, args.SessionInfo)
		return nil
	}

	// Unlock tables should execute before lock tables.
	// Normally execute to here is impossible.
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

// checkTableLocked uses to check whether table was locked.
func checkTableLocked(tbInfo *model.TableInfo, lockTp pmodel.TableLockType, sessionInfo model.SessionInfo) error {
	if !tbInfo.IsLocked() {
		return nil
	}
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if (tbInfo.Lock.Tp == pmodel.TableLockRead && lockTp == pmodel.TableLockRead) ||
		(tbInfo.Lock.Tp == pmodel.TableLockReadOnly && lockTp == pmodel.TableLockReadOnly) {
		return nil
	}
	sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, sessionInfo)
	// If the request session already locked the table before, In other words, repeat lock.
	if sessionIndex >= 0 {
		if tbInfo.Lock.Tp == lockTp {
			return nil
		}
		// If no other session locked this table, and it is not a read only lock (session unrelated).
		if len(tbInfo.Lock.Sessions) == 1 && tbInfo.Lock.Tp != pmodel.TableLockReadOnly {
			return nil
		}
	}
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

// unlockTables uses unlock a batch of table lock one by one.
func unlockTables(jobCtx *jobContext, t *meta.Meta, job *model.Job, args *model.LockTablesArgs) (ver int64, err error) {
	if args.IndexOfUnlock >= len(args.UnlockTables) {
		return ver, nil
	}
	job.SchemaID = args.UnlockTables[args.IndexOfUnlock].SchemaID
	job.TableID = args.UnlockTables[args.IndexOfUnlock].TableID
	tbInfo, err := getTableInfo(t, job.TableID, job.SchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			// The table maybe has been dropped. just ignore this err and go on.
			args.IndexOfUnlock++
			job.FillArgs(args)
			return ver, nil
		}
		return ver, err
	}

	needUpdateTableInfo := unlockTable(tbInfo, args)
	if needUpdateTableInfo {
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	args.IndexOfUnlock++
	job.FillArgs(args)
	return ver, nil
}

// unlockTable uses to unlock table lock that hold by the session.
func unlockTable(tbInfo *model.TableInfo, args *model.LockTablesArgs) (needUpdateTableInfo bool) {
	if !tbInfo.IsLocked() {
		return false
	}
	if args.IsCleanup {
		tbInfo.Lock = nil
		return true
	}

	sessionIndex := findSessionInfoIndex(tbInfo.Lock.Sessions, args.SessionInfo)
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

func onUnlockTables(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, err error) {
	args, err := model.GetLockTablesArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = unlockTables(jobCtx, t, job, args)
	if args.IndexOfUnlock == len(args.UnlockTables) {
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, nil)
	}
	return ver, err
}
