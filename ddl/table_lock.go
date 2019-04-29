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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
)

func onLockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	arg := &lockTablesArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fmt.Printf("on lock table: arg: %#v\n---------\n\n", arg)

	tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, err
	}
	if tbInfo.Lock == nil {
		tbInfo.Lock = &model.TableLockInfo{}
	}

	switch tbInfo.Lock.State {
	case model.TableLockStateNone:
		// none -> pre_lock
		err = checkLockTable(tbInfo, 0, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		tbInfo.Lock.Tp = arg.LockTypes[0]
		tbInfo.Lock.ServerIDs = append(tbInfo.Lock.ServerIDs, arg.ServerID)
		tbInfo.Lock.SessionIDs = append(tbInfo.Lock.SessionIDs, arg.SessionID)
		tbInfo.Lock.State = model.TableLockStatePreLock
		tbInfo.Lock.TS = t.StartTS
		job.SchemaState = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
	case model.TableLockStatePreLock:
		tbInfo.Lock.State = model.TableLockStatePublic
		tbInfo.Lock.TS = t.StartTS
		ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		return ver, ErrInvalidTableLockState.GenWithStack("invalid table lock state %v", tbInfo.Lock.State)

	}
	return ver, err
}

func checkLockTable(tbInfo *model.TableInfo, idx int, arg *lockTablesArg) error {
	if tbInfo.Lock == nil || len(tbInfo.Lock.ServerIDs) == 0 {
		return nil
	}
	if tbInfo.Lock.Tp == model.TableLockRead && arg.LockTypes[idx] == model.TableLockRead {
		return nil
	}

	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.ServerIDs, tbInfo.Lock.SessionIDs)
}

func onUnlockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	lockTablesArg := &lockTablesArg{}
	if err := job.DecodeArgs(lockTablesArg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fmt.Printf("on unlock table: arg: %#v\n---------\n\n", lockTablesArg)

	tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, err
	}
	// Nothing need to do.
	if tbInfo.Lock == nil {
		// should never run to this.
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("the lock of table %v was released, this should never hapen", tbInfo.Name.L)
	}

	tbInfo.Lock = nil
	ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	return ver, nil
}
