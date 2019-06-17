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

package lock

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/sessionctx"
)

// Checker uses to check tables lock.
type Checker struct {
	ctx sessionctx.Context
	is  infoschema.InfoSchema
}

// NewChecker return new lock Checker.
func NewChecker(ctx sessionctx.Context, is infoschema.InfoSchema) *Checker {
	return &Checker{ctx: ctx, is: is}
}

// CheckTableLock uses to check table lock.
func (c *Checker) CheckTableLock(db, table string, privilege mysql.PrivilegeType) error {
	if db == "" || table == "" {
		return nil
	}
	// Below database are not support table lock.
	if db == infoschema.LowerName || db == perfschema.LowerName || db == mysql.SystemDB {
		return nil
	}
	switch privilege {
	case mysql.ShowDBPriv, mysql.AllPrivMask:
		// AllPrivMask only used in show create table statement now.
		return nil
	case mysql.CreatePriv, mysql.CreateViewPriv:
		if c.ctx.HasLockedTables() {
			return infoschema.ErrTableNotLocked.GenWithStackByArgs(table)
		}
		return nil
	}
	// TODO: try to remove this get for speed up.
	tb, err := c.is.TableByName(model.NewCIStr(db), model.NewCIStr(table))
	// Ignore this error for "drop table if not exists t1" when t1 doesn't exists.
	if infoschema.ErrTableNotExists.Equal(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if c.ctx.HasLockedTables() {
		if locked, tp := c.ctx.CheckTableLocked(tb.Meta().ID); locked {
			if checkLockTpMeetPrivilege(tp, privilege) {
				return nil
			}
			return infoschema.ErrTableNotLockedForWrite.GenWithStackByArgs(tb.Meta().Name)
		}
		return infoschema.ErrTableNotLocked.GenWithStackByArgs(tb.Meta().Name)
	}

	if tb.Meta().Lock == nil {
		return nil
	}

	if privilege == mysql.SelectPriv {
		switch tb.Meta().Lock.Tp {
		case model.TableLockRead, model.TableLockWriteLocal:
			return nil
		}
	}
	return infoschema.ErrTableLocked.GenWithStackByArgs(tb.Meta().Name.L, tb.Meta().Lock.Tp, tb.Meta().Lock.Sessions[0])
}

func checkLockTpMeetPrivilege(tp model.TableLockType, privilege mysql.PrivilegeType) bool {
	switch tp {
	case model.TableLockWrite, model.TableLockWriteLocal:
		return true
	case model.TableLockRead:
		// ShowDBPriv, AllPrivMask,CreatePriv, CreateViewPriv already checked before.
		// The other privilege in read lock was not allowed.
		if privilege == mysql.SelectPriv {
			return true
		}
	}
	return false
}
