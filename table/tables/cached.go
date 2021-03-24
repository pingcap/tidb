// Copyright 2021 PingCAP, Inc.
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

package tables

import (
	"fmt"
	"time"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
)

/// CachedTable implement the table.Table interface, it caches the whole table data.
type CachedTable struct {
	table.Table

	lease    uint64
	handles []kv.Handle
	fullRows [][]types.Datum
}

func (t *CachedTable) IterRecords(sctx sessionctx.Context, cols []*table.Column, fn table.RecordIterFunc) error {
	fmt.Println("run here!!! retrive cached table data ====")
	tbInfo := t.Table.Meta()
	switch tbInfo.Cache.State {
	case model.TableCacheStateSwitching:
		// Read from the real table.
		// TODO: handle partition.
		return IterRecords(t.Table, sctx, t.Table.Cols(), fn)
	case model.TableCacheStateEnabled:
		data, handles, err := t.readAll(sctx)
		if err != nil {
			return err
		}
		for i :=0; i<len(data); i++ {
			more, err1 := fn(handles[i], data[i], cols)
			if err1 != nil {
				return err1
			}
			if !more {
				fmt.Println("no more data, return...")
				break
			}
		}
		return nil
	}
	return errors.New("should never run here: table cache state wrong!")
}

// readAllCachedTable first try to read from the cache, if the read lock lease is there.
// If the lease TTL is coming, it will try to renew the lease asynchronously.
// If the cache is not available, the operation read from the remote, and
// try to refill the cache asynchronously.
func (t *CachedTable) readAll(sctx sessionctx.Context) (data [][]types.Datum, handles []kv.Handle, err error) {
	txn, err1 := sctx.Txn(true)
	if err1 != nil {
		return nil, nil, err1
	}
	startTS := txn.StartTS()
	lease := t.lease

	if leaseValid(lease, startTS) {
		fmt.Println("lease is VALID, startTS =", startTS, " and lease = ", lease)
		data, handles = t.fullRows, t.handles
		if leaseShouldRenew(lease, startTS) {
			fmt.Println("lease need RENEW ...", startTS, " and lease = ", lease)
			t.notifyUpdate(sctx, t.Table.Meta().ID, startTS)
		}
	} else {
		fmt.Println("lease is OUTDATED, read from remote startTS =", startTS, " and lease = ", lease)
		tm := oracle.GetTimeFromTS(startTS)
		tm = tm.Add(3 * time.Second)
		lease := oracle.GoTimeToTS(tm)
		sr := stateRemote{sctx.(sqlexec.RestrictedSQLExecutor)}
		ok, err := sr.LockForRead(context.Background(), t.Meta().ID, lease)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		data, handles, err = t.readAllFromRemote(sctx)
		if ok {
			t.fullRows = data
			t.handles = handles
			t.lease = lease
		}
	}

	return
}

// TODO: table.Table should be a physical table!
func (t *CachedTable) readAllFromRemote(sctx sessionctx.Context) ([][]types.Datum, []kv.Handle, error) {
	rows := make([][]types.Datum, 0, 1024)
	handles := make([]kv.Handle, 0, 1024)
	err := IterRecords(t.Table, sctx, t.Table.Cols(), func(h kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
		rows = append(rows, rec)
		handles = append(handles, h)
		return true, nil
	})
	return rows, handles, err
}

func (t *CachedTable) notifyUpdate(sctx sessionctx.Context, tid int64, startTS uint64) {
	lease := tsoAddDuration(startTS, 3*time.Second)
	sr := stateRemote{sctx.(sqlexec.RestrictedSQLExecutor)}
	ok, err := sr.RenewLease(context.Background(), tid, startTS, lease)
	if err != nil || !ok {
		// log print on err?
		return
	}

	rows, handles, err := t.readAllFromRemote(sctx)
	if err != nil {
		return
	}
	t.fullRows = rows
	t.handles = handles
	t.lease = lease
}


func leaseValid(lease, startTS uint64) bool {
	return lease > startTS
}

func leaseShouldRenew(lease, startTS uint64) bool {
	return tsoAddDuration(startTS, time.Second + 500*time.Millisecond) >= lease
}

func tsoAddDuration(ts uint64, dur time.Duration) uint64 {
	t := oracle.GetTimeFromTS(ts)
	t = t.Add(dur)
	return oracle.GoTimeToTS(t)
}

func setTableCacheOptionForTxn(ctx sessionctx.Context, tid int64) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	val := txn.GetOption(kv.ModifyCachedTable)
	if val == nil {
		m := make(map[int64]struct{})
		m[tid] = struct{}{}
		txn.SetOption(kv.ModifyCachedTable, m)
	} else {
		ref := val.(map[int64]struct{})
		ref[tid] = struct{}{}
	}
	return nil
}

func (t *CachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	if err := setTableCacheOptionForTxn(ctx, t.Meta().ID); err != nil {
		return nil, err
	}
	return t.Table.AddRecord(ctx, r, opts...)
}

type StateRemote interface {
	LockForRead(ctx context.Context, tid int64, lease uint64) (bool, error)
	RenewLease(ctx context.Context, tid int64, startTS, lease uint64) (bool, error)
	LockForWrite(ctx context.Context, tid int64) (time.Duration, error)
	// WriteAndUnlock()
	// CleanOrphanLock()
}

type stateRemote struct {
	sqlexec.RestrictedSQLExecutor
}

func (sr stateRemote) LockForRead(ctx context.Context, tid int64, lease uint64) (bool, error) {
	stmt, err := sr.ParseWithParams(ctx, `UPDATE mysql.table_cache SET lock_type = 'READ', lease = %? WHERE tid = %? AND lock_type != 'WRITE'`, lease, tid)
	if err != nil {
		return false, err
	}

	_, _, err = sr.ExecRestrictedStmt(ctx, stmt)
	if err != nil {
		return false, err
	}

	fmt.Println("LockForRead ===", tid, lease)
	return true, nil
}

func (sr stateRemote) RenewLease(ctx context.Context, tid int64, startTS, lease uint64) (bool, error) {
	stmt, err := sr.ParseWithParams(ctx, `UPDATE mysql.table_cache SET lease = %? WHERE tid = %? AND lock_type = 'READ'`, lease, tid)
	if err != nil {
		return false, err
	}

	_, _, err = sr.ExecRestrictedStmt(ctx, stmt)
	if err != nil {
		return false, err
	}

	fmt.Println("RenewLease ===", tid, lease)
	return true, nil
}
