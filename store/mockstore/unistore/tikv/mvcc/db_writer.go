// Copyright 2019-present PingCAP, Inc.
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

package mvcc

import (
	"sync"

	"github.com/pingcap/badger"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
)

// DBWriter is the interface to persistent data.
type DBWriter interface {
	Open()
	Close()
	Write(batch WriteBatch) error
	DeleteRange(start, end []byte, latchHandle LatchHandle) error
	NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) WriteBatch
}

// LatchHandle is the interface for handling latches.
type LatchHandle interface {
	AcquireLatches(hashVals []uint64)
	ReleaseLatches(hashVals []uint64)
}

// WriteBatch is the interface to batch write.
type WriteBatch interface {
	Prewrite(key []byte, lock *Lock)
	Commit(key []byte, lock *Lock)
	Rollback(key []byte, deleleLock bool)
	PessimisticLock(key []byte, lock *Lock)
	PessimisticRollback(key []byte)
}

// DBBundle represents the db bundle.
type DBBundle struct {
	DB         *badger.DB
	LockStore  *lockstore.MemStore
	MemStoreMu sync.Mutex
	StateTS    uint64
}

// DBSnapshot represents the db snapshot.
type DBSnapshot struct {
	Txn       *badger.Txn
	LockStore *lockstore.MemStore
}

// NewDBSnapshot returns a new db snapshot.
func NewDBSnapshot(db *DBBundle) *DBSnapshot {
	return &DBSnapshot{
		Txn:       db.DB.NewTransaction(false),
		LockStore: db.LockStore,
	}
}
