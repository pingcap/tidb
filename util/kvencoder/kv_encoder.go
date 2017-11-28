// Copyright 2017 PingCAP, Inc.
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

package kvenc

import (
	"fmt"
	"sync/atomic"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/mysql"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	goctx "golang.org/x/net/context"
)

var _ KvEncoder = &kvEncoder{}
var mockConnID uint64

// KvPair is a key-value pair.
type KvPair struct {
	// Key is the key of the pair.
	Key []byte
	// Val is the value of the pair. if the op is delete, the len(Val) == 0
	Val []byte
}

// KvEncoder is a encoder that transfer sql to key-value pairs.
type KvEncoder interface {
	// Encode transfer sql to kv pairs.
	// before use Encode() method, please make sure you already call Schema() method.
	// NOTE: now we just support transfer insert statement to kv pairs.
	// (if we wanna support other statement, we need to add a kv.Storage parameter,
	// and pass tikv store in.)
	Encode(sql string) ([]KvPair, error)

	// ExecDDLSQL execute ddl sql, you must use it to create schema infos.
	ExecDDLSQL(sql string) error

	// SetTableID set KvEncoder's tableID, just for test
	SetTableID(tableID int64)
}

type kvEncoder struct {
	store   kv.Storage
	dom     *domain.Domain
	se      tidb.Session
	tableID int64
}

// New new a KvEncoder
func New(dbName string, tableID int64, idAlloc autoid.Allocator) (KvEncoder, error) {
	kvEnc := &kvEncoder{}
	err := kvEnc.initial(dbName, idAlloc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kvEnc.tableID = tableID
	return kvEnc, nil
}

func (e *kvEncoder) Encode(sql string) ([]KvPair, error) {
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	_, err := e.se.Execute(goctx.Background(), sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer func() {
		err1 := e.se.RollbackTxn(goctx.Background())
		if err1 != nil {
			log.Error(errors.ErrorStack(err1))
		}
	}()

	txn := e.se.Txn()
	kvPairs := make([]KvPair, 0, txn.Len())
	err = kv.WalkMemBuffer(txn.GetMemBuffer(), func(k kv.Key, v []byte) error {
		if tablecodec.IsRecordKey(k) {
			k = tablecodec.ReplaceRecordKeyTableID(k, e.tableID)
		}
		kvPairs = append(kvPairs, KvPair{Key: k, Val: v})
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvPairs, nil
}

func (e *kvEncoder) ExecDDLSQL(sql string) error {
	_, err := e.se.Execute(goctx.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *kvEncoder) SetTableID(tableID int64) {
	e.tableID = tableID
}

func newMockTikvWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := tikv.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tidb.SetSchemaLease(0)
	dom, err := tidb.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

func (e *kvEncoder) initial(dbName string, idAlloc autoid.Allocator) error {
	store, dom, err := newMockTikvWithBootstrap()
	if err != nil {
		return errors.Trace(err)
	}
	se, err := tidb.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}

	se.SetConnectionID(atomic.AddUint64(&mockConnID, 1))
	_, err = se.Execute(goctx.Background(), fmt.Sprintf("use %s", dbName))
	if err != nil {
		return errors.Trace(err)
	}

	se.GetSessionVars().IDAllocator = idAlloc
	se.GetSessionVars().SkipConstraintCheck = true
	e.se = se
	e.store = store
	e.dom = dom
	return nil
}
