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
	"bytes"
	"fmt"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
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

// KvEncoder is an encoder that transfer sql to key-value pairs.
type KvEncoder interface {
	// Encode transfers sql to kv pairs.
	// Before use Encode() method, please make sure you already call Schema() method.
	// NOTE: now we just support transfers insert statement to kv pairs.
	// (if we wanna support other statement, we need to add a kv.Storage parameter,
	// and pass tikv store in.)
	// return encoded kvs array that generate by sql, and affectRows count.
	Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error)

	// ExecDDLSQL executes ddl sql, you must use it to create schema infos.
	ExecDDLSQL(sql string) error

	// Close cleanup the kvEncoder.
	Close() error
}

type kvEncoder struct {
	store kv.Storage
	dom   *domain.Domain
	se    tidb.Session
}

// New new a KvEncoder
func New(dbName string, idAlloc autoid.Allocator) (KvEncoder, error) {
	kvEnc := &kvEncoder{}
	err := kvEnc.initial(dbName, idAlloc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return kvEnc, nil
}

func (e *kvEncoder) Close() error {
	e.dom.Close()
	if err := e.store.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *kvEncoder) Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error) {
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	_, err = e.se.Execute(goctx.Background(), sql)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	defer func() {
		err1 := e.se.RollbackTxn(goctx.Background())
		if err1 != nil {
			log.Error(errors.ErrorStack(err1))
		}
	}()

	txnMemBuffer := e.se.Txn().GetMemBuffer()
	kvPairs = make([]KvPair, 0, txnMemBuffer.Len())
	err = kv.WalkMemBuffer(txnMemBuffer, func(k kv.Key, v []byte) error {
		if bytes.HasPrefix(k, tablecodec.TablePrefix()) {
			k = tablecodec.ReplaceRecordKeyTableID(k, tableID)
		}
		kvPairs = append(kvPairs, KvPair{Key: k, Val: v})
		return nil
	})

	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return kvPairs, e.se.GetSessionVars().StmtCtx.AffectedRows(), nil
}

func (e *kvEncoder) ExecDDLSQL(sql string) error {
	_, err := e.se.Execute(goctx.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
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
