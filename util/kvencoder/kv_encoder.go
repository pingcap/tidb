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
	Encode(sql string) ([]KvPair, error)
}

type kvEncoder struct {
	store kv.Storage
	se    tidb.Session
}

// New new a KvEncoder
func New(store kv.Storage, dbName string, idAlloc autoid.Allocator, skipConstraintCheck bool) (KvEncoder, error) {
	kvEnc := &kvEncoder{}
	err := kvEnc.initial(store, dbName, idAlloc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kvEnc.se.GetSessionVars().SkipConstraintCheck = skipConstraintCheck
	return kvEnc, nil
}

func (e *kvEncoder) Encode(sql string) ([]KvPair, error) {
	if e.se == nil {
		return nil, errors.Errorf("Please call KvEncoder.Init() first.")
	}

	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	_, err := e.se.Execute(goctx.Background(), sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	txn := e.se.Txn()
	kvPairs := make([]KvPair, 0, txn.Len())
	defer func() {
		err1 := e.se.RollbackTxn(goctx.Background())
		if err1 != nil {
			log.Error(errors.ErrorStack(err1))
		}
	}()

	err = kv.WalkMemBuffer(txn.GetMemBuffer(), func(k kv.Key, v []byte) error {
		kvPairs = append(kvPairs, KvPair{Key: k, Val: v})
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvPairs, nil
}

func (e *kvEncoder) initial(store kv.Storage, dbName string, idAlloc autoid.Allocator) error {
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
	e.se = se
	e.store = store
	return nil
}
