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

package lmdb

import (
	"os"
	"runtime"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ engine.DB = (*db)(nil)
)

type lmdbop struct {
	op  lmdb.TxnOp
	res chan<- error
}

type db struct {
	env   *lmdb.Env
	dbi   lmdb.DBI
	queue chan *lmdbop
}

func (d *db) worker() {
	runtime.LockOSThread()
	defer runtime.LockOSThread()

	for op := range d.queue {
		op.res <- d.env.UpdateLocked(op.op)
	}
}

func (d *db) update(op lmdb.TxnOp) error {
	res := make(chan error)
	d.queue <- &lmdbop{op, res}
	return <-res
}

func (d *db) Get(key []byte) ([]byte, error) {
	var value []byte

	err := d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(d.dbi, key)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return errors.Trace(engine.ErrNotFound)
			}
			return err
		}
		value = cloneBytes(v)
		return nil
	})
	return value, errors.Trace(err)
}

func (d *db) Seek(startKey []byte) ([]byte, []byte, error) {
	var key, value []byte
	err := d.env.View(func(txn *lmdb.Txn) error {
		// Set RawRead true to reduce once memory copy
		txn.RawRead = true
		c, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer c.Close()
		var k, v []byte
		if startKey == nil {
			k, v, err = c.Get(nil, nil, lmdb.First)
		} else {

			k, v, err = c.Get(startKey, nil, lmdb.SetRange)
		}
		if err != nil {
			if lmdb.IsNotFound(err) {
				return errors.Trace(engine.ErrNotFound)
			}
			return err
		}
		key, value = cloneBytes(k), cloneBytes(v)
		return nil
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return key, value, nil
}

func (d *db) SeekReverse(startKey []byte) ([]byte, []byte, error) {
	var key, value []byte
	err := d.env.View(func(txn *lmdb.Txn) error {
		// Set RawRead true to reduce once memory copy
		txn.RawRead = true
		c, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer c.Close()
		var k, v []byte
		if startKey == nil {
			k, v, err = c.Get(nil, nil, lmdb.Last)
		} else {
			// Seek to startKey's prev
			k, v, err = c.Get(startKey, nil, lmdb.SetRange)
			if lmdb.IsNotFound(err) {
				// If we got not found, we should give the last one
				k, v, err = c.Get(nil, nil, lmdb.Last)
			} else {
				// get prev item.
				k, v, err = c.Get(nil, nil, lmdb.Prev)
			}
		}
		if err != nil {
			if lmdb.IsNotFound(err) {
				return errors.Trace(engine.ErrNotFound)
			}
			return err
		}
		key, value = cloneBytes(k), cloneBytes(v)
		return nil
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return key, value, nil
}

func (d *db) NewBatch() engine.Batch {
	return &batch{}
}

func (d *db) Commit(b engine.Batch) error {
	bt, ok := b.(*batch)
	if !ok {
		return errors.Errorf("invalid batch type %T", b)
	}
	err := d.update(func(txn *lmdb.Txn) error {
		var err error
		for _, w := range bt.writes {
			if w.isDelete {
				err = txn.Del(d.dbi, w.key, nil)
				// If key is not found just ignore it
				if lmdb.IsNotFound(err) {
					err = nil
				}
			} else {
				err = txn.Put(d.dbi, w.key, w.value, 0)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return errors.Trace(err)
}

func (d *db) Close() error {
	return d.env.Close()
}

type write struct {
	key      []byte
	value    []byte
	isDelete bool
}

type batch struct {
	writes []write
}

func (b *batch) Put(key []byte, value []byte) {
	w := write{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	}
	b.writes = append(b.writes, w)
}

func (b *batch) Delete(key []byte) {
	w := write{
		key:      append([]byte(nil), key...),
		value:    nil,
		isDelete: true,
	}
	b.writes = append(b.writes, w)
}

func (b *batch) Len() int {
	return len(b.writes)
}

// Driver implements engine Driver
type Driver struct {
}

// Open opens or creates a local storage database with given path.
func (driver Driver) Open(dbPath string) (engine.DB, error) {
	os.MkdirAll(dbPath, 0755)

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		env.Close()
		return nil, errors.Trace(err)
	}
	err = env.SetMapSize(1 << 40)
	if err != nil {
		env.Close()
		return nil, errors.Trace(err)
	}
	err = env.Open(dbPath, 0, 0644)
	if err != nil {
		env.Close()
		return nil, errors.Trace(err)
	}
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		env.Close()
		return nil, errors.Trace(err)
	}
	if staleReaders > 0 {
		log.Info("[lmdb] cleared %d reader slots from dead processes", staleReaders)
	}

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("tidb")
		return err
	})
	if err != nil {
		env.Close()
		return nil, errors.Trace(err)
	}
	dbEngine := &db{
		env:   env,
		dbi:   dbi,
		queue: make(chan *lmdbop),
	}
	go dbEngine.worker()
	return dbEngine, nil
}

// cloneBytes returns a deep copy of slice b.
func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
