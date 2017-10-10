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

package boltdb

import (
	"os"
	"path"

	"github.com/boltdb/bolt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/terror"
)

var (
	_ engine.DB = (*db)(nil)
)

var (
	bucketName = []byte("tidb")
)

type db struct {
	*bolt.DB
}

func (d *db) Get(key []byte) ([]byte, error) {
	var value []byte

	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		v := b.Get(key)
		if v == nil {
			return errors.Trace(engine.ErrNotFound)
		}
		value = cloneBytes(v)
		return nil
	})

	return value, errors.Trace(err)
}

func (d *db) Seek(startKey []byte) ([]byte, []byte, error) {
	var key, value []byte
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		var k, v []byte
		if startKey == nil {
			k, v = c.First()
		} else {
			k, v = c.Seek(startKey)
		}
		if k != nil {
			key, value = cloneBytes(k), cloneBytes(v)
		}
		return nil
	})

	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if key == nil {
		return nil, nil, errors.Trace(engine.ErrNotFound)
	}
	return key, value, nil
}

func (d *db) SeekReverse(startKey []byte) ([]byte, []byte, error) {
	var key, value []byte
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		var k, v []byte
		if startKey == nil {
			k, v = c.Last()
		} else {
			c.Seek(startKey)
			k, v = c.Prev()
		}
		if k != nil {
			key, value = cloneBytes(k), cloneBytes(v)
		}
		return nil
	})

	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if key == nil {
		return nil, nil, errors.Trace(engine.ErrNotFound)
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
	err := d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		var err error
		for _, w := range bt.writes {
			if !w.isDelete {
				err = b.Put(w.key, w.value)
			} else {
				err = b.Delete(w.key)
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
	return d.DB.Close()
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

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates a local storage database with given path.
func (driver Driver) Open(dbPath string) (engine.DB, error) {
	base := path.Dir(dbPath)
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}

	d, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tx, err := d.Begin(true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if _, err = tx.CreateBucketIfNotExists(bucketName); err != nil {
		err1 := tx.Rollback()
		terror.Log(errors.Trace(err1))
		return nil, errors.Trace(err)
	}

	if err = tx.Commit(); err != nil {
		return nil, errors.Trace(err)
	}

	return &db{d}, nil
}

// cloneBytes returns a deep copy of slice b.
func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
