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
	"github.com/pingcap/tidb/util/bytes"
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
			return nil
		}

		value = append([]byte(nil), v...)
		return nil
	})

	return value, errors.Trace(err)
}

func (d *db) Seek(startKey []byte) (engine.Iterator, error) {
	return &iterator{key: startKey, seeked: false, d: d}, nil
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
		// err1 is used for passing `go tool vet --shadow` check.
		var err1 error
		for _, w := range bt.writes {
			if !w.isDelete {
				err1 = b.Put(w.key, w.value)
			} else {
				err1 = b.Delete(w.key)
			}

			if err1 != nil {
				return errors.Trace(err1)
			}
		}

		return nil
	})
	return errors.Trace(err)
}

func (d *db) Close() error {
	return d.DB.Close()
}

type iterator struct {
	d      *db
	seeked bool
	key    []byte
	value  []byte
}

func (i *iterator) Next() bool {
	i.d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		var key []byte
		var value []byte

		if !i.seeked {
			i.seeked = true
			if i.key == nil {
				key, value = c.First()
			} else {
				key, value = c.Seek(i.key)
			}
		} else {
			c.Seek(i.key)
			key, value = c.Next()
		}

		i.key = bytes.CloneBytes(key)
		i.value = bytes.CloneBytes(value)

		return nil
	})

	return i.key != nil
}

func (i *iterator) Seek(startKey []byte) bool {
	i.d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		var key []byte
		var value []byte

		key, value = c.Seek(startKey)

		i.key = bytes.CloneBytes(key)
		i.value = bytes.CloneBytes(value)

		return nil
	})

	return i.key != nil
}

func (i *iterator) Key() []byte {
	return i.key
}

func (i *iterator) Value() []byte {
	return i.value
}

func (i *iterator) Release() {

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
	os.MkdirAll(base, 0755)

	d, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	tx, err := d.Begin(true)
	if err != nil {
		return nil, err
	}

	if _, err = tx.CreateBucketIfNotExists(bucketName); err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return &db{d}, nil
}
