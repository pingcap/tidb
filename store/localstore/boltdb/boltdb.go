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

	"github.com/juju/errors"
	"github.com/ngaut/bolt"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ engine.DB = (*db)(nil)
)

var (
	bucketName = []byte("tidb_bucket")
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

func (d *db) GetSnapshot() (engine.Snapshot, error) {
	tx, err := d.DB.Begin(false)
	if err != nil {
		return nil, err
	}
	return &snapshot{tx}, nil
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
		for _, w := range bt.Writes {
			if w.Value != nil {
				err1 = b.Put(w.Key, w.Value)
			} else {
				err1 = b.Delete(w.Key)
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

type snapshot struct {
	*bolt.Tx
}

func (s *snapshot) Get(key []byte) ([]byte, error) {
	b := s.Tx.Bucket(bucketName)
	v := b.Get(key)
	if v == nil {
		return nil, nil
	}

	value := append([]byte(nil), v...)

	return value, nil
}

func (s *snapshot) NewIterator(startKey []byte) engine.Iterator {
	return &iterator{tx: s.Tx, key: startKey}
}

func (s *snapshot) Release() {
	err := s.Tx.Rollback()
	if err != nil {
		log.Errorf("commit err %v", err)
	}
}

type iterator struct {
	tx *bolt.Tx
	*bolt.Cursor

	key   []byte
	value []byte
}

func (i *iterator) Next() bool {
	if i.Cursor == nil {
		i.Cursor = i.tx.Bucket(bucketName).Cursor()
		if i.key == nil {
			i.key, i.value = i.Cursor.First()
		} else {
			i.key, i.value = i.Cursor.Seek(i.key)
		}
	} else {
		i.key, i.value = i.Cursor.Next()
	}

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
	Key   []byte
	Value []byte
}

type batch struct {
	Writes []write
}

func (b *batch) Put(key []byte, value []byte) {
	w := write{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	b.Writes = append(b.Writes, w)
}

func (b *batch) Delete(key []byte) {
	w := write{
		Key:   append([]byte(nil), key...),
		Value: nil,
	}
	b.Writes = append(b.Writes, w)
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
