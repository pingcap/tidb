// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var _  MemBuffer = &fileDBBuffer{}

// fileDBBuffer implements the MemBuffer interface.
// It uses file-system backed storage to reduce memory usage.
// Used by the large transaction.
type fileDBBuffer struct {
	db *leveldb.DB
	size int
	len int
}

func NewFileDBBuffer(path string) (MemBuffer, error) {
	db, err := leveldb.OpenFile(path,
		&opt.Options{ErrorIfExist : true})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &fileDBBuffer{
		db: db,
	}, nil
}

func (mb *fileDBBuffer) Size() int {
	return mb.size
}

func (mb *fileDBBuffer) Len() int {
	return mb.len
}

func (mb *fileDBBuffer) Reset() {
	mb.db.Close()
	mb.db = nil
}

func (mb *fileDBBuffer) SetCap(cap int) {}

func (mb *fileDBBuffer) Get(ctx context.Context, k Key) ([]byte, error) {
	return mb.db.Get(k, nil)
}

type fileDBBufferIter struct {
	iterator.Iterator
	valid bool
}

func (i *fileDBBufferIter) Valid() bool {
	return i.valid
}

func (i *fileDBBufferIter) Key() Key {
	return i.Iterator.Key()
}

func (i *fileDBBufferIter) Next() error {
	i.valid = i.Iterator.Next()
	return nil
}

func (iter *fileDBBufferIter) Close() {
	iter.Iterator.Release()
}

func (mb *fileDBBuffer) Iter(k Key, upperBound Key) (Iterator, error) {
	slice := util.Range{
		Start: k,
		Limit: upperBound,
	}
	iter := mb.db.NewIterator(&slice, nil)
	i := &fileDBBufferIter{Iterator:iter, valid: true}
	i.Next()
	return i, nil
}

func (mb *fileDBBuffer) IterReverse(k Key) (Iterator, error) {
	return nil, errors.New("not supported")
}

func (mb *fileDBBuffer) Set(k Key, v []byte) error {
	err := mb.db.Put(k, v, nil)
	if err != nil {
		return errors.Trace(err)
	}
	mb.size = mb.size + len(k) + len(v)
	mb.len++
	return nil
}

func (mb *fileDBBuffer) Delete(k Key) error {
	err := mb.db.Delete(k, nil)
	if err != nil {
		return errors.Trace(err)
	}
	mb.size += len(k)
	mb.len++
	return nil
}
