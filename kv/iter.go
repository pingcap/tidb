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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import "github.com/pingcap/errors"

// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
func NextUntil(it Iterator, fn FnKeyCmp) error {
	var err error
	for it.Valid() && !fn(it.Key()) {
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

type SliceIter struct {
	data []*Entry
	cur  int
}

func NewSliceIter(data []*Entry) *SliceIter {
	return &SliceIter{
		data,
		0,
	}
}

// GetSlice returns the inner slice
func (i *SliceIter) GetSlice() []*Entry {
	return i.data
}

// Valid returns true if the current iterator is valid.
func (i *SliceIter) Valid() bool {
	return i.cur >= 0 && i.cur < len(i.data)
}

// Key returns the current key.
func (i *SliceIter) Key() Key {
	if !i.Valid() {
		return nil
	}
	return i.data[i.cur].Key
}

// Value returns the current value.
func (i *SliceIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.data[i.cur].Value
}

// Next goes the next position. Always return error for this iterator
func (i *SliceIter) Next() error {
	if !i.Valid() {
		return errors.New("iterator is invalid")
	}

	i.cur++
	return nil
}

// Close closes the iterator.
func (i *SliceIter) Close() {
	i.cur = -1
}
