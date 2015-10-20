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

package structure

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/errors2"
)

type listMeta struct {
	LIndex int64
	RIndex int64
}

func (m listMeta) Value() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.LIndex))
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.RIndex))
	return buf
}

func (m listMeta) IsEmpty() bool {
	return m.LIndex >= m.RIndex
}

// LPush prepends one or multiple values to a list.
func (t *TStructure) LPush(key []byte, values ...[]byte) error {
	return t.listPush(key, true, values...)
}

// RPush appends one or multiple values to a list.
func (t *TStructure) RPush(key []byte, values ...[]byte) error {
	return t.listPush(key, false, values...)
}

func (t *TStructure) listPush(key []byte, left bool, values ...[]byte) error {
	if len(values) == 0 {
		return nil
	}

	metaKey := t.encodeListMetaKey(key)
	m, err := t.loadListMeta(metaKey)
	if err != nil {
		return errors.Trace(err)
	}

	if err = t.txn.LockKeys(metaKey); err != nil {
		return errors.Trace(err)
	}

	index := int64(0)
	for _, v := range values {
		if left {
			m.LIndex--
			index = m.LIndex
		} else {
			index = m.RIndex
			m.RIndex++
		}

		dataKey := t.encodeListDataKey(key, index)
		if err = t.txn.Set(dataKey, v); err != nil {
			return errors.Trace(err)
		}
	}

	return t.txn.Set(metaKey, m.Value())
}

// LPop removes and gets the first element in a list.
func (t *TStructure) LPop(key []byte) ([]byte, error) {
	return t.listPop(key, true)
}

// RPop removes and gets the last element in a list.
func (t *TStructure) RPop(key []byte) ([]byte, error) {
	return t.listPop(key, false)
}

func (t *TStructure) listPop(key []byte, left bool) ([]byte, error) {
	metaKey := t.encodeListMetaKey(key)
	m, err := t.loadListMeta(metaKey)
	if err != nil || m.IsEmpty() {
		return nil, errors.Trace(err)
	}

	if err = t.txn.LockKeys(metaKey); err != nil {
		return nil, errors.Trace(err)
	}

	index := int64(0)
	if left {
		index = m.LIndex
		m.LIndex++
	} else {
		m.RIndex--
		index = m.RIndex
	}

	dataKey := t.encodeListDataKey(key, index)

	var data []byte
	data, err = t.txn.Get(dataKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = t.txn.Delete(dataKey); err != nil {
		return nil, errors.Trace(err)
	}

	if m.LIndex < m.RIndex {
		err = t.txn.Set(metaKey, m.Value())
	} else {
		err = t.txn.Delete(metaKey)
	}

	return data, errors.Trace(err)
}

// LLen gets the length of a list.
func (t *TStructure) LLen(key []byte) (int64, error) {
	metaKey := t.encodeListMetaKey(key)
	m, err := t.loadListMeta(metaKey)
	return m.RIndex - m.LIndex, errors.Trace(err)
}

// LIndex gets an element from a list by its index.
func (t *TStructure) LIndex(key []byte, index int64) ([]byte, error) {
	metaKey := t.encodeListMetaKey(key)
	m, err := t.loadListMeta(metaKey)
	if err != nil || m.IsEmpty() {
		return nil, errors.Trace(err)
	}

	index = adjustIndex(index, m.LIndex, m.RIndex)

	if index >= m.LIndex && index < m.RIndex {
		return t.txn.Get(t.encodeListDataKey(key, index))
	}
	return nil, nil
}

// LClear removes the list of the key.
func (t *TStructure) LClear(key []byte) error {
	metaKey := t.encodeListMetaKey(key)
	m, err := t.loadListMeta(metaKey)
	if err != nil || m.IsEmpty() {
		return errors.Trace(err)
	}

	if err = t.txn.LockKeys(metaKey); err != nil {
		return errors.Trace(err)
	}

	for index := m.LIndex; index < m.RIndex; index++ {
		dataKey := t.encodeListDataKey(key, index)
		if err = t.txn.Delete(dataKey); err != nil {
			return errors.Trace(err)
		}
	}

	return t.txn.Delete(metaKey)
}

func (t *TStructure) loadListMeta(metaKey []byte) (listMeta, error) {
	v, err := t.txn.Get(metaKey)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	} else if err != nil {
		return listMeta{}, errors.Trace(err)
	}

	meta := listMeta{0, 0}
	if v == nil {
		return meta, nil
	}

	if len(v) != 16 {
		return meta, errors.Errorf("invalid list meta data")
	}

	meta.LIndex = int64(binary.BigEndian.Uint64(v[0:8]))
	meta.RIndex = int64(binary.BigEndian.Uint64(v[8:16]))
	return meta, nil
}

func adjustIndex(index int64, min, max int64) int64 {
	if index >= 0 {
		return index + min
	}

	return index + max
}
