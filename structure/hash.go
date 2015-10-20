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
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/errors2"
)

type hashMeta struct {
	Length int64
}

func (m hashMeta) Value() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Length))
	return buf
}

func (m hashMeta) IsEmpty() bool {
	return m.Length <= 0
}

// HSet sets the string value of a hash field.
func (t *TStructure) HSet(key []byte, field []byte, value []byte) error {
	metaKey := encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return errors.Trace(err)
	}

	dataKey := encodeHashDataKey(key, field)
	var oldValue []byte
	oldValue, err = t.loadHashValue(dataKey)
	if err != nil {
		return errors.Trace(err)
	}

	if oldValue == nil {
		meta.Length++
	}

	if err = t.txn.Set(dataKey, value); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(t.txn.Set(metaKey, meta.Value()))
}

// HGet gets the value of a hash field.
func (t *TStructure) HGet(key []byte, field []byte) ([]byte, error) {
	dataKey := encodeHashDataKey(key, field)
	value, err := t.txn.Get(dataKey)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return value, errors.Trace(err)
}

// HLen gets the number of fields in a hash.
func (t *TStructure) HLen(key []byte) (int64, error) {
	metaKey := encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return meta.Length, nil
}

// HDel deletes one or more hash fields.
func (t *TStructure) HDel(key []byte, fields ...[]byte) error {
	metaKey := encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return errors.Trace(err)
	}

	var value []byte
	for _, field := range fields {
		dataKey := encodeHashDataKey(key, field)

		value, err = t.loadHashValue(dataKey)
		if err != nil {
			return errors.Trace(err)
		}

		if value != nil {
			if err = t.txn.Delete(dataKey); err != nil {
				return errors.Trace(err)
			}

			meta.Length--
		}
	}

	if meta.IsEmpty() {
		err = t.txn.Delete(metaKey)
	} else {
		err = t.txn.Set(metaKey, meta.Value())
	}

	return errors.Trace(err)
}

// HKeys gets all the fields in a hash.
func (t *TStructure) HKeys(key []byte) ([][]byte, error) {
	var keys [][]byte
	err := t.iterateHash(key, func(field []byte, value []byte) error {
		keys = append(keys, append([]byte{}, field...))
		return nil
	})

	return keys, errors.Trace(err)
}

// HClear removes the hash value of the key.
func (t *TStructure) HClear(key []byte) error {
	metaKey := encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return errors.Trace(err)
	}

	if meta.IsEmpty() {
		return nil
	}

	err = t.iterateHash(key, func(field []byte, value []byte) error {
		k := encodeHashDataKey(key, field)
		return errors.Trace(t.txn.Delete(k))
	})

	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(t.txn.Delete(metaKey))
}

func (t *TStructure) iterateHash(key []byte, fn func(k []byte, v []byte) error) error {
	dataPrefix := hashDataKeyPrefix(key)
	it, err := t.txn.Seek(dataPrefix)
	if err != nil {
		return errors.Trace(err)
	}

	var field []byte

	for it.Valid() {
		k := []byte(it.Key())
		if !bytes.HasPrefix(k, dataPrefix) {
			break
		}

		_, field, err = decodeHashDataKey(k)
		if err != nil {
			return errors.Trace(err)
		}

		if err = fn(field, it.Value()); err != nil {
			return errors.Trace(err)
		}

		it, err = it.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (t *TStructure) loadHashMeta(metaKey []byte) (hashMeta, error) {
	v, err := t.txn.Get(metaKey)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	} else if err != nil {
		return hashMeta{}, errors.Trace(err)
	}

	meta := hashMeta{Length: 0}
	if v == nil {
		return meta, nil
	}

	if len(v) != 8 {
		return meta, errors.Errorf("invalid list meta data")
	}

	meta.Length = int64(binary.BigEndian.Uint64(v[0:8]))
	return meta, nil
}

func (t *TStructure) loadHashValue(dataKey []byte) ([]byte, error) {
	v, err := t.txn.Get(dataKey)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
		v = nil
	} else if err != nil {
		return nil, errors.Trace(err)
	}

	return v, nil
}
