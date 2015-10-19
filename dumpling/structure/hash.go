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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/errors2"
)

// HSet sets the string value of a hash field.
func (t *TStructure) HSet(key []byte, field []byte, value []byte) error {
	dateKey := encodeHashDataKey(key, field)
	return t.txn.Set(dateKey, value)
}

// HGet gets the value of a hash field.
func (t *TStructure) HGet(key []byte, field []byte) ([]byte, error) {
	dateKey := encodeHashDataKey(key, field)
	value, err := t.txn.Get(dateKey)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return value, errors.Trace(err)
}

// HLen gets the number of fields in a hash.
func (t *TStructure) HLen(key []byte) (int64, error) {
	// TODO: in our scenario, we may use this rarely, so here
	// just using iteration, not another meta key, if we really need
	// this, we will update it.
	cnt := int64(0)

	err := t.iterateHash(key, func(field []byte, value []byte) error {
		cnt++
		return nil
	})

	return cnt, errors.Trace(err)
}

// HDel deletes one or more hash fields.
func (t *TStructure) HDel(key []byte, fields ...[]byte) error {
	for _, field := range fields {
		dataKey := encodeHashDataKey(key, field)
		if err := t.txn.Delete(dataKey); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
	err := t.iterateHash(key, func(field []byte, value []byte) error {
		// TODO: optimize
		k := encodeHashDataKey(key, field)
		return errors.Trace(t.txn.Delete(k))
	})
	return errors.Trace(err)
}

func (t *TStructure) iterateHash(key []byte, fn func(k []byte, v []byte) error) error {
	dataPrefix := hashDataKeyPrefix(key)
	it, err := t.txn.Seek(dataPrefix, nil)
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

		it, err = it.Next(nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
