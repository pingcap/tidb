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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/errors2"
)

// Set sets the string value of the key.
func (t *TStructure) Set(key []byte, value []byte) error {
	ek := encodeStringDataKey(key)
	return t.txn.Set(ek, value)
}

// Get gets the string value of a key.
func (t *TStructure) Get(key []byte) ([]byte, error) {
	ek := encodeStringDataKey(key)
	value, err := t.txn.Get(ek)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return value, errors.Trace(err)
}

// Inc increments the integer value of a key by step, returns
// the value after the increment.
func (t *TStructure) Inc(key []byte, step int64) (int64, error) {
	ek := encodeStringDataKey(key)
	n, err := t.txn.Inc(ek, step)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return n, errors.Trace(err)
}

// Clear removes the string value of the key.
func (t *TStructure) Clear(key []byte) error {
	ek := encodeStringDataKey(key)
	err := t.txn.Delete(ek)
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		err = nil
	}
	return errors.Trace(err)
}
