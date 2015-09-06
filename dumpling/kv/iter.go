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

package kv

import (
	"errors"

	"github.com/pingcap/tidb/util/codec"
)

var (
	// ErrClosed is used when close an already closed txn.
	ErrClosed = errors.New("Error: Transaction already closed")
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = errors.New("Error: key not exist")
	// ErrConditionNotMatch is used when condition is not met.
	ErrConditionNotMatch = errors.New("Error: Condition not match")
	// ErrLockConflict is used when try to lock an already locked key.
	ErrLockConflict = errors.New("Error: Lock conflict")
)

var (
	// keyPrefix is used to avoid key conflict with some database metadata keys.
	keyPrefix    = []byte("z")
	codecEncoder = &encoder{
		codec.EncodeKey,
		codec.DecodeKey,
	}

	defaultEncoder = codecEncoder
)

type encoder struct {
	enc func(...interface{}) ([]byte, error)
	dec func([]byte) ([]interface{}, error)
}

// EncodeKey appends the k behind keyPrefix.
func EncodeKey(k []byte) []byte {
	return append(keyPrefix, k...)
}

// DecodeKey removes the prefixed keyPrefix.
func DecodeKey(k []byte) []byte {
	return k[len(keyPrefix):]
}

// EncodeValue encodes values before it is stored to the KV store.
func EncodeValue(values ...interface{}) ([]byte, error) {
	return defaultEncoder.enc(values...)
}

// DecodeValue decodes values after it is fetched from the KV store.
func DecodeValue(data []byte) ([]interface{}, error) {
	return defaultEncoder.dec(data)
}

// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or occur error
func NextUntil(it Iterator, fn FnKeyCmp) (Iterator, error) {
	var err error
	for it.Valid() && !fn([]byte(it.Key())) {
		it, err = it.Next(nil)
		if err != nil {
			return nil, err
		}
	}
	return it, nil
}
