// Copyright 2019-present PingCAP, Inc.
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

package mvcc

import (
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// WriteType defines a write type.
type WriteType = byte

// WriteType
const (
	WriteTypeLock     WriteType = 'L'
	WriteTypeRollback WriteType = 'R'
	WriteTypeDelete   WriteType = 'D'
	WriteTypePut      WriteType = 'P'
)

// WriteCFValue represents a write CF value.
type WriteCFValue struct {
	Type     WriteType
	StartTS  uint64
	ShortVal []byte
}

var errInvalidWriteCFValue = errors.New("invalid write CF value")

// ParseWriteCFValue parses the []byte data and returns a WriteCFValue.
func ParseWriteCFValue(data []byte) (wv WriteCFValue, err error) {
	if len(data) == 0 {
		err = errInvalidWriteCFValue
		return
	}
	wv.Type = data[0]
	switch wv.Type {
	case WriteTypePut, WriteTypeDelete, WriteTypeLock, WriteTypeRollback:
	default:
		err = errInvalidWriteCFValue
		return
	}
	wv.ShortVal, wv.StartTS, err = codec.DecodeUvarint(data[1:])
	return
}

const (
	shortValuePrefix  = 'v'
	forUpdatePrefix   = 'f'
	minCommitTsPrefix = 'm'

	//ShortValueMaxLen defines max length of short value.
	ShortValueMaxLen = 64
)

// EncodeWriteCFValue accepts a write cf parameters and return the encoded bytes data.
// Just like the tikv encoding form. See tikv/src/storage/mvcc/write.rs for more detail.
func EncodeWriteCFValue(t WriteType, startTs uint64, shortVal []byte) []byte {
	data := make([]byte, 0)
	data = append(data, t)
	data = codec.EncodeUvarint(data, startTs)
	if len(shortVal) != 0 {
		data = append(data, byte(shortValuePrefix), byte(len(shortVal)))
		return append(data, shortVal...)
	}
	return data
}

// EncodeLockCFValue encodes the mvcc lock and returns putLock value and putDefault value if exists.
func EncodeLockCFValue(lock *Lock) ([]byte, []byte) {
	data := make([]byte, 0)
	switch lock.Op {
	case byte(kvrpcpb.Op_Put):
		data = append(data, LockTypePut)
	case byte(kvrpcpb.Op_Del):
		data = append(data, LockTypeDelete)
	case byte(kvrpcpb.Op_Lock):
		data = append(data, LockTypeLock)
	case byte(kvrpcpb.Op_PessimisticLock):
		data = append(data, LockTypePessimistic)
	default:
		panic("invalid lock op")
	}
	var longValue []byte
	data = codec.EncodeUvarint(codec.EncodeCompactBytes(data, lock.Primary), lock.StartTS)
	data = codec.EncodeUvarint(data, uint64(lock.TTL))
	if len(lock.Value) <= ShortValueMaxLen {
		if len(lock.Value) != 0 {
			data = append(data, byte(shortValuePrefix), byte(len(lock.Value)))
			data = append(data, lock.Value...)
		}
	} else {
		longValue = y.SafeCopy(nil, lock.Value)
	}
	if lock.ForUpdateTS > 0 {
		data = append(data, byte(forUpdatePrefix))
		data = codec.EncodeUint(data, lock.ForUpdateTS)
	}
	if lock.MinCommitTS > 0 {
		data = append(data, byte(minCommitTsPrefix))
		data = codec.EncodeUint(data, lock.MinCommitTS)
	}
	return data, longValue
}

// LockType defines a lock type.
type LockType = byte

// LockType
const (
	LockTypePut         LockType = 'P'
	LockTypeDelete      LockType = 'D'
	LockTypeLock        LockType = 'L'
	LockTypePessimistic LockType = 'S'
)

var errInvalidLockCFValue = errors.New("invalid lock CF value")
