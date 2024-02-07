// Copyright 2022-present PingCAP, Inc.
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

package stream

import (
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// RawMetaKey specified a transaction meta key.
type RawMetaKey struct {
	Key   []byte
	Field []byte
	Ts    uint64
}

// ParseTxnMetaKeyFrom gets a `RawMetaKey` struct by parsing transaction meta key.
func ParseTxnMetaKeyFrom(txnKey kv.Key) (*RawMetaKey, error) {
	lessBuff, rawKey, err := codec.DecodeBytes(txnKey, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	key, field, err := tablecodec.DecodeMetaKey(rawKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	_, ts, err := codec.DecodeUintDesc(lessBuff)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &RawMetaKey{
		Key:   key,
		Field: field,
		Ts:    ts,
	}, nil
}

// UpdateKey updates `key` field in `RawMetaKey` struct.
func (k *RawMetaKey) UpdateKey(key []byte) {
	k.Key = key
}

// UpdateField updates `Field` field in `RawMetaKey` struct.
func (k *RawMetaKey) UpdateField(field []byte) {
	k.Field = field
}

// UpdateTS updates `Ts` field in `RawMetaKey` struct.
func (k *RawMetaKey) UpdateTS(ts uint64) {
	k.Ts = ts
}

// EncodeMetaKey Encodes RawMetaKey into a transaction key
func (k *RawMetaKey) EncodeMetaKey() kv.Key {
	rawKey := tablecodec.EncodeMetaKey(k.Key, k.Field)
	encodedKey := codec.EncodeBytes(nil, rawKey)
	return codec.EncodeUintDesc(encodedKey, k.Ts)
}

// WriteType defines a write type.
type WriteType = byte

// WriteType
const (
	WriteTypeLock     WriteType = 'L'
	WriteTypeRollback WriteType = 'R'
	WriteTypeDelete   WriteType = 'D'
	WriteTypePut      WriteType = 'P'
)

func WriteTypeFrom(t byte) (WriteType, error) {
	var (
		wt  WriteType
		err error
	)

	switch t {
	case WriteTypeDelete:
		wt = WriteTypeDelete
	case WriteTypeLock:
		wt = WriteTypeLock
	case WriteTypePut:
		wt = WriteTypePut
	case WriteTypeRollback:
		wt = WriteTypeRollback
	default:
		err = errors.Annotatef(berrors.ErrInvalidArgument, "invalid write type:%c", t)
	}
	return wt, err
}

const (
	flagShortValuePrefix   = byte('v')
	flagOverlappedRollback = byte('R')
	flagGCFencePrefix      = byte('F')
	flagLastChangePrefix   = byte('l')
	flagTxnSourcePrefix    = byte('S')
)

// RawWriteCFValue represents the value in write columnFamily.
// Detail see line: https://github.com/tikv/tikv/blob/release-6.5/components/txn_types/src/write.rs#L70
type RawWriteCFValue struct {
	t                     WriteType
	startTs               uint64
	shortValue            []byte
	hasOverlappedRollback bool

	// Records the next version after this version when overlapping rollback
	// happens on an already existed commit record.
	//
	// See [`Write::gc_fence`] for more detail.
	hasGCFence bool
	gcFence    uint64

	// The number of versions that need skipping from this record
	// to find the latest PUT/DELETE record.
	// If versions_to_last_change > 0 but last_change_ts == 0, the key does not
	// have a PUT/DELETE record before this write record.
	lastChangeTs         uint64
	versionsToLastChange uint64

	// The source of this txn.
	txnSource uint64
}

// ParseFrom decodes the value to get the struct `RawWriteCFValue`.
func (v *RawWriteCFValue) ParseFrom(data []byte) error {
	if len(data) < 9 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid input value, len:%v", len(data))
	}

	t, err := WriteTypeFrom(data[0])
	if err != nil {
		return errors.Trace(err)
	}
	v.t = t

	data, ts, err := codec.DecodeUvarint(data[1:])
	if err != nil {
		return errors.Trace(err)
	}
	v.startTs = ts

l_for:
	for len(data) > 0 {
		switch data[0] {
		case flagShortValuePrefix:
			vlen := data[1]
			if len(data[2:]) < int(vlen) {
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"the length of short value is invalid, vlen: %v", int(vlen))
			}
			v.shortValue = data[2 : vlen+2]
			data = data[vlen+2:]
		case flagOverlappedRollback:
			v.hasOverlappedRollback = true
			data = data[1:]
		case flagGCFencePrefix:
			v.hasGCFence = true
			data, v.gcFence, err = codec.DecodeUint(data[1:])
			if err != nil {
				return errors.Annotate(berrors.ErrInvalidArgument, "decode gc fence failed")
			}
		case flagLastChangePrefix:
			data, v.lastChangeTs, err = codec.DecodeUint(data[1:])
			if err != nil {
				return errors.Annotate(berrors.ErrInvalidArgument, "decode last change ts failed")
			}
			data, v.versionsToLastChange, err = codec.DecodeUvarint(data)
			if err != nil {
				return errors.Annotate(berrors.ErrInvalidArgument, "decode versions to last change failed")
			}
		case flagTxnSourcePrefix:
			data, v.txnSource, err = codec.DecodeUvarint(data[1:])
			if err != nil {
				return errors.Annotate(berrors.ErrInvalidArgument, "decode txn source failed")
			}
		default:
			break l_for
		}
	}
	return nil
}

// IsRollback checks whether the value in cf is a `rollback` record.
func (v *RawWriteCFValue) IsRollback() bool {
	return v.GetWriteType() == WriteTypeRollback
}

// IsRollback checks whether the value in cf is a `delete` record.
func (v *RawWriteCFValue) IsDelete() bool {
	return v.GetWriteType() == WriteTypeDelete
}

// HasShortValue checks whether short value is stored in write cf.
func (v *RawWriteCFValue) HasShortValue() bool {
	return len(v.shortValue) > 0
}

// UpdateShortValue gets the shortValue field.
func (v *RawWriteCFValue) GetShortValue() []byte {
	return v.shortValue
}

// UpdateShortValue updates the shortValue field.
func (v *RawWriteCFValue) UpdateShortValue(value []byte) {
	v.shortValue = value
}

func (v *RawWriteCFValue) GetStartTs() uint64 {
	return v.startTs
}

func (v *RawWriteCFValue) GetWriteType() byte {
	return v.t
}

// EncodeTo encodes the RawWriteCFValue to get encoded value.
func (v *RawWriteCFValue) EncodeTo() []byte {
	data := make([]byte, 0, 9)
	data = append(data, v.t)
	data = codec.EncodeUvarint(data, v.startTs)

	if len(v.shortValue) > 0 {
		data = append(data, flagShortValuePrefix, byte(len(v.shortValue)))
		data = append(data, v.shortValue...)
	}
	if v.hasOverlappedRollback {
		data = append(data, flagOverlappedRollback)
	}
	if v.hasGCFence {
		data = append(data, flagGCFencePrefix)
		data = codec.EncodeUint(data, v.gcFence)
	}
	if v.lastChangeTs > 0 || v.versionsToLastChange > 0 {
		data = append(data, flagLastChangePrefix)
		data = codec.EncodeUint(data, v.lastChangeTs)
		data = codec.EncodeUvarint(data, v.versionsToLastChange)
	}
	if v.txnSource > 0 {
		data = append(data, flagTxnSourcePrefix)
		data = codec.EncodeUvarint(data, v.txnSource)
	}
	return data
}
