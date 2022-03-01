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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
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
)

type RawWriteCFValue struct {
	t                     WriteType
	startTs               uint64
	shortValue            []byte
	hasOverlappedRollback bool
	hasGCFence            bool
	gcFence               uint64
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
		default:
			break l_for
		}
	}
	return nil
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
	return data
}
