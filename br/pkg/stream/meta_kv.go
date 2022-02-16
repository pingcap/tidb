// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package stream

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

type RawMetaKey struct {
	Key   []byte
	Field []byte
	Ts    uint64
}

func (k *RawMetaKey) ParseRawMetaKey(data kv.Key) error {
	var err error
	k.Key, k.Field, err = tablecodec.DecodeMetaKey(data[:len(data)-8])
	if err != nil {
		return errors.Trace(err)
	}

	_, k.Ts, err = codec.DecodeUint(data[len(data)-8:])
	return errors.Trace(err)
}

func (k *RawMetaKey) UpdateKey(key []byte) {
	k.Key = key
}

func (k *RawMetaKey) UpdateField(field []byte) {
	k.Field = field
}

func (k *RawMetaKey) EncodeMetaKey() kv.Key {
	output := tablecodec.EncodeMetaKey(k.Key, k.Field)
	return codec.EncodeUint(output, k.Ts)
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

var (
	flagShortValuePrefix = byte('v')
)

type RawWriteCFValue struct {
	t           WriteType
	startTs     uint64
	short_value []byte
}

func (v *RawWriteCFValue) ParseWriteCFValue(data []byte) error {
	v.t = data[0]
	data, ts, err := codec.DecodeUvarint(data[1:])
	if err != nil {
		return errors.Trace(err)
	}
	v.startTs = ts

	if len(data) > 0 {
		t := data[0]
		if t == flagShortValuePrefix {
			len := uint8(data[1])
			v.short_value = data[2 : len+2]
		}
	}
	return nil
}

func (v *RawWriteCFValue) HasShortValue() bool {
	return len(v.short_value) > 0
}

func (v *RawWriteCFValue) GetShortValue() []byte {
	return v.short_value
}

func (v *RawWriteCFValue) UpdateShortValue(value []byte) {
	v.short_value = value
}

func (v *RawWriteCFValue) EncodeWriteCFValue() []byte {
	data := make([]byte, 0)
	data = append(data, v.t)
	data = codec.EncodeUvarint(data, v.startTs)

	if len(v.short_value) > 0 {
		data = append(data, flagShortValuePrefix, byte(len(v.short_value)))
		data = append(data, v.short_value...)
	}
	return data
}
