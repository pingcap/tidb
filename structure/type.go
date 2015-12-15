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
	"github.com/pingcap/tidb/util/codec"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag byte

const (
	// StringMeta is the flag for string meta.
	StringMeta TypeFlag = 'S'
	// StringData is the flag for string data.
	StringData TypeFlag = 's'
	// HashMeta is the flag for hash meta.
	HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func (t *TxStructure) encodeStringDataKey(key []byte) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(StringData))
	return b.Bytes()
}

func (t *TxStructure) encodeHashMetaKey(key []byte) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(HashMeta))
	return b.Bytes()
}

func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(HashData))
	codec.AscEncoder.WriteBytes(&b, field)
	return b.Bytes()
}

func (t *TxStructure) decodeHashDataKey(ek []byte) ([]byte, []byte, error) {
	b := bytes.NewBuffer(ek)

	prefix := b.Next(len(t.prefix))
	if !bytes.Equal(prefix, t.prefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}

	key, err := codec.AscEncoder.ReadBytes(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	tp, err := codec.AscEncoder.ReadSingleByte(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if TypeFlag(tp) != HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}

	field, err := codec.AscEncoder.ReadBytes(b)
	return key, field, errors.Trace(err)
}

func (t *TxStructure) hashDataKeyPrefix(key []byte) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(HashData))
	return b.Bytes()

}

func (t *TxStructure) encodeListMetaKey(key []byte) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(ListMeta))
	return b.Bytes()
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) []byte {
	var b bytes.Buffer
	b.Write(t.prefix)
	codec.AscEncoder.WriteBytes(&b, key)
	codec.AscEncoder.WriteSingleByte(&b, byte(ListData))
	codec.AscEncoder.WriteInt(&b, index)
	return b.Bytes()

}
