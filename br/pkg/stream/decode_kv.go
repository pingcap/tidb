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
	"encoding/binary"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

// Iterator specifies a read iterator Interface.
type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	GetError() error
}

// EventIterator is used for reading kv-event from buffer by iterator.
type EventIterator struct {
	buff []byte
	pos  uint32
	k    []byte
	v    []byte
	err  error
}

// NewEventIterator creates a Iterator to read kv-event.
func NewEventIterator(buff []byte) Iterator {
	return &EventIterator{buff: buff}
}

// Next specifies the next iterative element.
func (ei *EventIterator) Next() {
	if !ei.Valid() {
		return
	}

	k, v, pos, err := DecodeKVEntry(ei.buff[ei.pos:])
	if err != nil {
		ei.err = err
		return
	}

	ei.k = k
	ei.v = v
	ei.pos += pos
}

// Valid checks whether the iterator is valid.
func (ei *EventIterator) Valid() bool {
	return ei.err == nil && ei.pos < uint32(len(ei.buff))
}

// Key gets the key in kv-event if valid() == true
func (ei *EventIterator) Key() []byte {
	return ei.k
}

// Value gets the value in kv-event if valid() == true
func (ei *EventIterator) Value() []byte {
	return ei.v
}

// GetError gets the error in iterator.
// it returns nil if it has nothing mistaken happened
func (ei *EventIterator) GetError() error {
	return ei.err
}

// EncodeKVEntry gets a entry-event from input: k, v.
func EncodeKVEntry(k, v []byte) []byte {
	entry := make([]byte, 0, 4+len(k)+4+len(v))
	length := make([]byte, 4)

	binary.LittleEndian.PutUint32(length, uint32(len(k)))
	entry = append(entry, length...)
	entry = append(entry, k...)

	binary.LittleEndian.PutUint32(length, uint32(len(v)))
	entry = append(entry, length...)
	entry = append(entry, v...)
	return entry
}

// DecodeKVEntry decodes kv-entry from buff.
// If error returned is nil, it can get key/value and the length occupied in buff.
func DecodeKVEntry(buff []byte) (k, v []byte, length uint32, err error) {
	if len(buff) < 8 {
		return nil, nil, 0, errors.Annotate(berrors.ErrInvalidArgument, "invalid buff")
	}

	var pos uint32 = 0
	kLen := binary.LittleEndian.Uint32(buff)
	pos += 4
	if uint32(len(buff)) < 8+kLen {
		return nil, nil, 0, errors.Annotate(berrors.ErrInvalidArgument, "invalid buff")
	}

	k = buff[pos : pos+kLen]
	pos += kLen
	vLen := binary.LittleEndian.Uint32(buff[pos:])
	pos += 4

	if uint32(len(buff)) < 8+kLen+vLen {
		return nil, nil, 0, errors.Annotate(berrors.ErrInvalidArgument, "invalid buff")
	}

	v = buff[pos : pos+vLen]
	pos += vLen
	return k, v, pos, nil
}
