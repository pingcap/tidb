// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package stream

import "encoding/binary"

type Iterator interface {
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
}

type EventIterator struct {
	buff []byte
	pos  uint32
	k    []byte
	v    []byte
}

func NewEventIterator(buff []byte) Iterator {
	return &EventIterator{buff: buff}
}

func (ei *EventIterator) Next() {
	if !ei.Valid() {
		return
	}

	kLen := binary.LittleEndian.Uint32(ei.buff[ei.pos:])
	ei.pos += 4
	ei.k = ei.buff[ei.pos : ei.pos+kLen]
	ei.pos += kLen

	vLen := binary.LittleEndian.Uint32(ei.buff[ei.pos:])
	ei.pos += 4
	ei.v = ei.buff[ei.pos : ei.pos+vLen]
	ei.pos += vLen
}

func (ei *EventIterator) Valid() bool {
	return ei.pos < uint32(len(ei.buff))
}

func (ei *EventIterator) Key() []byte {
	return ei.k
}

func (ei *EventIterator) Value() []byte {
	return ei.v
}
