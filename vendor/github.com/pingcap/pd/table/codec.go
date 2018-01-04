// Copyright 2016 PingCAP, Inc.
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

package table

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
)

var (
	tablePrefix = []byte{'t'}
	metaPrefix  = []byte{'m'}
)

const (
	signMask uint64 = 0x8000000000000000

	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

// Key represents high-level Key type.
type Key []byte

// TableID returns the table ID of the key, if the key is not table key, returns 0.
func (k Key) TableID() int64 {
	_, key, err := decodeBytes(k)
	if err != nil {
		// should never happen
		return 0
	}
	if !bytes.HasPrefix(key, tablePrefix) {
		return 0
	}
	key = key[len(tablePrefix):]

	_, tableID, _ := DecodeInt(key)
	return tableID
}

// IsMeta returns if the key is a meta key.
func (k Key) IsMeta() bool {
	_, key, err := decodeBytes(k)
	if err != nil {
		return false
	}
	return bytes.HasPrefix(key, metaPrefix)
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := decodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

func decodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

func decodeBytes(b []byte) ([]byte, []byte, error) {
	data := make([]byte, 0, len(b))
	for {
		if len(b) < encGroupSize+1 {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		padCount := encMarker - marker
		if padCount > encGroupSize {
			return nil, nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		data = append(data, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			var padByte = encPad
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return b, data, nil
}
