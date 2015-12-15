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

package localstore

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

// ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
var ErrInvalidEncodedKey = errors.New("invalid encoded key")

func isTombstone(v []byte) bool {
	return len(v) == 0
}

// MvccEncodeVersionKey returns the encoded key.
func MvccEncodeVersionKey(key kv.Key, ver kv.Version) kv.EncodedKey {
	var b bytes.Buffer
	codec.AscEncoder.WriteBytes(&b, key)
	codec.DescEncoder.WriteUint(&b, ver.Ver)
	return b.Bytes()
}

// MvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func MvccDecode(encodedKey kv.EncodedKey) (kv.Key, kv.Version, error) {
	b := bytes.NewBuffer(encodedKey)
	// read key
	key, err := codec.AscEncoder.ReadBytes(b)
	if err != nil {
		// should never happen
		return nil, kv.Version{}, errors.Trace(err)
	}
	// if it's meta key
	if b.Len() == 0 {
		return key, kv.Version{}, nil
	}
	ver, err := codec.DescEncoder.ReadUint(b)
	if err != nil {
		// should never happen
		return nil, kv.Version{}, errors.Trace(err)
	}
	if b.Len() != 0 {
		return nil, kv.Version{}, ErrInvalidEncodedKey
	}
	return key, kv.Version{Ver: ver}, nil
}
