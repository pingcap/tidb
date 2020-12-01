// Copyright 2020 PingCAP, Inc.
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

package vitess

import (
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
)

var nullKeyBlock cipher.Block

func init() {
	block, err := des.NewCipher(make([]byte, 8))
	if err != nil {
		panic(errors.Trace(err))
	}
	nullKeyBlock = block
}

// VitessHash implements vitess' method of calculating a hash used for determining a shard key range.
// Uses a DES encryption with 64 bit key, 64 bit block, null-key
func VitessHash(shardKey []byte) ([]byte, error) {
	if len(shardKey) > 8 {
		return nil, fmt.Errorf("shard key is too long: %v", hex.EncodeToString(shardKey))
	} else if len(shardKey) == 8 {
		var hashed [8]byte
		nullKeyBlock.Encrypt(hashed[:], shardKey[:])
		return hashed[:], nil
	} else {
		var keybytes, hashed [8]byte
		copy(keybytes[len(keybytes)-len(shardKey):], shardKey)
		nullKeyBlock.Encrypt(hashed[:], keybytes[:])
		return hashed[:], nil
	}
}

// VitessHashUint64 implements vitess' method of calculating a hash used for determining a shard key range.
// Uses a DES encryption with 64 bit key, 64 bit block, null-key
func VitessHashUint64(shardKey uint64) ([]byte, error) {
	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	return VitessHash(keybytes[:])
}
