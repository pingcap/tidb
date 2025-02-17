// Copyright 2021 PingCAP, Inc.
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

package vitess

import (
	"crypto/cipher"
	"crypto/des" // #nosec G502
	"encoding/binary"

	"github.com/pingcap/errors"
)

var nullKeyBlock cipher.Block

func init() {
	var err error
	nullKeyBlock, err = des.NewCipher(make([]byte, 8)) // #nosec G401 G502
	if err != nil {
		panic(errors.Trace(err))
	}
}

// HashUint64 implements vitess' method of calculating a hash used for determining a shard key range.
// Uses a DES encryption with 64 bit key, 64 bit block, null-key
func HashUint64(shardKey uint64) (uint64, error) {
	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	var hashed [8]byte
	nullKeyBlock.Encrypt(hashed[:], keybytes[:])
	return binary.BigEndian.Uint64(hashed[:]), nil
}
