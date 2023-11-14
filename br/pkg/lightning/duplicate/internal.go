// Copyright 2023 PingCAP, Inc.
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

package duplicate

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/codec"
)

type internalKey struct {
	key   []byte
	keyID []byte
}

func (ikey internalKey) String() string {
	if len(ikey.keyID) == 0 {
		return fmt.Sprintf("%X", ikey.key)
	}
	return fmt.Sprintf("%X@%X", ikey.key, ikey.keyID)
}

func compareInternalKey(a, b internalKey) int {
	if cmp := bytes.Compare(a.key, b.key); cmp != 0 {
		return cmp
	}
	return bytes.Compare(a.keyID, b.keyID)
}

func encodeInternalKey(appendTo []byte, ikey internalKey) []byte {
	appendTo = codec.EncodeBytes(appendTo, ikey.key)
	return append(appendTo, ikey.keyID...)
}

func decodeInternalKey(data []byte, ikey *internalKey) error {
	leftover, key, err := codec.DecodeBytes(data, ikey.key[:0])
	if err != nil {
		return err
	}
	ikey.key = key
	ikey.keyID = append(ikey.keyID[:0], leftover...)
	return nil
}
