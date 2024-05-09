// Copyright 2024 PingCAP, Inc.
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

package utils

import "github.com/pingcap/tidb/pkg/util/codec"

func TruncateTS(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

func EncodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix = append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}
