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

package collate

import (
	"strings"

	"github.com/pingcap/tidb/util/codec"
)

var (
	CollatorMap map[string]Collator
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the collate key for str, the returned slice will point to an allocation in buf if it's passed in.
	Key(buf []byte, str []byte) []byte
}

// GetCollator get the collator according to collate, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollator(collate string) Collator {
	ctor, ok := CollatorMap[collate]
	if !ok {
		return CollatorMap["binary"]
	}
	return ctor
}

type binCollator struct {
}

// Compare implement Collator interface.
func (bc *binCollator) Compare(a, b string) int {
	return strings.Compare(a, b)
}

// Key implement Collator interface.
func (bc *binCollator) Key(buf []byte, str []byte) []byte {
	// TODO: Put the logic here and let codec.EncodeBytes deal with collation.
	return codec.EncodeBytes(buf, str)
}

func init() {
	CollatorMap = make(map[string]Collator)

	CollatorMap["binary"] = &binCollator{}
}
