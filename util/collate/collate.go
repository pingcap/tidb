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
)

var (
	collatorMap map[string]Collator
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the collate key for str.
	Key(str string) []byte
}

// GetCollator get the collator according to collate, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollator(collate string) Collator {
	ctor, ok := collatorMap[collate]
	if !ok {
		return collatorMap["binary"]
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
func (bc *binCollator) Key(str string) []byte {
	return []byte(str)
}

func init() {
	collatorMap = make(map[string]Collator)

	collatorMap["binary"] = &binCollator{}
}
