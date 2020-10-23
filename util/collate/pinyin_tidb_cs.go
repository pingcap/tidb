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

type zhPinyinTiDBASCS struct {
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Compare(a, b string) int {
	panic("implement me")
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Key(str string) []byte {
	panic("implement me")
}

// Collator interface, no implements now.
func (g zhPinyinTiDBASCS) Pattern() WildcardPattern {
	panic("implement me")
}
