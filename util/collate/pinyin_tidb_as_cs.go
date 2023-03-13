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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

// Collation of utf8mb4_zh_pinyin_tidb_as_cs
type zhPinyinTiDBASCSCollator struct {
}

// Collator interface, no implements now.
func (py *zhPinyinTiDBASCSCollator) Compare(a, b string) int {
	panic("implement me")
}

// Collator interface, no implements now.
func (py *zhPinyinTiDBASCSCollator) Key(str string) []byte {
	panic("implement me")
}

// Collator interface, no implements now.
func (py *zhPinyinTiDBASCSCollator) KeyWithoutTrimRightSpace(str string) []byte {
	panic("implement me")
}

// Collator interface, no implements now.
func (py *zhPinyinTiDBASCSCollator) Pattern() WildcardPattern {
	panic("implement me")
}
