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

// Compare is not implemented.
func (*zhPinyinTiDBASCSCollator) Compare(_, _ string) int {
	panic("implement me")
}

// Key is not implemented.
func (*zhPinyinTiDBASCSCollator) Key(_ string) []byte {
	panic("implement me")
}

// KeyWithoutTrimRightSpace is not implemented.
func (*zhPinyinTiDBASCSCollator) KeyWithoutTrimRightSpace(_ string) []byte {
	panic("implement me")
}

// Pattern is not implemented.
func (*zhPinyinTiDBASCSCollator) Pattern() WildcardPattern {
	panic("implement me")
}
