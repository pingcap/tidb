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

package collate

import (
	"golang.org/x/text/encoding"
)

// Exported Collator types for test package

// ExportedBinCollator is an exported alias for binCollator
type ExportedBinCollator = binCollator

// ExportedBinPaddingCollator is an exported alias for binPaddingCollator
type ExportedBinPaddingCollator = binPaddingCollator

// ExportedGeneralCICollator is an exported alias for generalCICollator
type ExportedGeneralCICollator = generalCICollator

// ExportedUnicodeCICollator is an exported alias for unicodeCICollator
type ExportedUnicodeCICollator = unicodeCICollator

// ExportedUnicode0900AICICollator is an exported alias for unicode0900AICICollator
type ExportedUnicode0900AICICollator = unicode0900AICICollator

// ExportedZhPinyinTiDBASCSCollator is an exported alias for zhPinyinTiDBASCSCollator
type ExportedZhPinyinTiDBASCSCollator = zhPinyinTiDBASCSCollator

// ExportedDerivedBinCollator is an exported alias for derivedBinCollator
type ExportedDerivedBinCollator = derivedBinCollator

// ExportedGbkBinCollator is an exported alias for gbkBinCollator
type ExportedGbkBinCollator = gbkBinCollator

// ExportedGbkChineseCICollator is an exported alias for gbkChineseCICollator
type ExportedGbkChineseCICollator = gbkChineseCICollator

// ExportedGb18030BinCollator is an exported alias for gb18030BinCollator
type ExportedGb18030BinCollator = gb18030BinCollator

// NewExportedGbkBinCollator creates a new ExportedGbkBinCollator with encoder
func NewExportedGbkBinCollator(encoder *encoding.Encoder) *ExportedGbkBinCollator {
	coll := &gbkBinCollator{e: encoder}
	return coll
}

// NewExportedGb18030BinCollator creates a new ExportedGb18030BinCollator with encoder
func NewExportedGb18030BinCollator(encoder *encoding.Encoder) *ExportedGb18030BinCollator {
	coll := &gb18030BinCollator{e: encoder}
	return coll
}

// ExportedGb18030ChineseCICollator is an exported alias for gb18030ChineseCICollator
type ExportedGb18030ChineseCICollator = gb18030ChineseCICollator
