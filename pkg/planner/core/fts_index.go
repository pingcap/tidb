// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

// publicFTSIndexOnColumns finds a public FULLTEXT index whose ordered columns
// exactly match the MATCH column list. Separate single-column indexes are not
// an equivalent substitute for one multi-column MATCH expression.
func publicFTSIndexOnColumns(tblInfo *model.TableInfo, columnNames []pmodel.CIStr, nativeParserOnly bool) *model.IndexInfo {
	if tblInfo == nil || len(columnNames) == 0 {
		return nil
	}
	for _, index := range tblInfo.Indices {
		if index.FullTextInfo == nil || !index.IsPublic() || len(index.Columns) != len(columnNames) {
			continue
		}
		if nativeParserOnly && !isNativeFTSParser(index.FullTextInfo.ParserType) {
			continue
		}
		matched := true
		for i, column := range index.Columns {
			if column.Name.L != columnNames[i].L {
				matched = false
				break
			}
		}
		if matched {
			return index
		}
	}
	return nil
}

func isNativeFTSParser(parserType model.FullTextParserType) bool {
	return parserType == model.FullTextParserTypeStandardV1 || parserType == model.FullTextParserTypeNgramV1
}
