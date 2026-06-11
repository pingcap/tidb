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

package mviewutil

import (
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// HasVisibleIndexWithPrefixCoveringColumns reports whether the base table has
// a public visible key layout usable by MIN/MAX materialized-view refresh:
// either PK-is-handle on the single group key, or an index whose leading
// columns cover all group-by columns without prefix length.
func HasVisibleIndexWithPrefixCoveringColumns(baseTableInfo *model.TableInfo, groupByCols []string) bool {
	return HasIndexWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", true)
}

// HasIndexWithPrefixCoveringColumns reports whether the table has a key layout
// whose leading columns cover all group-by columns without prefix length.
// excludedIndexName is used by DDL checks to evaluate a post-DDL table shape
// where that index should not be considered.
func HasIndexWithPrefixCoveringColumns(
	baseTableInfo *model.TableInfo,
	groupByCols []string,
	excludedIndexName string,
	requireVisiblePublic bool,
) bool {
	if baseTableInfo == nil {
		return false
	}
	prefixLen := len(groupByCols)
	if prefixLen == 0 {
		return false
	}
	groupBySet := make(map[string]struct{}, prefixLen)
	for _, col := range groupByCols {
		groupBySet[strings.ToLower(col)] = struct{}{}
	}

	excludedIndexName = strings.ToLower(excludedIndexName)
	if baseTableInfo.PKIsHandle && prefixLen == 1 && excludedIndexName != strings.ToLower(mysql.PrimaryKeyName) {
		if pkCol := baseTableInfo.GetPkColInfo(); pkCol != nil {
			if _, ok := groupBySet[pkCol.Name.L]; ok {
				return true
			}
		}
	}

	for _, idx := range baseTableInfo.Indices {
		if idx == nil || len(idx.Columns) < prefixLen {
			continue
		}
		if requireVisiblePublic && (idx.State != model.StatePublic || idx.Invisible) {
			continue
		}
		if excludedIndexName != "" && idx.Name.L == excludedIndexName {
			continue
		}
		matched := make(map[string]struct{}, prefixLen)
		ok := true
		for i := 0; i < prefixLen; i++ {
			idxCol := idx.Columns[i]
			if idxCol.Length > 0 {
				ok = false
				break
			}
			name := idxCol.Name.L
			if _, exists := groupBySet[name]; !exists {
				ok = false
				break
			}
			if _, exists := matched[name]; exists {
				ok = false
				break
			}
			matched[name] = struct{}{}
		}
		if ok && len(matched) == prefixLen {
			return true
		}
	}
	return false
}
