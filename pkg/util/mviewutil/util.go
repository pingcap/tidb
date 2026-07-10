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
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// FormatMViewRefreshInfoEndTime formats LAST_SUCCESS_ENDTIME for mysql.tidb_mview_refresh_info.
func FormatMViewRefreshInfoEndTime(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format(types.TimeFSPFormat)
}

// FindVisibleIndexWithPrefixCoveringColumns returns the first public visible key layout
// usable by MIN/MAX materialized-view refresh.
func FindVisibleIndexWithPrefixCoveringColumns(baseTableInfo *model.TableInfo, groupByCols []string) (string, bool) {
	indexNames := findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", true)
	if len(indexNames) == 0 {
		return "", false
	}
	return indexNames[0], true
}

// FindVisibleIndexesWithPrefixCoveringColumns returns all public visible key layouts
// usable by MIN/MAX materialized-view refresh.
func FindVisibleIndexesWithPrefixCoveringColumns(baseTableInfo *model.TableInfo, groupByCols []string) []string {
	return findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", true)
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
	return len(findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, excludedIndexName, requireVisiblePublic)) > 0
}

func findIndexesWithPrefixCoveringColumns(
	baseTableInfo *model.TableInfo,
	groupByCols []string,
	excludedIndexName string,
	requireVisiblePublic bool,
) []string {
	if baseTableInfo == nil {
		return nil
	}
	prefixLen := len(groupByCols)
	if prefixLen == 0 {
		return nil
	}
	groupBySet := make(map[string]struct{}, prefixLen)
	for _, col := range groupByCols {
		groupBySet[strings.ToLower(col)] = struct{}{}
	}

	indexNames := make([]string, 0, len(baseTableInfo.Indices))
	excludedIndexName = strings.ToLower(excludedIndexName)
	if baseTableInfo.PKIsHandle && prefixLen == 1 && excludedIndexName != strings.ToLower(mysql.PrimaryKeyName) {
		if pkCol := baseTableInfo.GetPkColInfo(); pkCol != nil {
			if _, ok := groupBySet[pkCol.Name.L]; ok {
				indexNames = append(indexNames, mysql.PrimaryKeyName)
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
		if indexPrefixCoversColumns(idx, prefixLen, groupBySet) {
			indexNames = append(indexNames, idx.Name.O)
		}
	}
	return indexNames
}

func indexPrefixCoversColumns(idx *model.IndexInfo, prefixLen int, groupBySet map[string]struct{}) bool {
	matched := make(map[string]struct{}, prefixLen)
	for i := range prefixLen {
		idxCol := idx.Columns[i]
		if idxCol.Length > 0 {
			return false
		}
		name := idxCol.Name.L
		if _, exists := groupBySet[name]; !exists {
			return false
		}
		if _, exists := matched[name]; exists {
			return false
		}
		matched[name] = struct{}{}
	}
	return len(matched) == prefixLen
}
