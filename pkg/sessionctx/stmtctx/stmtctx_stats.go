// Copyright 2017 PingCAP, Inc.
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

package stmtctx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// UsedStatsInfoForTable records stats that are used during query and their information.
type UsedStatsInfoForTable struct {
	Name                  string
	TblInfo               *model.TableInfo
	Version               uint64
	RealtimeCount         int64
	ModifyCount           int64
	ColumnStatsLoadStatus map[int64]string
	IndexStatsLoadStatus  map[int64]string
	ColAndIdxStatus       any
}

// FormatForExplain format the content in the format expected to be printed in the execution plan.
// case 1: if stats version is 0, print stats:pseudo.
// case 2: if stats version is not 0, and there are column/index stats that are not full loaded,
// print stats:partial, then print status of 3 column/index status at most. For the rest, only
// the count will be printed, in the format like (more: 1 onlyCmsEvicted, 2 onlyHistRemained).
func (s *UsedStatsInfoForTable) FormatForExplain() string {
	// statistics.PseudoVersion == 0
	if s.Version == 0 {
		return "stats:pseudo"
	}
	var b strings.Builder
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) == 0 {
		return ""
	}
	b.WriteString("stats:partial")
	outputNumsLeft := 3
	statusCnt := make(map[string]uint64, 1)
	var strs []string
	strs = append(strs, s.collectFromColOrIdxStatus(false, &outputNumsLeft, statusCnt)...)
	strs = append(strs, s.collectFromColOrIdxStatus(true, &outputNumsLeft, statusCnt)...)
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	if len(statusCnt) > 0 {
		b.WriteString("...(more: ")
		keys := slices.Collect(maps.Keys(statusCnt))
		slices.Sort(keys)
		var cntStrs []string
		for _, key := range keys {
			cntStrs = append(cntStrs, strconv.FormatUint(statusCnt[key], 10)+" "+key)
		}
		b.WriteString(strings.Join(cntStrs, ", "))
		b.WriteString(")")
	}
	b.WriteString("]")
	return b.String()
}

// WriteToSlowLog format the content in the format expected to be printed to the slow log, then write to w.
// The format is table name partition name:version[realtime row count;modify count][index load status][column load status].
func (s *UsedStatsInfoForTable) WriteToSlowLog(w io.Writer) {
	ver := "pseudo"
	// statistics.PseudoVersion == 0
	if s.Version != 0 {
		ver = strconv.FormatUint(s.Version, 10)
	}
	fmt.Fprintf(w, "%s:%s[%d;%d]", s.Name, ver, s.RealtimeCount, s.ModifyCount)
	if ver == "pseudo" {
		return
	}
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) > 0 {
		fmt.Fprintf(w,
			"[%s][%s]",
			strings.Join(s.collectFromColOrIdxStatus(false, nil, nil), ","),
			strings.Join(s.collectFromColOrIdxStatus(true, nil, nil), ","),
		)
	}
}

// collectFromColOrIdxStatus prints the status of column or index stats to a slice
// of the string in the format of "col/idx name:status".
// If outputNumsLeft is not nil, this function will output outputNumsLeft column/index
// status at most, the rest will be counted in statusCnt, which is a map of status->count.
func (s *UsedStatsInfoForTable) collectFromColOrIdxStatus(
	forColumn bool,
	outputNumsLeft *int,
	statusCnt map[string]uint64,
) []string {
	var status map[int64]string
	if forColumn {
		status = s.ColumnStatsLoadStatus
	} else {
		status = s.IndexStatsLoadStatus
	}
	keys := slices.Collect(maps.Keys(status))
	slices.Sort(keys)
	strs := make([]string, 0, len(status))
	for _, id := range keys {
		if outputNumsLeft == nil || *outputNumsLeft > 0 {
			var name string
			if s.TblInfo != nil {
				if forColumn {
					name = s.TblInfo.FindColumnNameByID(id)
				} else {
					name = s.TblInfo.FindIndexNameByID(id)
				}
			}
			if len(name) == 0 {
				name = "ID " + strconv.FormatInt(id, 10)
			}
			strs = append(strs, name+":"+status[id])
			if outputNumsLeft != nil {
				*outputNumsLeft--
			}
		} else if statusCnt != nil {
			statusCnt[status[id]] = statusCnt[status[id]] + 1
		}
	}
	return strs
}

func (s *UsedStatsInfoForTable) recordedColIdxCount() int {
	return len(s.IndexStatsLoadStatus) + len(s.ColumnStatsLoadStatus)
}

// UsedStatsInfo is a map for recording the used stats during query.
// The key is the table ID, and the value is the used stats info for the table.
type UsedStatsInfo struct {
	store sync.Map
}

// GetUsedInfo gets the used stats info for the table.
func (u *UsedStatsInfo) GetUsedInfo(tableID int64) *UsedStatsInfoForTable {
	v, ok := u.store.Load(tableID)
	if !ok {
		return nil
	}
	return v.(*UsedStatsInfoForTable)
}

// RecordUsedInfo records the used stats info for the table.
func (u *UsedStatsInfo) RecordUsedInfo(tableID int64, info *UsedStatsInfoForTable) {
	u.store.Store(tableID, info)
}

// Keys returns all the table IDs for the used stats info.
func (u *UsedStatsInfo) Keys() []int64 {
	var ret []int64
	if u == nil {
		return ret
	}
	u.store.Range(func(k, v any) bool {
		ret = append(ret, k.(int64))
		return true
	})
	return ret
}

// Values returns all the used stats info for the table.
func (u *UsedStatsInfo) Values() []*UsedStatsInfoForTable {
	var ret []*UsedStatsInfoForTable
	if u == nil {
		return ret
	}
	u.store.Range(func(k, v any) bool {
		ret = append(ret, v.(*UsedStatsInfoForTable))
		return true
	})
	return ret
}

// StatsLoadResult indicates result for StatsLoad
type StatsLoadResult struct {
	Item  model.TableItemID
	Error error
}

// HasError returns whether result has error
func (r StatsLoadResult) HasError() bool {
	return r.Error != nil
}

// ErrorMsg returns StatsLoadResult err msg
func (r StatsLoadResult) ErrorMsg() string {
	if r.Error == nil {
		return ""
	}
	b := bytes.NewBufferString("tableID:")
	b.WriteString(strconv.FormatInt(r.Item.TableID, 10))
	b.WriteString(", id:")
	b.WriteString(strconv.FormatInt(r.Item.ID, 10))
	b.WriteString(", isIndex:")
	b.WriteString(strconv.FormatBool(r.Item.IsIndex))
	b.WriteString(", err:")
	b.WriteString(r.Error.Error())
	return b.String()
}

type stmtLabelKeyType struct{}

var stmtLabelKey stmtLabelKeyType

// WithStmtLabel sets the label for the statement node.
func WithStmtLabel(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, stmtLabelKey, label)
}

// GetStmtLabel returns the label for the statement node.
// context with stmtLabelKey will return the label if it exists.
func GetStmtLabel(ctx context.Context, node ast.StmtNode) string {
	if val := ctx.Value(stmtLabelKey); val != nil {
		return val.(string)
	}
	return ast.GetStmtLabel(node)
}
