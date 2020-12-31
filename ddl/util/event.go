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
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"

	"github.com/pingcap/parser/model"
)

// Event is an event that a ddl operation happened.
type Event struct {
	Tp          model.ActionType
	TableInfo   *model.TableInfo
	PartInfo    *model.PartitionInfo
	ColumnInfos []*model.ColumnInfo
	IndexInfo   *model.IndexInfo
}

// String implements fmt.Stringer interface.
func (e *Event) String() string {
	ret := fmt.Sprintf("(Event Type: %s", e.Tp)
	if e.TableInfo != nil {
		ret += fmt.Sprintf(", Table ID: %d, Table Name %s", e.TableInfo.ID, e.TableInfo.Name)
	}
	if e.PartInfo != nil {
		ids := make([]int64, 0, len(e.PartInfo.Definitions))
		for _, def := range e.PartInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Partition IDs: %v", ids)
	}
	for _, columnInfo := range e.ColumnInfos {
		ret += fmt.Sprintf(", Column ID: %d, Column Name %s", columnInfo.ID, columnInfo.Name)
	}
	if e.IndexInfo != nil {
		ret += fmt.Sprintf(", Index ID: %d, Index Name %s", e.IndexInfo.ID, e.IndexInfo.Name)
	}
	return ret
}
