// Copyright 2025 PingCAP, Inc.
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

package access

import (
	"strings"

	"github.com/pingcap/tipb/go-tipb"
)

// ScanAccessObject represents the access to a table.
// It may also represent the access to indexes and partitions of a table.
type ScanAccessObject struct {
	Database   string
	Table      string
	Indexes    []IndexAccess
	Partitions []string
}

// NormalizedString implements AccessObject.
func (s *ScanAccessObject) NormalizedString() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partitions) > 0 {
		b.WriteString(", partition:?")
	}
	for _, index := range s.Indexes {
		if index.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(index.Name + "(" + strings.Join(index.Cols, ", ") + ")")
	}
	return b.String()
}

func (s *ScanAccessObject) String() string {
	var b strings.Builder
	if len(s.Table) > 0 {
		b.WriteString("table:" + s.Table)
	}
	if len(s.Partitions) > 0 {
		b.WriteString(", partition:" + strings.Join(s.Partitions, ","))
	}
	for _, index := range s.Indexes {
		if index.IsClusteredIndex {
			b.WriteString(", clustered index:")
		} else {
			b.WriteString(", index:")
		}
		b.WriteString(index.Name + "(" + strings.Join(index.Cols, ", ") + ")")
	}
	return b.String()
}

// SetIntoPB implements AccessObject.
func (s *ScanAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if s == nil || pb == nil {
		return
	}
	pbObj := tipb.ScanAccessObject{
		Database:   s.Database,
		Table:      s.Table,
		Partitions: s.Partitions,
	}
	for i := range s.Indexes {
		pbObj.Indexes = append(pbObj.Indexes, s.Indexes[i].ToPB())
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_ScanObject{ScanObject: &pbObj},
		},
	}
}

// IndexAccess represents the index accessed by an operator.
type IndexAccess struct {
	Name             string
	Cols             []string
	IsClusteredIndex bool
}

// ToPB turns itself into a protobuf message.
func (a *IndexAccess) ToPB() *tipb.IndexAccess {
	if a == nil {
		return nil
	}
	return &tipb.IndexAccess{
		Name:             a.Name,
		Cols:             a.Cols,
		IsClusteredIndex: a.IsClusteredIndex,
	}
}
