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

// OtherAccessObject represents other kinds of access.
type OtherAccessObject string

func (o OtherAccessObject) String() string {
	return string(o)
}

// NormalizedString implements AccessObject.
func (o OtherAccessObject) NormalizedString() string {
	return o.String()
}

// SetIntoPB implements AccessObject.
func (o OtherAccessObject) SetIntoPB(pb *tipb.ExplainOperator) {
	if pb == nil {
		return
	}
	if o == "" {
		return
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_OtherObject{OtherObject: string(o)},
		},
	}
}

// DynamicPartitionAccessObject represents the partitions accessed by the children of this operator.
// It's mainly used in dynamic pruning mode.
type DynamicPartitionAccessObject struct {
	Database      string
	Table         string
	AllPartitions bool
	Partitions    []string
	Err           string
}

func (d *DynamicPartitionAccessObject) String() string {
	if len(d.Err) > 0 {
		return d.Err
	}
	if d.AllPartitions {
		return "partition:all"
	} else if len(d.Partitions) == 0 {
		return "partition:dual"
	}
	return "partition:" + strings.Join(d.Partitions, ",")
}

// DynamicPartitionAccessObjects is a list of DynamicPartitionAccessObject.
type DynamicPartitionAccessObjects []*DynamicPartitionAccessObject

func (d DynamicPartitionAccessObjects) String() string {
	if len(d) == 0 {
		return ""
	}
	if len(d) == 1 {
		return d[0].String()
	}
	var b strings.Builder
	for i, access := range d {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(access.String())
		b.WriteString(" of " + access.Table)
	}
	return b.String()
}

// NormalizedString implements AccessObject.
func (d DynamicPartitionAccessObjects) NormalizedString() string {
	return d.String()
}

// SetIntoPB implements AccessObject.
func (d DynamicPartitionAccessObjects) SetIntoPB(pb *tipb.ExplainOperator) {
	if len(d) == 0 || pb == nil {
		return
	}
	pbObjSlice := make([]tipb.DynamicPartitionAccessObject, len(d))
	for i, obj := range d {
		if len(obj.Err) > 0 {
			continue
		}
		pbObj := &pbObjSlice[i]
		pbObj.Database = obj.Database
		pbObj.Table = obj.Table
		pbObj.AllPartitions = obj.AllPartitions
		pbObj.Partitions = obj.Partitions
	}
	pbObjs := tipb.DynamicPartitionAccessObjects{Objects: make([]*tipb.DynamicPartitionAccessObject, 0, len(d))}
	for i := range pbObjSlice {
		pbObjs.Objects = append(pbObjs.Objects, &pbObjSlice[i])
	}
	pb.AccessObjects = []*tipb.AccessObject{
		{
			AccessObject: &tipb.AccessObject_DynamicPartitionObjects{DynamicPartitionObjects: &pbObjs},
		},
	}
}
