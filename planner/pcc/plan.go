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

package pcc

import (
	"bytes"
	"strings"
)

// PlanVer indicates version of Plan
type PlanVer string

const (
	// V6 indicates tidb v6.0.0 and higher
	V6 = "v6"
	// VUnknown indicates unknown version
	VUnknown = "unknown"
)

// OpType indicates type of op
type OpType int

const (
	// OpTypeUnknown indicates unknown
	OpTypeUnknown OpType = iota
	// OpTypeHashJoin indicates OpTypeHashJoin
	OpTypeHashJoin
	// OpTypeIndexJoin indicates OpTypeIndexJoin
	OpTypeIndexJoin
	// OpTypeMergeJoin indicates OpTypeMergeJoin
	OpTypeMergeJoin
	// OpTypeHashAgg indicates OpTypeHashAgg
	OpTypeHashAgg
	// OpTypeStreamAgg indicates OpTypeStreamAgg
	OpTypeStreamAgg
	// OpTypeSelection indicates OpTypeSelection
	OpTypeSelection
	// OpTypeProjection indicates OpTypeProjection
	OpTypeProjection
	// OpTypeTableReader indicates OpTypeTableReader
	OpTypeTableReader
	// OpTypeTableScan indicates OpTypeTableScan
	OpTypeTableScan
	// OpTypeIndexReader indicates OpTypeIndexReader
	OpTypeIndexReader
	// OpTypeIndexScan indicates OpTypeIndexScan
	OpTypeIndexScan
	// OpTypeIndexLookup indicates OpTypeIndexLookup
	OpTypeIndexLookup
	// OpTypePointGet indicates OpTypePointGet
	OpTypePointGet
	// OpTypeMaxOneRow indicates OpTypeMaxOneRow
	OpTypeMaxOneRow
	// OpTypeApply indicates OpTypeApply
	OpTypeApply
	// OpTypeLimit indicates OpTypeLimit
	OpTypeLimit
	// OpTypeSort indicates OpTypeSort
	OpTypeSort
	// OpTypeTopN indicates OpTypeTopN
	OpTypeTopN
	// OpTypeTableDual indicates OpTypeTableDual
	OpTypeTableDual
	// OpTypeSelectLock indicates OpTypeSelectLock
	OpTypeSelectLock
)

// OpTypeIsDataSource indicates is DataSource
func OpTypeIsDataSource(opType OpType) bool {
	switch opType {
	case OpTypeTableReader, OpTypeIndexReader, OpTypeIndexLookup, OpTypePointGet:
		return true
	}
	return false
}

// OpTypeIsJoin indicates is Join
func OpTypeIsJoin(opType OpType) bool {
	switch opType {
	case OpTypeIndexJoin, OpTypeMergeJoin, OpTypeHashJoin:
		return true
	}
	return false
}

// JoinType indicates join type
type JoinType int

const (
	// JoinTypeUnknown indicates JoinTypeUnknown
	JoinTypeUnknown JoinType = iota
	// JoinTypeInner indicates JoinTypeInner
	JoinTypeInner
	// JoinTypeLeftOuter indicates JoinTypeLeftOuter
	JoinTypeLeftOuter
	// JoinTypeRightOuter indicates JoinTypeRightOuter
	JoinTypeRightOuter
	// JoinTypeSemi indicates JoinTypeSemi
	JoinTypeSemi
	// JoinTypeAntiSemi indicates JoinTypeAntiSemi
	JoinTypeAntiSemi
	// JoinTypeLeftOuterSemi indicates JoinTypeLeftOuterSemi
	JoinTypeLeftOuterSemi
	// JoinTypeAntiLeftOuterSemi indicates JoinTypeAntiLeftOuterSemi
	JoinTypeAntiLeftOuterSemi
)

// TaskType indicates task type
type TaskType int

const (
	//  TaskTypeRoot indicates root task type
	TaskTypeRoot TaskType = iota
	// TaskTypeTiKV indicates tikv task type
	TaskTypeTiKV
	// TaskTypeTiFlash indicates tiflash task type
	TaskTypeTiFlash
)

// Plan indicates plan
type Plan struct {
	SQL  string
	Ver  PlanVer
	Root Operator
}

// Format returns string format
func (p Plan) Format() string {
	return p.Root.Format(0)
}

// Operator indicates operator
type Operator interface {
	ID() string
	Type() OpType
	EstRow() float64
	Task() TaskType

	Format(indent int) string
	Children() []Operator
	SetChild(i int, child Operator)
}

// BaseOp indicates BaseOp
type BaseOp struct {
	id     string
	opType OpType
	estRow float64
	task   TaskType

	children []Operator
}

// ID implements Operator
func (op BaseOp) ID() string {
	return op.id
}

// Type implements Operator
func (op BaseOp) Type() OpType {
	return op.opType
}

// EstRow implements Operator
func (op BaseOp) EstRow() float64 {
	return op.estRow
}

// Task implements Operator
func (op BaseOp) Task() TaskType {
	return op.task
}

// Format implements Operator
func (op BaseOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\n")
	for _, child := range op.children {
		buf.WriteString(child.Format(indent + 4))
	}
	return buf.String()
}

// Children implements Operator
func (op BaseOp) Children() []Operator {
	return op.children
}

// SetChild implements Operator
func (op BaseOp) SetChild(i int, child Operator) {
	op.children[i] = child
}

// HashJoinOp is Hash Join OP
type HashJoinOp struct {
	BaseOp
	JoinType JoinType
}

// Format indicates Format
func (op HashJoinOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "build: " + op.children[0].ID() + "\n")
	for _, child := range op.children {
		buf.WriteString(child.Format(indent + 4))
	}
	return buf.String()
}

// IndexJoinOp is IndexJoinOp
type IndexJoinOp struct {
	BaseOp
	JoinType JoinType
}

// MergeJoinOp is MergeJoinOp
type MergeJoinOp struct {
	BaseOp
	JoinType JoinType
}

// TableReaderOp is TableReaderOp
type TableReaderOp struct {
	BaseOp
}

// TableScanOp is TableScanOp
type TableScanOp struct {
	BaseOp
	Table string
}

// Format indicates Format
func (op TableScanOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\ttable:" + op.Table + "\n")
	return buf.String()
}

// IndexReaderOp is IndexReaderOp
type IndexReaderOp struct {
	BaseOp
}

// IndexScanOp is IndexScanOp
type IndexScanOp struct {
	BaseOp
	Table string
	Index string
}

// Format indicates Format
func (op IndexScanOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\ttable:" + op.Table + ", index:" + op.Index + "\n")
	return buf.String()
}

// IndexLookupOp is IndexLookupOp
type IndexLookupOp struct {
	BaseOp
}

// SelectionOp is SelectionOp
type SelectionOp struct {
	BaseOp
}

// ProjectionOp is ProjectionOp
type ProjectionOp struct {
	BaseOp
}

// PointGetOp is PointGetOp
type PointGetOp struct {
	BaseOp
	Batch bool
	Table string
}

// StreamAggOp is StreamAggOp
type StreamAggOp struct {
	BaseOp
}

// HashAggOp is HashAggOp
type HashAggOp struct {
	BaseOp
}

// MaxOneRowOp is MaxOneRowOp
type MaxOneRowOp struct {
	BaseOp
}

// ApplyOp is ApplyOp
type ApplyOp struct {
	BaseOp
}

// LimitOp is LimitOp
type LimitOp struct {
	BaseOp
}

// SortOp is SortOp
type SortOp struct {
	BaseOp
}

// TopNOp indicates TopNOp
type TopNOp struct {
	BaseOp
}

// TableDual is TableDual
type TableDual struct {
	BaseOp
}

// SelectLock is SelectLock
type SelectLock struct {
	BaseOp
}
