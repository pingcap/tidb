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

package ast

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
)

var (
	_ StmtNode = &AnalyzeTableStmt{}
	_ StmtNode = &DropStatsStmt{}
	_ StmtNode = &LoadStatsStmt{}
)

// AnalyzeTableStmt is used to create table statistics.
type AnalyzeTableStmt struct {
	stmtNode

	TableNames     []*TableName
	PartitionNames []model.CIStr
	IndexNames     []model.CIStr
	AnalyzeOpts    []AnalyzeOpt

	// IndexFlag is true when we only analyze indices for a table.
	IndexFlag   bool
	Incremental bool
	// HistogramOperation is set in "ANALYZE TABLE ... UPDATE/DROP HISTOGRAM ..." statement.
	HistogramOperation HistogramOperationType
	ColumnNames        []*ColumnName
}

// AnalyzeOptType is the type for analyze options.
type AnalyzeOptionType int

// Analyze option types.
const (
	AnalyzeOptNumBuckets = iota
	AnalyzeOptNumTopN
	AnalyzeOptCMSketchDepth
	AnalyzeOptCMSketchWidth
	AnalyzeOptNumSamples
)

// AnalyzeOptionString stores the string form of analyze options.
var AnalyzeOptionString = map[AnalyzeOptionType]string{
	AnalyzeOptNumBuckets:    "BUCKETS",
	AnalyzeOptNumTopN:       "TOPN",
	AnalyzeOptCMSketchWidth: "CMSKETCH WIDTH",
	AnalyzeOptCMSketchDepth: "CMSKETCH DEPTH",
	AnalyzeOptNumSamples:    "SAMPLES",
}

// HistogramOperationType is the type for histogram operation.
type HistogramOperationType int

// Histogram operation types.
const (
	// HistogramOperationNop shows no operation in histogram. Default value.
	HistogramOperationNop HistogramOperationType = iota
	HistogramOperationUpdate
	HistogramOperationDrop
)

// String implements fmt.Stringer for HistogramOperationType.
func (hot HistogramOperationType) String() string {
	switch hot {
	case HistogramOperationUpdate:
		return "UPDATE HISTOGRAM"
	case HistogramOperationDrop:
		return "DROP HISTOGRAM"
	}
	return ""
}

// AnalyzeOpt stores the analyze option type and value.
type AnalyzeOpt struct {
	Type  AnalyzeOptionType
	Value uint64
}

// Restore implements Node interface.
func (n *AnalyzeTableStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Incremental {
		ctx.WriteKeyWord("ANALYZE INCREMENTAL TABLE ")
	} else {
		ctx.WriteKeyWord("ANALYZE TABLE ")
	}
	for i, table := range n.TableNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AnalyzeTableStmt.TableNames[%d]", i)
		}
	}
	if len(n.PartitionNames) != 0 {
		ctx.WriteKeyWord(" PARTITION ")
	}
	for i, partition := range n.PartitionNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(partition.O)
	}
	if n.HistogramOperation != HistogramOperationNop {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(n.HistogramOperation.String())
		ctx.WritePlain(" ")
	}
	if len(n.ColumnNames) > 0 {
		ctx.WriteKeyWord("ON ")
		for i, columnName := range n.ColumnNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(columnName.Name.O)
		}
	}
	if n.IndexFlag {
		ctx.WriteKeyWord(" INDEX")
	}
	for i, index := range n.IndexNames {
		if i != 0 {
			ctx.WritePlain(",")
		} else {
			ctx.WritePlain(" ")
		}
		ctx.WriteName(index.O)
	}
	if len(n.AnalyzeOpts) != 0 {
		ctx.WriteKeyWord(" WITH")
		for i, opt := range n.AnalyzeOpts {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlainf(" %d ", opt.Value)
			ctx.WritePlain(AnalyzeOptionString[opt.Type])
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AnalyzeTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AnalyzeTableStmt)
	for i, val := range n.TableNames {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.TableNames[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// DropStatsStmt is used to drop table statistics.
type DropStatsStmt struct {
	stmtNode

	Table *TableName
}

// Restore implements Node interface.
func (n *DropStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP STATS ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while add table")
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropStatsStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
}

// LoadStatsStmt is the statement node for loading statistic.
type LoadStatsStmt struct {
	stmtNode

	Path string
}

// Restore implements Node interface.
func (n *LoadStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOAD STATS ")
	ctx.WriteString(n.Path)
	return nil
}

// Accept implements Node Accept interface.
func (n *LoadStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LoadStatsStmt)
	return v.Leave(n)
}
