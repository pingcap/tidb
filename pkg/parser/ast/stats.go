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
	"github.com/pingcap/tidb/pkg/parser/format"
)

var (
	_ StmtNode = &AnalyzeTableStmt{}
	_ StmtNode = &DropStatsStmt{}
	_ StmtNode = &LoadStatsStmt{}
	_ StmtNode = &RefreshStatsStmt{}
)

// AnalyzeTableStmt is used to create table statistics.
type AnalyzeTableStmt struct {
	stmtNode

	TableNames     []*TableName
	PartitionNames []CIStr
	IndexNames     []CIStr
	AnalyzeOpts    []AnalyzeOpt

	// IndexFlag is true when we only analyze indices for a table.
	IndexFlag       bool
	Incremental     bool
	NoWriteToBinLog bool
	// HistogramOperation is set in "ANALYZE TABLE ... UPDATE/DROP HISTOGRAM ..." statement.
	HistogramOperation HistogramOperationType
	// ColumnNames indicate the columns whose statistics need to be collected.
	ColumnNames  []CIStr
	ColumnChoice ColumnChoice
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
	AnalyzeOptSampleRate
)

// AnalyzeOptionString stores the string form of analyze options.
var AnalyzeOptionString = map[AnalyzeOptionType]string{
	AnalyzeOptNumBuckets:    "BUCKETS",
	AnalyzeOptNumTopN:       "TOPN",
	AnalyzeOptCMSketchWidth: "CMSKETCH WIDTH",
	AnalyzeOptCMSketchDepth: "CMSKETCH DEPTH",
	AnalyzeOptNumSamples:    "SAMPLES",
	AnalyzeOptSampleRate:    "SAMPLERATE",
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
	Value ValueExpr
}

// Restore implements Node interface.
func (n *AnalyzeTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ANALYZE ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	if n.Incremental {
		ctx.WriteKeyWord("INCREMENTAL TABLE ")
	} else {
		ctx.WriteKeyWord("TABLE ")
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
		if len(n.ColumnNames) > 0 {
			ctx.WriteKeyWord("ON ")
			for i, columnName := range n.ColumnNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(columnName.O)
			}
		}
	}
	switch n.ColumnChoice {
	case AllColumns:
		ctx.WriteKeyWord(" ALL COLUMNS")
	case PredicateColumns:
		ctx.WriteKeyWord(" PREDICATE COLUMNS")
	case ColumnList:
		ctx.WriteKeyWord(" COLUMNS ")
		for i, columnName := range n.ColumnNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(columnName.O)
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
			ctx.WritePlainf(" %v ", opt.Value.GetValue())
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
// if the PartitionNames is not empty, or IsGlobalStats is true, it will contain exactly one table
type DropStatsStmt struct {
	stmtNode

	Tables         []*TableName
	PartitionNames []CIStr
	IsGlobalStats  bool
}

// Restore implements Node interface.
func (n *DropStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP STATS ")

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropStatsStmt.Tables[%d]", index)
		}
	}

	if n.IsGlobalStats {
		ctx.WriteKeyWord(" GLOBAL")
		return nil
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
	return nil
}

// Accept implements Node Accept interface.
func (n *DropStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropStatsStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
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

// LockStatsStmt is the statement node for lock table statistic
type LockStatsStmt struct {
	stmtNode

	Tables []*TableName
}

// Restore implements Node interface.
func (n *LockStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOCK STATS ")
	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore LockStatsStmt.Tables[%d]", index)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LockStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LockStatsStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// UnlockStatsStmt is the statement node for unlock table statistic
type UnlockStatsStmt struct {
	stmtNode

	Tables []*TableName
}

// Restore implements Node interface.
func (n *UnlockStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNLOCK STATS ")
	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore UnlockStatsStmt.Tables[%d]", index)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *UnlockStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UnlockStatsStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// RefreshStatsStmt is the statement node for refreshing statistics.
// It is used to refresh the statistics of a table, database, or all databases.
// For example:
// REFRESH STATS table1, db1.*
// REFRESH STATS *.*
type RefreshStatsStmt struct {
	stmtNode

	RefreshObjects []*RefreshObject
}

func (n *RefreshStatsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REFRESH STATS ")
	for index, refreshObject := range n.RefreshObjects {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := refreshObject.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RefreshStatsStmt.RefreshObjects[%d]", index)
		}
	}
	return nil
}

func (n *RefreshStatsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RefreshStatsStmt)
	return v.Leave(n)
}

type RefreshObjectScopeType int

const (
	// RefreshObjectScopeTable is the scope of a table.
	RefreshObjectScopeTable RefreshObjectScopeType = iota + 1
	// RefreshObjectScopeDatabase is the scope of a database.
	RefreshObjectScopeDatabase
	// RefreshObjectScopeGlobal is the scope of all databases.
	RefreshObjectScopeGlobal
)

type RefreshObject struct {
	RefreshObjectScope RefreshObjectScopeType
	DBName             CIStr
	TableName          CIStr
}

func (o *RefreshObject) Restore(ctx *format.RestoreCtx) error {
	switch o.RefreshObjectScope {
	case RefreshObjectScopeTable:
		if o.DBName.O != "" {
			ctx.WriteName(o.DBName.O)
			ctx.WritePlain(".")
		}
		ctx.WriteName(o.TableName.O)
	case RefreshObjectScopeDatabase:
		ctx.WriteName(o.DBName.O)
		ctx.WritePlain(".*")
	case RefreshObjectScopeGlobal:
		ctx.WritePlain("*.*")
	default:
		// This should never happen.
		return errors.Errorf("invalid refresh object scope: %d", o.RefreshObjectScope)
	}
	return nil
}
