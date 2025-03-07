// Copyright 2015 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var (
	_ StmtNode = &AdminStmt{}
	_ StmtNode = &AlterUserStmt{}
	_ StmtNode = &AlterRangeStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &BinlogStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &CreateUserStmt{}
	_ StmtNode = &DeallocateStmt{}
	_ StmtNode = &DoStmt{}
	_ StmtNode = &ExecuteStmt{}
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &GrantStmt{}
	_ StmtNode = &PrepareStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &SetPwdStmt{}
	_ StmtNode = &SetRoleStmt{}
	_ StmtNode = &SetDefaultRoleStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &SetSessionStatesStmt{}
	_ StmtNode = &UseStmt{}
	_ StmtNode = &FlushStmt{}
	_ StmtNode = &KillStmt{}
	_ StmtNode = &CreateBindingStmt{}
	_ StmtNode = &DropBindingStmt{}
	_ StmtNode = &SetBindingStmt{}
	_ StmtNode = &ShutdownStmt{}
	_ StmtNode = &RestartStmt{}
	_ StmtNode = &RenameUserStmt{}
	_ StmtNode = &HelpStmt{}
	_ StmtNode = &PlanReplayerStmt{}
	_ StmtNode = &CompactTableStmt{}
	_ StmtNode = &SetResourceGroupStmt{}
	_ StmtNode = &TrafficStmt{}

	_ Node = &PrivElem{}
	_ Node = &VariableAssignment{}
)

// Isolation level constants.
const (
	ReadCommitted   = "READ-COMMITTED"
	ReadUncommitted = "READ-UNCOMMITTED"
	Serializable    = "SERIALIZABLE"
	RepeatableRead  = "REPEATABLE-READ"
)

// Transaction mode constants.
const (
	Optimistic  = "OPTIMISTIC"
	Pessimistic = "PESSIMISTIC"
)

// TypeOpt is used for parsing data type option from SQL.
type TypeOpt struct {
	IsUnsigned bool
	IsZerofill bool
}

// FloatOpt is used for parsing floating-point type option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html
type FloatOpt struct {
	Flen    int
	Decimal int
}

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// ByAuthString set as true, if AuthString is used for authorization. Otherwise, authorization is done by HashString.
	ByAuthString bool
	AuthString   string
	ByHashString bool
	HashString   string
	AuthPlugin   string
}

// Restore implements Node interface.
func (n *AuthOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("IDENTIFIED")
	if n.AuthPlugin != "" {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteString(n.AuthPlugin)
	}
	if n.ByAuthString {
		ctx.WriteKeyWord(" BY ")
		ctx.WriteString(n.AuthString)
	} else if n.ByHashString {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteString(n.HashString)
	}
	return nil
}

// TraceStmt is a statement to trace what sql actually does at background.
type TraceStmt struct {
	stmtNode

	Stmt   StmtNode
	Format string

	TracePlan       bool
	TracePlanTarget string
}

// Restore implements Node interface.
func (n *TraceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("TRACE ")
	if n.TracePlan {
		ctx.WriteKeyWord("PLAN ")
		if n.TracePlanTarget != "" {
			ctx.WriteKeyWord("TARGET")
			ctx.WritePlain(" = ")
			ctx.WriteString(n.TracePlanTarget)
			ctx.WritePlain(" ")
		}
	} else if n.Format != "row" {
		ctx.WriteKeyWord("FORMAT")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TraceStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TraceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TraceStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// ExplainForStmt is a statement to provite information about how is SQL statement executeing
// in connection #ConnectionID
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainForStmt struct {
	stmtNode

	Format       string
	ConnectionID uint64
}

// Restore implements Node interface.
func (n *ExplainForStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXPLAIN ")
	ctx.WriteKeyWord("FORMAT ")
	ctx.WritePlain("= ")
	ctx.WriteString(n.Format)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord("FOR ")
	ctx.WriteKeyWord("CONNECTION ")
	ctx.WritePlain(strconv.FormatUint(n.ConnectionID, 10))
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainForStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainForStmt)
	return v.Leave(n)
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt    StmtNode
	Format  string
	Analyze bool
}

// Restore implements Node interface.
func (n *ExplainStmt) Restore(ctx *format.RestoreCtx) error {
	if showStmt, ok := n.Stmt.(*ShowStmt); ok {
		ctx.WriteKeyWord("DESC ")
		if err := showStmt.Table.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Table")
		}
		if showStmt.Column != nil {
			ctx.WritePlain(" ")
			if err := showStmt.Column.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Column")
			}
		}
		return nil
	}
	ctx.WriteKeyWord("EXPLAIN ")
	if n.Analyze {
		ctx.WriteKeyWord("ANALYZE ")
	}
	if !n.Analyze || strings.ToLower(n.Format) != "row" {
		ctx.WriteKeyWord("FORMAT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ExplainStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// PlanReplayerStmt is a statement to dump or load information for recreating plans
type PlanReplayerStmt struct {
	stmtNode

	Stmt                StmtNode
	Analyze             bool
	Load                bool
	HistoricalStatsInfo *AsOfClause

	// Capture indicates 'plan replayer capture <sql_digest> <plan_digest>'
	Capture bool
	// Remove indicates `plan replayer capture remove <sql_digest> <plan_digest>
	Remove bool

	SQLDigest  string
	PlanDigest string

	// File is used to store 2 cases:
	// 1. plan replayer load 'file';
	// 2. plan replayer dump explain <analyze> 'file'
	File string

	// Fields below are currently useless.

	// Where is the where clause in select statement.
	Where ExprNode
	// OrderBy is the ordering expression list.
	OrderBy *OrderByClause
	// Limit is the limit clause.
	Limit *Limit
}

// Restore implements Node interface.
func (n *PlanReplayerStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Load {
		ctx.WriteKeyWord("PLAN REPLAYER LOAD ")
		ctx.WriteString(n.File)
		return nil
	}
	if n.Capture {
		ctx.WriteKeyWord("PLAN REPLAYER CAPTURE ")
		ctx.WriteString(n.SQLDigest)
		ctx.WriteKeyWord(" ")
		ctx.WriteString(n.PlanDigest)
		return nil
	}
	if n.Remove {
		ctx.WriteKeyWord("PLAN REPLAYER CAPTURE REMOVE ")
		ctx.WriteString(n.SQLDigest)
		ctx.WriteKeyWord(" ")
		ctx.WriteString(n.PlanDigest)
		return nil
	}

	ctx.WriteKeyWord("PLAN REPLAYER DUMP ")

	if n.HistoricalStatsInfo != nil {
		ctx.WriteKeyWord("WITH STATS ")
		if err := n.HistoricalStatsInfo.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PlanReplayerStmt.HistoricalStatsInfo")
		}
		ctx.WriteKeyWord(" ")
	}
	if n.Analyze {
		ctx.WriteKeyWord("EXPLAIN ANALYZE ")
	} else {
		ctx.WriteKeyWord("EXPLAIN ")
	}
	if n.Stmt == nil {
		if len(n.File) > 0 {
			ctx.WriteString(n.File)
			return nil
		}
		ctx.WriteKeyWord("SLOW QUERY")
		if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PlanReplayerStmt.Where")
			}
		}
		if n.OrderBy != nil {
			ctx.WriteKeyWord(" ")
			if err := n.OrderBy.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PlanReplayerStmt.OrderBy")
			}
		}
		if n.Limit != nil {
			ctx.WriteKeyWord(" ")
			if err := n.Limit.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore PlanReplayerStmt.Limit")
			}
		}
		return nil
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore PlanReplayerStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PlanReplayerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*PlanReplayerStmt)

	if n.Load {
		return v.Leave(n)
	}

	if n.HistoricalStatsInfo != nil {
		info, ok := n.HistoricalStatsInfo.Accept(v)
		if !ok {
			return n, false
		}
		n.HistoricalStatsInfo = info.(*AsOfClause)
	}

	if n.Stmt == nil {
		if n.Where != nil {
			node, ok := n.Where.Accept(v)
			if !ok {
				return n, false
			}
			n.Where = node.(ExprNode)
		}

		if n.OrderBy != nil {
			node, ok := n.OrderBy.Accept(v)
			if !ok {
				return n, false
			}
			n.OrderBy = node.(*OrderByClause)
		}

		if n.Limit != nil {
			node, ok := n.Limit.Accept(v)
			if !ok {
				return n, false
			}
			n.Limit = node.(*Limit)
		}
		return v.Leave(n)
	}

	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// TrafficOpType is traffic operation type.
type TrafficOpType int

const (
	TrafficOpCapture TrafficOpType = iota
	TrafficOpReplay
	TrafficOpShow
	TrafficOpCancel
)

// TrafficOptionType is traffic option type.
type TrafficOptionType int

const (
	// capture options
	TrafficOptionDuration TrafficOptionType = iota
	TrafficOptionEncryptionMethod
	TrafficOptionCompress
	// replay options
	TrafficOptionUsername
	TrafficOptionPassword
	TrafficOptionSpeed
	TrafficOptionReadOnly
)

var _ SensitiveStmtNode = (*TrafficStmt)(nil)

// TrafficStmt is traffic operation statement.
type TrafficStmt struct {
	stmtNode
	OpType  TrafficOpType
	Options []*TrafficOption
	Dir     string
}

// TrafficOption is traffic option.
type TrafficOption struct {
	OptionType TrafficOptionType
	FloatValue ValueExpr
	StrValue   string
	BoolValue  bool
}

// Restore implements Node interface.
func (n *TrafficStmt) Restore(ctx *format.RestoreCtx) error {
	switch n.OpType {
	case TrafficOpCapture:
		ctx.WriteKeyWord("TRAFFIC CAPTURE TO ")
		ctx.WriteString(n.Dir)
		for _, option := range n.Options {
			ctx.WritePlain(" ")
			switch option.OptionType {
			case TrafficOptionDuration:
				ctx.WriteKeyWord("DURATION ")
				ctx.WritePlain("= ")
				ctx.WriteString(option.StrValue)
			case TrafficOptionEncryptionMethod:
				ctx.WriteKeyWord("ENCRYPTION_METHOD ")
				ctx.WritePlain("= ")
				ctx.WriteString(option.StrValue)
			case TrafficOptionCompress:
				ctx.WriteKeyWord("COMPRESS ")
				ctx.WritePlain("= ")
				ctx.WritePlain(strings.ToUpper(fmt.Sprintf("%v", option.BoolValue)))
			}
		}
	case TrafficOpReplay:
		ctx.WriteKeyWord("TRAFFIC REPLAY FROM ")
		ctx.WriteString(n.Dir)
		for _, option := range n.Options {
			ctx.WritePlain(" ")
			switch option.OptionType {
			case TrafficOptionUsername:
				ctx.WriteKeyWord("USER ")
				ctx.WritePlain("= ")
				ctx.WriteString(option.StrValue)
			case TrafficOptionPassword:
				ctx.WriteKeyWord("PASSWORD ")
				ctx.WritePlain("= ")
				ctx.WriteString(option.StrValue)
			case TrafficOptionSpeed:
				ctx.WriteKeyWord("SPEED ")
				ctx.WritePlain("= ")
				ctx.WritePlainf("%v", option.FloatValue.GetValue())
			case TrafficOptionReadOnly:
				ctx.WriteKeyWord("READONLY ")
				ctx.WritePlain("= ")
				ctx.WritePlain(strings.ToUpper(fmt.Sprintf("%v", option.BoolValue)))
			}
		}
	case TrafficOpShow:
		ctx.WriteKeyWord("SHOW TRAFFIC JOBS")
	case TrafficOpCancel:
		ctx.WriteKeyWord("CANCEL TRAFFIC JOBS")
	}
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *TrafficStmt) SecureText() string {
	trafficStmt := n
	opts := n.Options
	switch n.OpType {
	case TrafficOpReplay:
		opts = make([]*TrafficOption, 0, len(n.Options))
		for _, opt := range n.Options {
			if opt.OptionType == TrafficOptionPassword {
				newOpt := *opt
				newOpt.StrValue = "xxxxxx"
				opt = &newOpt
			}
			opts = append(opts, opt)
		}
		fallthrough
	case TrafficOpCapture:
		trafficStmt = &TrafficStmt{
			OpType:  n.OpType,
			Options: opts,
			Dir:     RedactURL(n.Dir),
		}
	}
	var sb strings.Builder
	_ = trafficStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

// Accept implements Node Accept interface.
func (n *TrafficStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TrafficStmt)
	return v.Leave(n)
}

type CompactReplicaKind string

const (
	// CompactReplicaKindAll means compacting both TiKV and TiFlash replicas.
	CompactReplicaKindAll = "ALL"

	// CompactReplicaKindTiFlash means compacting TiFlash replicas.
	CompactReplicaKindTiFlash = "TIFLASH"

	// CompactReplicaKindTiKV means compacting TiKV replicas.
	CompactReplicaKindTiKV = "TIKV"
)

// CompactTableStmt is a statement to manually compact a table.
type CompactTableStmt struct {
	stmtNode

	Table          *TableName
	PartitionNames []CIStr
	ReplicaKind    CompactReplicaKind
}

// Restore implements Node interface.
func (n *CompactTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER TABLE ")
	n.Table.restoreName(ctx)

	ctx.WriteKeyWord(" COMPACT")
	if len(n.PartitionNames) != 0 {
		ctx.WriteKeyWord(" PARTITION ")
		for i, partition := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(partition.O)
		}
	}
	if n.ReplicaKind != CompactReplicaKindAll {
		ctx.WriteKeyWord(" ")
		// Note: There is only TiFlash replica available now. TiKV will be added later.
		ctx.WriteKeyWord(string(n.ReplicaKind))
		ctx.WriteKeyWord(" REPLICA")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CompactTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CompactTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	Name    string
	SQLText string
	SQLVar  *VariableExpr
}

// Restore implements Node interface.
func (n *PrepareStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PREPARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FROM ")
	if n.SQLText != "" {
		ctx.WriteString(n.SQLText)
		return nil
	}
	if n.SQLVar != nil {
		if err := n.SQLVar.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PrepareStmt.SQLVar")
		}
		return nil
	}
	return errors.New("An error occurred while restore PrepareStmt")
}

// Accept implements Node Accept interface.
func (n *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrepareStmt)
	if n.SQLVar != nil {
		node, ok := n.SQLVar.Accept(v)
		if !ok {
			return n, false
		}
		n.SQLVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *DeallocateStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DEALLOCATE PREPARE ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeallocateStmt)
	return v.Leave(n)
}

// Prepared represents a prepared statement.
type Prepared struct {
	Stmt     StmtNode
	StmtType string
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name       string
	UsingVars  []ExprNode
	BinaryArgs interface{}
	PrepStmt   interface{} // the corresponding prepared statement
	IdxInMulti int

	// FromGeneralStmt indicates whether this execute-stmt is converted from a general query.
	// e.g. select * from t where a>2 --> execute 'select * from t where a>?' using 2
	FromGeneralStmt bool
}

// Restore implements Node interface.
func (n *ExecuteStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXECUTE ")
	ctx.WriteName(n.Name)
	if len(n.UsingVars) > 0 {
		ctx.WriteKeyWord(" USING ")
		for i, val := range n.UsingVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := val.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore ExecuteStmt.UsingVars index %d", i)
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExecuteStmt)
	for i, val := range n.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
	Mode                  string
	CausalConsistencyOnly bool
	ReadOnly              bool
	// AS OF is used to read the data at a specific point of time.
	// Should only be used when ReadOnly is true.
	AsOf *AsOfClause
}

// Restore implements Node interface.
func (n *BeginStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Mode == "" {
		if n.ReadOnly {
			ctx.WriteKeyWord("START TRANSACTION READ ONLY")
			if n.AsOf != nil {
				ctx.WriteKeyWord(" ")
				return n.AsOf.Restore(ctx)
			}
		} else if n.CausalConsistencyOnly {
			ctx.WriteKeyWord("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
		} else {
			ctx.WriteKeyWord("START TRANSACTION")
		}
	} else {
		ctx.WriteKeyWord("BEGIN ")
		ctx.WriteKeyWord(n.Mode)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	if n.AsOf != nil {
		node, ok := n.AsOf.Accept(v)
		if !ok {
			return n, false
		}
		n.AsOf = node.(*AsOfClause)
	}

	n = newNode.(*BeginStmt)
	return v.Leave(n)
}

// BinlogStmt is an internal-use statement.
// We just parse and ignore it.
// See http://dev.mysql.com/doc/refman/5.7/en/binlog.html
type BinlogStmt struct {
	stmtNode
	Str string
}

// Restore implements Node interface.
func (n *BinlogStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BINLOG ")
	ctx.WriteString(n.Str)
	return nil
}

// Accept implements Node Accept interface.
func (n *BinlogStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BinlogStmt)
	return v.Leave(n)
}

// CompletionType defines completion_type used in COMMIT and ROLLBACK statements
type CompletionType int8

const (
	// CompletionTypeDefault refers to NO_CHAIN
	CompletionTypeDefault CompletionType = iota
	CompletionTypeChain
	CompletionTypeRelease
)

func (n CompletionType) Restore(ctx *format.RestoreCtx) error {
	switch n {
	case CompletionTypeDefault:
	case CompletionTypeChain:
		ctx.WriteKeyWord(" AND CHAIN")
	case CompletionTypeRelease:
		ctx.WriteKeyWord(" RELEASE")
	}
	return nil
}

// CommitStmt is a statement to commit the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
	// CompletionType overwrites system variable `completion_type` within transaction
	CompletionType CompletionType
}

// Restore implements Node interface.
func (n *CommitStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("COMMIT")
	if err := n.CompletionType.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CommitStmt.CompletionType")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CommitStmt)
	return v.Leave(n)
}

// RollbackStmt is a statement to roll back the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
	// CompletionType overwrites system variable `completion_type` within transaction
	CompletionType CompletionType
	// SavepointName is the savepoint name.
	SavepointName string
}

// Restore implements Node interface.
func (n *RollbackStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ROLLBACK")
	if n.SavepointName != "" {
		ctx.WritePlain(" TO ")
		ctx.WritePlain(n.SavepointName)
	}
	if err := n.CompletionType.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore RollbackStmt.CompletionType")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RollbackStmt)
	return v.Leave(n)
}

// UseStmt is a statement to use the DBName database as the current database.
// See https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Restore implements Node interface.
func (n *UseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("USE ")
	ctx.WriteName(n.DBName)
	return nil
}

// Accept implements Node Accept interface.
func (n *UseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UseStmt)
	return v.Leave(n)
}

const (
	// SetNames is the const for set names stmt.
	// If VariableAssignment.Name == Names, it should be set names stmt.
	SetNames = "SetNAMES"
	// SetCharset is the const for set charset stmt.
	SetCharset = "SetCharset"
	// TiDBCloudStorageURI is the const for set tidb_cloud_storage_uri stmt.
	TiDBCloudStorageURI = "tidb_cloud_storage_uri"
)

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	node
	Name     string
	Value    ExprNode
	IsGlobal bool
	IsSystem bool

	// ExtendValue is a way to store extended info.
	// VariableAssignment should be able to store information for SetCharset/SetPWD Stmt.
	// For SetCharsetStmt, Value is charset, ExtendValue is collation.
	// TODO: Use SetStmt to implement set password statement.
	ExtendValue ValueExpr
}

// Restore implements Node interface.
func (n *VariableAssignment) Restore(ctx *format.RestoreCtx) error {
	if n.IsSystem {
		ctx.WritePlain("@@")
		if n.IsGlobal {
			ctx.WriteKeyWord("GLOBAL")
		} else {
			ctx.WriteKeyWord("SESSION")
		}
		ctx.WritePlain(".")
	} else if n.Name != SetNames && n.Name != SetCharset {
		ctx.WriteKeyWord("@")
	}
	if n.Name == SetNames {
		ctx.WriteKeyWord("NAMES ")
	} else if n.Name == SetCharset {
		ctx.WriteKeyWord("CHARSET ")
	} else {
		ctx.WriteName(n.Name)
		ctx.WritePlain("=")
	}
	if n.Name == TiDBCloudStorageURI {
		// need to redact the url for safety when `show processlist;`
		ctx.WritePlain(RedactURL(n.Value.(ValueExpr).GetString()))
	} else if err := n.Value.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore VariableAssignment.Value")
	}
	if n.ExtendValue != nil {
		ctx.WriteKeyWord(" COLLATE ")
		if err := n.ExtendValue.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore VariableAssignment.ExtendValue")
		}
	}
	return nil
}

// Accept implements Node interface.
func (n *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableAssignment)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// FlushStmtType is the type for FLUSH statement.
type FlushStmtType int

// Flush statement types.
const (
	FlushNone FlushStmtType = iota
	FlushTables
	FlushPrivileges
	FlushStatus
	FlushTiDBPlugin
	FlushHosts
	FlushLogs
	FlushClientErrorsSummary
)

// LogType is the log type used in FLUSH statement.
type LogType int8

const (
	LogTypeDefault LogType = iota
	LogTypeBinary
	LogTypeEngine
	LogTypeError
	LogTypeGeneral
	LogTypeSlow
)

// FlushStmt is a statement to flush tables/privileges/optimizer costs and so on.
type FlushStmt struct {
	stmtNode

	Tp              FlushStmtType // Privileges/Tables/...
	NoWriteToBinLog bool
	LogType         LogType
	Tables          []*TableName // For FlushTableStmt, if Tables is empty, it means flush all tables.
	ReadLock        bool
	Plugins         []string
}

// Restore implements Node interface.
func (n *FlushStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FLUSH ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	switch n.Tp {
	case FlushTables:
		ctx.WriteKeyWord("TABLES")
		for i, v := range n.Tables {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FlushStmt.Tables[%d]", i)
			}
		}
		if n.ReadLock {
			ctx.WriteKeyWord(" WITH READ LOCK")
		}
	case FlushPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	case FlushStatus:
		ctx.WriteKeyWord("STATUS")
	case FlushTiDBPlugin:
		ctx.WriteKeyWord("TIDB PLUGINS")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case FlushHosts:
		ctx.WriteKeyWord("HOSTS")
	case FlushLogs:
		var logType string
		switch n.LogType {
		case LogTypeDefault:
			logType = "LOGS"
		case LogTypeBinary:
			logType = "BINARY LOGS"
		case LogTypeEngine:
			logType = "ENGINE LOGS"
		case LogTypeError:
			logType = "ERROR LOGS"
		case LogTypeGeneral:
			logType = "GENERAL LOGS"
		case LogTypeSlow:
			logType = "SLOW LOGS"
		}
		ctx.WriteKeyWord(logType)
	case FlushClientErrorsSummary:
		ctx.WriteKeyWord("CLIENT_ERRORS_SUMMARY")
	default:
		return errors.New("Unsupported type of FlushStmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FlushStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FlushStmt)
	for i, t := range n.Tables {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// KillStmt is a statement to kill a query or connection.
type KillStmt struct {
	stmtNode

	// Query indicates whether terminate a single query on this connection or the whole connection.
	// If Query is true, terminates the statement the connection is currently executing, but leaves the connection itself intact.
	// If Query is false, terminates the connection associated with the given ConnectionID, after terminating any statement the connection is executing.
	Query        bool
	ConnectionID uint64
	// TiDBExtension is used to indicate whether the user knows he is sending kill statement to the right tidb-server.
	// When the SQL grammar is "KILL TIDB [CONNECTION | QUERY] connectionID", TiDBExtension will be set.
	// It's a special grammar extension in TiDB. This extension exists because, when the connection is:
	// client -> LVS proxy -> TiDB, and type Ctrl+C in client, the following action will be executed:
	// new a connection; kill xxx;
	// kill command may send to the wrong TiDB, because the exists of LVS proxy, and kill the wrong session.
	// So, "KILL TIDB" grammar is introduced, and it REQUIRES DIRECT client -> TiDB TOPOLOGY.
	// TODO: The standard KILL grammar will be supported once we have global connectionID.
	TiDBExtension bool

	Expr ExprNode
}

// Restore implements Node interface.
func (n *KillStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("KILL")
	if n.TiDBExtension {
		ctx.WriteKeyWord(" TIDB")
	}
	if n.Query {
		ctx.WriteKeyWord(" QUERY")
	}
	if n.Expr != nil {
		ctx.WriteKeyWord(" ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		ctx.WritePlainf(" %d", n.ConnectionID)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *KillStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*KillStmt)
	return v.Leave(n)
}

// SavepointStmt is the statement of SAVEPOINT.
type SavepointStmt struct {
	stmtNode
	// Name is the savepoint name.
	Name string
}

// Restore implements Node interface.
func (n *SavepointStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SAVEPOINT ")
	ctx.WritePlain(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *SavepointStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*SavepointStmt)
	return v.Leave(n)
}

// ReleaseSavepointStmt is the statement of RELEASE SAVEPOINT.
type ReleaseSavepointStmt struct {
	stmtNode
	// Name is the savepoint name.
	Name string
}

// Restore implements Node interface.
func (n *ReleaseSavepointStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RELEASE SAVEPOINT ")
	ctx.WritePlain(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *ReleaseSavepointStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*ReleaseSavepointStmt)
	return v.Leave(n)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Restore implements Node interface.
func (n *SetStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET ")
	for i, v := range n.Variables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore SetStmt.Variables[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetStmt)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

// SecureText implements SensitiveStatement interface.
// need to redact the tidb_cloud_storage_url for safety when `show processlist;`
func (n *SetStmt) SecureText() string {
	redactedStmt := *n
	var sb strings.Builder
	_ = redactedStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

// SetConfigStmt is the statement to set cluster configs.
type SetConfigStmt struct {
	stmtNode

	Type     string // TiDB, TiKV, PD
	Instance string // '127.0.0.1:3306'
	Name     string // the variable name
	Value    ExprNode
}

func (n *SetConfigStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET CONFIG ")
	if n.Type != "" {
		ctx.WriteKeyWord(n.Type)
	} else {
		ctx.WriteString(n.Instance)
	}
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.Name)
	ctx.WritePlain(" = ")
	return n.Value.Restore(ctx)
}

func (n *SetConfigStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetConfigStmt)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// SetSessionStatesStmt is a statement to restore session states.
type SetSessionStatesStmt struct {
	stmtNode

	SessionStates string
}

func (n *SetSessionStatesStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET SESSION_STATES ")
	ctx.WriteString(n.SessionStates)
	return nil
}

func (n *SetSessionStatesStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetSessionStatesStmt)
	return v.Leave(n)
}

/*
// SetCharsetStmt is a statement to assign values to character and collation variables.
// See https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
type SetCharsetStmt struct {
	stmtNode

	Charset string
	Collate string
}

// Accept implements Node Accept interface.
func (n *SetCharsetStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetCharsetStmt)
	return v.Leave(n)
}
*/

// SetPwdStmt is a statement to assign a password to user account.
// See https://dev.mysql.com/doc/refman/5.7/en/set-password.html
type SetPwdStmt struct {
	stmtNode

	User     *auth.UserIdentity
	Password string
}

// Restore implements Node interface.
func (n *SetPwdStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET PASSWORD")
	if n.User != nil {
		ctx.WriteKeyWord(" FOR ")
		if err := n.User.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore SetPwdStmt.User")
		}
	}
	ctx.WritePlain("=")
	ctx.WriteString(n.Password)
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *SetPwdStmt) SecureText() string {
	return fmt.Sprintf("set password for user %s", n.User)
}

// Accept implements Node Accept interface.
func (n *SetPwdStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetPwdStmt)
	return v.Leave(n)
}

type ChangeStmt struct {
	stmtNode

	NodeType string
	State    string
	NodeID   string
}

// Restore implements Node interface.
func (n *ChangeStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CHANGE ")
	ctx.WriteKeyWord(n.NodeType)
	ctx.WriteKeyWord(" TO NODE_STATE ")
	ctx.WritePlain("=")
	ctx.WriteString(n.State)
	ctx.WriteKeyWord(" FOR NODE_ID ")
	ctx.WriteString(n.NodeID)
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *ChangeStmt) SecureText() string {
	return fmt.Sprintf("change %s to node_state='%s' for node_id '%s'", strings.ToLower(n.NodeType), n.State, n.NodeID)
}

// Accept implements Node Accept interface.
func (n *ChangeStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ChangeStmt)
	return v.Leave(n)
}

// SetRoleStmtType is the type for FLUSH statement.
type SetRoleStmtType int

// SetRole statement types.
const (
	SetRoleDefault SetRoleStmtType = iota
	SetRoleNone
	SetRoleAll
	SetRoleAllExcept
	SetRoleRegular
)

type SetRoleStmt struct {
	stmtNode

	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
}

func (n *SetRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET ROLE")
	switch n.SetRoleOpt {
	case SetRoleDefault:
		ctx.WriteKeyWord(" DEFAULT")
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	case SetRoleAllExcept:
		ctx.WriteKeyWord(" ALL EXCEPT")
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Restore(ctx)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetRoleStmt.RoleList")
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetRoleStmt)
	return v.Leave(n)
}

type SetDefaultRoleStmt struct {
	stmtNode

	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
	UserList   []*auth.UserIdentity
}

func (n *SetDefaultRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET DEFAULT ROLE")
	switch n.SetRoleOpt {
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	default:
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Restore(ctx)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetDefaultRoleStmt.RoleList")
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	ctx.WritePlain(" TO")
	for i, user := range n.UserList {
		ctx.WritePlain(" ")
		err := user.Restore(ctx)
		if err != nil {
			return errors.Annotate(err, "An error occurred while restore SetDefaultRoleStmt.UserList")
		}
		if i != len(n.UserList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetDefaultRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetDefaultRoleStmt)
	return v.Leave(n)
}

// UserSpec is used for parsing create user statement.
type UserSpec struct {
	User    *auth.UserIdentity
	AuthOpt *AuthOption
	IsRole  bool
}

// Restore implements Node interface.
func (n *UserSpec) Restore(ctx *format.RestoreCtx) error {
	if err := n.User.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore UserSpec.User")
	}
	if n.AuthOpt != nil {
		ctx.WritePlain(" ")
		if err := n.AuthOpt.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore UserSpec.AuthOpt")
		}
	}
	return nil
}

// SecurityString formats the UserSpec without password information.
func (n *UserSpec) SecurityString() string {
	withPassword := false
	if opt := n.AuthOpt; opt != nil {
		if len(opt.AuthString) > 0 || len(opt.HashString) > 0 {
			withPassword = true
		}
	}
	if withPassword {
		return fmt.Sprintf("{%s password = ***}", n.User)
	}
	return n.User.String()
}

type AuthTokenOrTLSOption struct {
	Type  AuthTokenOrTLSOptionType
	Value string
}

func (t *AuthTokenOrTLSOption) Restore(ctx *format.RestoreCtx) error {
	switch t.Type {
	case TlsNone:
		ctx.WriteKeyWord("NONE")
	case Ssl:
		ctx.WriteKeyWord("SSL")
	case X509:
		ctx.WriteKeyWord("X509")
	case Cipher:
		ctx.WriteKeyWord("CIPHER ")
		ctx.WriteString(t.Value)
	case Issuer:
		ctx.WriteKeyWord("ISSUER ")
		ctx.WriteString(t.Value)
	case Subject:
		ctx.WriteKeyWord("SUBJECT ")
		ctx.WriteString(t.Value)
	case SAN:
		ctx.WriteKeyWord("SAN ")
		ctx.WriteString(t.Value)
	case TokenIssuer:
		ctx.WriteKeyWord("TOKEN_ISSUER ")
		ctx.WriteString(t.Value)
	default:
		return errors.Errorf("Unsupported AuthTokenOrTLSOption.Type %d", t.Type)
	}
	return nil
}

type AuthTokenOrTLSOptionType int

const (
	TlsNone AuthTokenOrTLSOptionType = iota
	Ssl
	X509
	Cipher
	Issuer
	Subject
	SAN
	TokenIssuer
)

func (t AuthTokenOrTLSOptionType) String() string {
	switch t {
	case TlsNone:
		return "NONE"
	case Ssl:
		return "SSL"
	case X509:
		return "X509"
	case Cipher:
		return "CIPHER"
	case Issuer:
		return "ISSUER"
	case Subject:
		return "SUBJECT"
	case SAN:
		return "SAN"
	case TokenIssuer:
		return "TOKEN_ISSUER"
	default:
		return "UNKNOWN"
	}
}

const (
	MaxQueriesPerHour = iota + 1
	MaxUpdatesPerHour
	MaxConnectionsPerHour
	MaxUserConnections
)

type ResourceOption struct {
	Type  int
	Count int64
}

func (r *ResourceOption) Restore(ctx *format.RestoreCtx) error {
	switch r.Type {
	case MaxQueriesPerHour:
		ctx.WriteKeyWord("MAX_QUERIES_PER_HOUR ")
	case MaxUpdatesPerHour:
		ctx.WriteKeyWord("MAX_UPDATES_PER_HOUR ")
	case MaxConnectionsPerHour:
		ctx.WriteKeyWord("MAX_CONNECTIONS_PER_HOUR ")
	case MaxUserConnections:
		ctx.WriteKeyWord("MAX_USER_CONNECTIONS ")
	default:
		return errors.Errorf("Unsupported ResourceOption.Type %d", r.Type)
	}
	ctx.WritePlainf("%d", r.Count)
	return nil
}

const (
	PasswordExpire = iota + 1
	PasswordExpireDefault
	PasswordExpireNever
	PasswordExpireInterval
	PasswordHistory
	PasswordHistoryDefault
	PasswordReuseInterval
	PasswordReuseDefault
	Lock
	Unlock
	FailedLoginAttempts
	PasswordLockTime
	PasswordLockTimeUnbounded
	UserCommentType
	UserAttributeType
	PasswordRequireCurrentDefault

	UserResourceGroupName
)

type PasswordOrLockOption struct {
	Type  int
	Count int64
}

func (p *PasswordOrLockOption) Restore(ctx *format.RestoreCtx) error {
	switch p.Type {
	case PasswordExpire:
		ctx.WriteKeyWord("PASSWORD EXPIRE")
	case PasswordExpireDefault:
		ctx.WriteKeyWord("PASSWORD EXPIRE DEFAULT")
	case PasswordExpireNever:
		ctx.WriteKeyWord("PASSWORD EXPIRE NEVER")
	case PasswordExpireInterval:
		ctx.WriteKeyWord("PASSWORD EXPIRE INTERVAL")
		ctx.WritePlainf(" %d", p.Count)
		ctx.WriteKeyWord(" DAY")
	case Lock:
		ctx.WriteKeyWord("ACCOUNT LOCK")
	case Unlock:
		ctx.WriteKeyWord("ACCOUNT UNLOCK")
	case FailedLoginAttempts:
		ctx.WriteKeyWord("FAILED_LOGIN_ATTEMPTS")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordLockTime:
		ctx.WriteKeyWord("PASSWORD_LOCK_TIME")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordLockTimeUnbounded:
		ctx.WriteKeyWord("PASSWORD_LOCK_TIME UNBOUNDED")
	case PasswordHistory:
		ctx.WriteKeyWord("PASSWORD HISTORY")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordHistoryDefault:
		ctx.WriteKeyWord("PASSWORD HISTORY DEFAULT")
	case PasswordReuseInterval:
		ctx.WriteKeyWord("PASSWORD REUSE INTERVAL")
		ctx.WritePlainf(" %d", p.Count)
		ctx.WriteKeyWord(" DAY")
	case PasswordReuseDefault:
		ctx.WriteKeyWord("PASSWORD REUSE INTERVAL DEFAULT")
	default:
		return errors.Errorf("Unsupported PasswordOrLockOption.Type %d", p.Type)
	}
	return nil
}

type CommentOrAttributeOption struct {
	Type  int
	Value string
}

func (c *CommentOrAttributeOption) Restore(ctx *format.RestoreCtx) error {
	if c.Type == UserCommentType {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(c.Value)
	} else if c.Type == UserAttributeType {
		ctx.WriteKeyWord(" ATTRIBUTE ")
		ctx.WriteString(c.Value)
	}
	return nil
}

type ResourceGroupNameOption struct {
	Value string
}

func (c *ResourceGroupNameOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(" RESOURCE GROUP ")
	ctx.WriteName(c.Value)
	return nil
}

// CreateUserStmt creates user account.
// See https://dev.mysql.com/doc/refman/8.0/en/create-user.html
type CreateUserStmt struct {
	stmtNode

	IsCreateRole             bool
	IfNotExists              bool
	Specs                    []*UserSpec
	AuthTokenOrTLSOptions    []*AuthTokenOrTLSOption
	ResourceOptions          []*ResourceOption
	PasswordOrLockOptions    []*PasswordOrLockOption
	CommentOrAttributeOption *CommentOrAttributeOption
	ResourceGroupNameOption  *ResourceGroupNameOption
}

// Restore implements Node interface.
func (n *CreateUserStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsCreateRole {
		ctx.WriteKeyWord("CREATE ROLE ")
	} else {
		ctx.WriteKeyWord("CREATE USER ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.Specs[%d]", i)
		}
	}

	if len(n.AuthTokenOrTLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.AuthTokenOrTLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.AuthTokenOrTLSOptions[%d]", i)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.ResourceOptions[%d]", i)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.PasswordOrLockOptions[%d]", i)
		}
	}

	if n.CommentOrAttributeOption != nil {
		if err := n.CommentOrAttributeOption.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.CommentOrAttributeOption")
		}
	}

	if n.ResourceGroupNameOption != nil {
		if err := n.ResourceGroupNameOption.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateUserStmt.ResourceGroupNameOption")
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateUserStmt)
	return v.Leave(n)
}

// SecureText implements SensitiveStatement interface.
func (n *CreateUserStmt) SecureText() string {
	var buf bytes.Buffer
	buf.WriteString("create user")
	for _, user := range n.Specs {
		buf.WriteString(" ")
		buf.WriteString(user.SecurityString())
	}
	return buf.String()
}

// AlterUserStmt modifies user account.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-user.html
type AlterUserStmt struct {
	stmtNode

	IfExists                 bool
	CurrentAuth              *AuthOption
	Specs                    []*UserSpec
	AuthTokenOrTLSOptions    []*AuthTokenOrTLSOption
	ResourceOptions          []*ResourceOption
	PasswordOrLockOptions    []*PasswordOrLockOption
	CommentOrAttributeOption *CommentOrAttributeOption
	ResourceGroupNameOption  *ResourceGroupNameOption
}

// Restore implements Node interface.
func (n *AlterUserStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER USER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if n.CurrentAuth != nil {
		ctx.WriteKeyWord("USER")
		ctx.WritePlain("() ")
		if err := n.CurrentAuth.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterUserStmt.CurrentAuth")
		}
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.Specs[%d]", i)
		}
	}

	if len(n.AuthTokenOrTLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.AuthTokenOrTLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.AuthTokenOrTLSOptions[%d]", i)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.ResourceOptions[%d]", i)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.PasswordOrLockOptions[%d]", i)
		}
	}

	if n.CommentOrAttributeOption != nil {
		if err := n.CommentOrAttributeOption.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.CommentOrAttributeOption")
		}
	}

	if n.ResourceGroupNameOption != nil {
		if err := n.ResourceGroupNameOption.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterUserStmt.ResourceGroupNameOption")
		}
	}

	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *AlterUserStmt) SecureText() string {
	var buf bytes.Buffer
	buf.WriteString("alter user")
	for _, user := range n.Specs {
		buf.WriteString(" ")
		buf.WriteString(user.SecurityString())
	}
	return buf.String()
}

// Accept implements Node Accept interface.
func (n *AlterUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterUserStmt)
	return v.Leave(n)
}

// AlterInstanceStmt modifies instance.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-instance.html
type AlterInstanceStmt struct {
	stmtNode

	ReloadTLS         bool
	NoRollbackOnError bool
}

// Restore implements Node interface.
func (n *AlterInstanceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER INSTANCE")
	if n.ReloadTLS {
		ctx.WriteKeyWord(" RELOAD TLS")
	}
	if n.NoRollbackOnError {
		ctx.WriteKeyWord(" NO ROLLBACK ON ERROR")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterInstanceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterInstanceStmt)
	return v.Leave(n)
}

// AlterRangeStmt modifies range configuration.
type AlterRangeStmt struct {
	stmtNode
	RangeName       CIStr
	PlacementOption *PlacementOption
}

// Restore implements Node interface.
func (n *AlterRangeStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER RANGE ")
	ctx.WriteName(n.RangeName.O)
	ctx.WritePlain(" ")
	if err := n.PlacementOption.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterRangeStmt.PlacementOption")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterRangeStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterRangeStmt)
	return v.Leave(n)
}

// DropUserStmt creates user account.
// See http://dev.mysql.com/doc/refman/5.7/en/drop-user.html
type DropUserStmt struct {
	stmtNode

	IfExists   bool
	IsDropRole bool
	UserList   []*auth.UserIdentity
}

// Restore implements Node interface.
func (n *DropUserStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsDropRole {
		ctx.WriteKeyWord("DROP ROLE ")
	} else {
		ctx.WriteKeyWord("DROP USER ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, v := range n.UserList {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropUserStmt.UserList[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropUserStmt)
	return v.Leave(n)
}

type StringOrUserVar struct {
	node
	StringLit string
	UserVar   *VariableExpr
}

func (n *StringOrUserVar) Restore(ctx *format.RestoreCtx) error {
	if len(n.StringLit) > 0 {
		ctx.WriteString(n.StringLit)
	}
	if n.UserVar != nil {
		if err := n.UserVar.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ColumnNameOrUserVar.UserVar")
		}
	}
	return nil
}

func (n *StringOrUserVar) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChild := v.Enter(n)
	if skipChild {
		return v.Leave(newNode)
	}
	n = newNode.(*StringOrUserVar)
	if n.UserVar != nil {
		node, ok = n.UserVar.Accept(v)
		if !ok {
			return node, false
		}
		n.UserVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// RecommendIndexOption is the option for recommend index.
type RecommendIndexOption struct {
	Option string
	Value  ValueExpr
}

// RecommendIndexStmt is a statement to recommend index.
type RecommendIndexStmt struct {
	stmtNode

	Action  string
	SQL     string
	ID      int64
	Options []RecommendIndexOption
}

func (n *RecommendIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RECOMMEND INDEX")
	switch n.Action {
	case "run":
		ctx.WriteKeyWord(" RUN")
		if n.SQL != "" {
			ctx.WriteKeyWord(" FOR ")
			ctx.WriteString(n.SQL)
		}
		if len(n.Options) > 0 {
			ctx.WriteKeyWord(" WITH ")
			for i, opt := range n.Options {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				ctx.WriteKeyWord(opt.Option)
				ctx.WritePlain(" = ")
				if err := opt.Value.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore RecommendIndexStmt.Options[%d]", i)
				}
			}
		}
	case "show":
		ctx.WriteKeyWord(" SHOW OPTION")
	case "apply":
		ctx.WriteKeyWord(" APPLY ")
		ctx.WriteKeyWord(fmt.Sprintf("%d", n.ID))
	case "ignore":
		ctx.WriteKeyWord(" IGNORE ")
		ctx.WriteKeyWord(fmt.Sprintf("%d", n.ID))
	case "set":
		ctx.WriteKeyWord(" SET ")
		for i, opt := range n.Options {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteKeyWord(opt.Option)
			ctx.WritePlain(" = ")
			if err := opt.Value.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore RecommendIndexStmt.Options[%d]", i)
			}
		}
	}
	return nil
}

func (n *RecommendIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RecommendIndexStmt)
	return v.Leave(n)
}

// CreateBindingStmt creates sql binding hint.
type CreateBindingStmt struct {
	stmtNode

	GlobalScope bool
	OriginNode  StmtNode
	HintedNode  StmtNode
	PlanDigests []*StringOrUserVar
}

func (n *CreateBindingStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.GlobalScope {
		ctx.WriteKeyWord("GLOBAL ")
	} else {
		ctx.WriteKeyWord("SESSION ")
	}
	if n.OriginNode == nil {
		ctx.WriteKeyWord("BINDING FROM HISTORY USING PLAN DIGEST ")
		for i, v := range n.PlanDigests {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore CreateBindingStmt.PlanDigests[%d]", i)
			}
		}
	} else {
		ctx.WriteKeyWord("BINDING FOR ")
		if err := n.OriginNode.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		ctx.WriteKeyWord(" USING ")
		if err := n.HintedNode.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (n *CreateBindingStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateBindingStmt)
	if n.OriginNode != nil {
		origNode, ok := n.OriginNode.Accept(v)
		if !ok {
			return n, false
		}
		n.OriginNode = origNode.(StmtNode)
		hintedNode, ok := n.HintedNode.Accept(v)
		if !ok {
			return n, false
		}
		n.HintedNode = hintedNode.(StmtNode)
	} else {
		for i, digest := range n.PlanDigests {
			newDigest, ok := digest.Accept(v)
			if !ok {
				return n, false
			}
			n.PlanDigests[i] = newDigest.(*StringOrUserVar)
		}
	}
	return v.Leave(n)
}

// DropBindingStmt deletes sql binding hint.
type DropBindingStmt struct {
	stmtNode

	GlobalScope bool
	OriginNode  StmtNode
	HintedNode  StmtNode
	SQLDigests  []*StringOrUserVar
}

func (n *DropBindingStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP ")
	if n.GlobalScope {
		ctx.WriteKeyWord("GLOBAL ")
	} else {
		ctx.WriteKeyWord("SESSION ")
	}
	ctx.WriteKeyWord("BINDING FOR ")
	if n.OriginNode == nil {
		ctx.WriteKeyWord("SQL DIGEST ")
		for i, v := range n.SQLDigests {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore CreateBindingStmt.PlanDigests[%d]", i)
			}
		}
	} else {
		if err := n.OriginNode.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		if n.HintedNode != nil {
			ctx.WriteKeyWord(" USING ")
			if err := n.HintedNode.Restore(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (n *DropBindingStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropBindingStmt)
	if n.OriginNode != nil {
		//  OriginNode is nil means we build drop binding by sql digest
		origNode, ok := n.OriginNode.Accept(v)
		if !ok {
			return n, false
		}
		n.OriginNode = origNode.(StmtNode)
		if n.HintedNode != nil {
			hintedNode, ok := n.HintedNode.Accept(v)
			if !ok {
				return n, false
			}
			n.HintedNode = hintedNode.(StmtNode)
		}
	} else {
		for i, digest := range n.SQLDigests {
			newDigest, ok := digest.Accept(v)
			if !ok {
				return n, false
			}
			n.SQLDigests[i] = newDigest.(*StringOrUserVar)
		}
	}
	return v.Leave(n)
}

// BindingStatusType defines the status type for the binding
type BindingStatusType int8

// Binding status types.
const (
	BindingStatusTypeEnabled BindingStatusType = iota
	BindingStatusTypeDisabled
)

// SetBindingStmt sets sql binding status.
type SetBindingStmt struct {
	stmtNode

	BindingStatusType BindingStatusType
	OriginNode        StmtNode
	HintedNode        StmtNode
	SQLDigest         string
}

func (n *SetBindingStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET ")
	ctx.WriteKeyWord("BINDING ")
	switch n.BindingStatusType {
	case BindingStatusTypeEnabled:
		ctx.WriteKeyWord("ENABLED ")
	case BindingStatusTypeDisabled:
		ctx.WriteKeyWord("DISABLED ")
	}
	ctx.WriteKeyWord("FOR ")
	if n.OriginNode == nil {
		ctx.WriteKeyWord("SQL DIGEST ")
		ctx.WriteString(n.SQLDigest)
	} else {
		if err := n.OriginNode.Restore(ctx); err != nil {
			return errors.Trace(err)
		}
		if n.HintedNode != nil {
			ctx.WriteKeyWord(" USING ")
			if err := n.HintedNode.Restore(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (n *SetBindingStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetBindingStmt)
	if n.OriginNode != nil {
		// OriginNode is nil means we set binding stmt by sql digest
		origNode, ok := n.OriginNode.Accept(v)
		if !ok {
			return n, false
		}
		n.OriginNode = origNode.(StmtNode)
		if n.HintedNode != nil {
			hintedNode, ok := n.HintedNode.Accept(v)
			if !ok {
				return n, false
			}
			n.HintedNode = hintedNode.(StmtNode)
		}
	}
	return v.Leave(n)
}

// Extended statistics types.
const (
	StatsTypeCardinality uint8 = iota
	StatsTypeDependency
	StatsTypeCorrelation
)

// StatisticsSpec is the specification for ADD /DROP STATISTICS.
type StatisticsSpec struct {
	StatsName string
	StatsType uint8
	Columns   []*ColumnName
}

// CreateStatisticsStmt is a statement to create extended statistics.
// Examples:
//
//	CREATE STATISTICS stats1 (cardinality) ON t(a, b, c);
//	CREATE STATISTICS stats2 (dependency) ON t(a, b);
//	CREATE STATISTICS stats3 (correlation) ON t(a, b);
type CreateStatisticsStmt struct {
	stmtNode

	IfNotExists bool
	StatsName   string
	StatsType   uint8
	Table       *TableName
	Columns     []*ColumnName
}

// Restore implements Node interface.
func (n *CreateStatisticsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE STATISTICS ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.StatsName)
	switch n.StatsType {
	case StatsTypeCardinality:
		ctx.WriteKeyWord(" (cardinality) ")
	case StatsTypeDependency:
		ctx.WriteKeyWord(" (dependency) ")
	case StatsTypeCorrelation:
		ctx.WriteKeyWord(" (correlation) ")
	}
	ctx.WriteKeyWord("ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateStatisticsStmt.Table")
	}

	ctx.WritePlain("(")
	for i, col := range n.Columns {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := col.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateStatisticsStmt.Columns: [%v]", i)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateStatisticsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateStatisticsStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, col := range n.Columns {
		node, ok = col.Accept(v)
		if !ok {
			return n, false
		}
		n.Columns[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// DropStatisticsStmt is a statement to drop extended statistics.
// Examples:
//
//	DROP STATISTICS stats1;
type DropStatisticsStmt struct {
	stmtNode

	StatsName string
}

// Restore implements Node interface.
func (n *DropStatisticsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP STATISTICS ")
	ctx.WriteName(n.StatsName)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropStatisticsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropStatisticsStmt)
	return v.Leave(n)
}

// DoStmt is the struct for DO statement.
type DoStmt struct {
	stmtNode

	Exprs []ExprNode
}

// Restore implements Node interface.
func (n *DoStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DO ")
	for i, v := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DoStmt.Exprs[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DoStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DoStmt)
	for i, val := range n.Exprs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Exprs[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// AdminStmtType is the type for admin statement.
type AdminStmtType int

// Admin statement types.
const (
	AdminShowDDL AdminStmtType = iota + 1
	AdminCheckTable
	AdminShowDDLJobs
	AdminCancelDDLJobs
	AdminPauseDDLJobs
	AdminResumeDDLJobs
	AdminCheckIndex
	AdminRecoverIndex
	AdminCleanupIndex
	AdminCheckIndexRange
	AdminShowDDLJobQueries
	AdminShowDDLJobQueriesWithRange
	AdminChecksumTable
	AdminShowSlow
	AdminShowNextRowID
	AdminReloadExprPushdownBlacklist
	AdminReloadOptRuleBlacklist
	AdminPluginDisable
	AdminPluginEnable
	AdminFlushBindings
	AdminCaptureBindings
	AdminEvolveBindings
	AdminReloadBindings
	AdminReloadStatistics
	AdminFlushPlanCache
	AdminSetBDRRole
	AdminShowBDRRole
	AdminUnsetBDRRole
	AdminAlterDDLJob
	AdminWorkloadRepoCreate
)

// HandleRange represents a range where handle value >= Begin and < End.
type HandleRange struct {
	Begin int64
	End   int64
}

// BDRRole represents the role of the cluster in BDR mode.
type BDRRole string

const (
	BDRRolePrimary   BDRRole = "primary"
	BDRRoleSecondary BDRRole = "secondary"
	BDRRoleNone      BDRRole = ""
)

type StatementScope int

const (
	StatementScopeNone StatementScope = iota
	StatementScopeSession
	StatementScopeInstance
	StatementScopeGlobal
)

// ShowSlowType defines the type for SlowSlow statement.
type ShowSlowType int

const (
	// ShowSlowTop is a ShowSlowType constant.
	ShowSlowTop ShowSlowType = iota
	// ShowSlowRecent is a ShowSlowType constant.
	ShowSlowRecent
)

// ShowSlowKind defines the kind for SlowSlow statement when the type is ShowSlowTop.
type ShowSlowKind int

const (
	// ShowSlowKindDefault is a ShowSlowKind constant.
	ShowSlowKindDefault ShowSlowKind = iota
	// ShowSlowKindInternal is a ShowSlowKind constant.
	ShowSlowKindInternal
	// ShowSlowKindAll is a ShowSlowKind constant.
	ShowSlowKindAll
)

// ShowSlow is used for the following command:
//
//	admin show slow top [ internal | all] N
//	admin show slow recent N
type ShowSlow struct {
	Tp    ShowSlowType
	Count uint64
	Kind  ShowSlowKind
}

// Restore implements Node interface.
func (n *ShowSlow) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ShowSlowRecent:
		ctx.WriteKeyWord("RECENT ")
	case ShowSlowTop:
		ctx.WriteKeyWord("TOP ")
		switch n.Kind {
		case ShowSlowKindDefault:
			// do nothing
		case ShowSlowKindInternal:
			ctx.WriteKeyWord("INTERNAL ")
		case ShowSlowKindAll:
			ctx.WriteKeyWord("ALL ")
		default:
			return errors.New("Unsupported kind of ShowSlowTop")
		}
	default:
		return errors.New("Unsupported type of ShowSlow")
	}
	ctx.WritePlainf("%d", n.Count)
	return nil
}

// LimitSimple is the struct for Admin statement limit option.
type LimitSimple struct {
	Count  uint64
	Offset uint64
}

type AlterJobOption struct {
	// Name is the name of the option, will be converted to lower case during parse.
	Name string
	// only literal is allowed, we use ExprNode to support negative number
	Value ExprNode
}

func (l *AlterJobOption) Restore(ctx *format.RestoreCtx) error {
	if l.Value == nil {
		ctx.WritePlain(l.Name)
	} else {
		ctx.WritePlain(l.Name + " = ")
		if err := l.Value.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterJobOption")
		}
	}
	return nil
}

// AdminStmt is the struct for Admin statement.
type AdminStmt struct {
	stmtNode

	Tp        AdminStmtType
	Index     string
	Tables    []*TableName
	JobIDs    []int64
	JobNumber int64

	HandleRanges    []HandleRange
	ShowSlow        *ShowSlow
	Plugins         []string
	Where           ExprNode
	StatementScope  StatementScope
	LimitSimple     LimitSimple
	BDRRole         BDRRole
	AlterJobOptions []*AlterJobOption
}

// Restore implements Node interface.
func (n *AdminStmt) Restore(ctx *format.RestoreCtx) error {
	restoreTables := func() error {
		for i, v := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AdminStmt.Tables[%d]", i)
			}
		}
		return nil
	}
	restoreJobIDs := func() {
		for i, v := range n.JobIDs {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WritePlainf("%d", v)
		}
	}

	ctx.WriteKeyWord("ADMIN ")
	switch n.Tp {
	case AdminShowDDL:
		ctx.WriteKeyWord("SHOW DDL")
	case AdminShowDDLJobs:
		ctx.WriteKeyWord("SHOW DDL JOBS")
		if n.JobNumber != 0 {
			ctx.WritePlainf(" %d", n.JobNumber)
		}
		if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore ShowStmt.Where")
			}
		}
	case AdminShowNextRowID:
		ctx.WriteKeyWord("SHOW ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WriteKeyWord(" NEXT_ROW_ID")
	case AdminCheckTable:
		ctx.WriteKeyWord("CHECK TABLE ")
		if err := restoreTables(); err != nil {
			return err
		}
	case AdminCheckIndex:
		ctx.WriteKeyWord("CHECK INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminRecoverIndex:
		ctx.WriteKeyWord("RECOVER INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminCleanupIndex:
		ctx.WriteKeyWord("CLEANUP INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
	case AdminCheckIndexRange:
		ctx.WriteKeyWord("CHECK INDEX ")
		if err := restoreTables(); err != nil {
			return err
		}
		ctx.WritePlainf(" %s", n.Index)
		if n.HandleRanges != nil {
			ctx.WritePlain(" ")
			for i, v := range n.HandleRanges {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				ctx.WritePlainf("(%d,%d)", v.Begin, v.End)
			}
		}
	case AdminChecksumTable:
		ctx.WriteKeyWord("CHECKSUM TABLE ")
		if err := restoreTables(); err != nil {
			return err
		}
	case AdminCancelDDLJobs:
		ctx.WriteKeyWord("CANCEL DDL JOBS ")
		restoreJobIDs()
	case AdminPauseDDLJobs:
		ctx.WriteKeyWord("PAUSE DDL JOBS ")
		restoreJobIDs()
	case AdminResumeDDLJobs:
		ctx.WriteKeyWord("RESUME DDL JOBS ")
		restoreJobIDs()
	case AdminShowDDLJobQueries:
		ctx.WriteKeyWord("SHOW DDL JOB QUERIES ")
		restoreJobIDs()
	case AdminShowDDLJobQueriesWithRange:
		ctx.WriteKeyWord("SHOW DDL JOB QUERIES LIMIT ")
		ctx.WritePlainf("%d, %d", n.LimitSimple.Offset, n.LimitSimple.Count)
	case AdminShowSlow:
		ctx.WriteKeyWord("SHOW SLOW ")
		if err := n.ShowSlow.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AdminStmt.ShowSlow")
		}
	case AdminReloadExprPushdownBlacklist:
		ctx.WriteKeyWord("RELOAD EXPR_PUSHDOWN_BLACKLIST")
	case AdminReloadOptRuleBlacklist:
		ctx.WriteKeyWord("RELOAD OPT_RULE_BLACKLIST")
	case AdminPluginEnable:
		ctx.WriteKeyWord("PLUGINS ENABLE")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case AdminPluginDisable:
		ctx.WriteKeyWord("PLUGINS DISABLE")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case AdminFlushBindings:
		ctx.WriteKeyWord("FLUSH BINDINGS")
	case AdminCaptureBindings:
		ctx.WriteKeyWord("CAPTURE BINDINGS")
	case AdminEvolveBindings:
		ctx.WriteKeyWord("EVOLVE BINDINGS")
	case AdminReloadBindings:
		ctx.WriteKeyWord("RELOAD BINDINGS")
	case AdminReloadStatistics:
		ctx.WriteKeyWord("RELOAD STATS_EXTENDED")
	case AdminFlushPlanCache:
		if n.StatementScope == StatementScopeSession {
			ctx.WriteKeyWord("FLUSH SESSION PLAN_CACHE")
		} else if n.StatementScope == StatementScopeInstance {
			ctx.WriteKeyWord("FLUSH INSTANCE PLAN_CACHE")
		} else if n.StatementScope == StatementScopeGlobal {
			ctx.WriteKeyWord("FLUSH GLOBAL PLAN_CACHE")
		}
	case AdminSetBDRRole:
		switch n.BDRRole {
		case BDRRolePrimary:
			ctx.WriteKeyWord("SET BDR ROLE PRIMARY")
		case BDRRoleSecondary:
			ctx.WriteKeyWord("SET BDR ROLE SECONDARY")
		default:
			return errors.New("Unsupported BDR role")
		}
	case AdminShowBDRRole:
		ctx.WriteKeyWord("SHOW BDR ROLE")
	case AdminUnsetBDRRole:
		ctx.WriteKeyWord("UNSET BDR ROLE")
	case AdminAlterDDLJob:
		ctx.WriteKeyWord("ALTER DDL JOBS ")
		ctx.WritePlainf("%d", n.JobNumber)
		for i, option := range n.AlterJobOptions {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlain(" ")
			if err := option.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AdminStmt.AlterJobOptions[%d]", i)
			}
		}
	case AdminWorkloadRepoCreate:
		ctx.WriteKeyWord("CREATE WORKLOAD SNAPSHOT")
	default:
		return errors.New("Unsupported AdminStmt type")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AdminStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*AdminStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}

	if n.Where != nil {
		node, ok := n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}

	return v.Leave(n)
}

// RoleOrPriv is a temporary structure to be further processed into auth.RoleIdentity or PrivElem
type RoleOrPriv struct {
	Symbols string      // hold undecided symbols
	Node    interface{} // hold auth.RoleIdentity or PrivElem that can be sure when parsing
}

func (n *RoleOrPriv) ToRole() (*auth.RoleIdentity, error) {
	if n.Node != nil {
		if r, ok := n.Node.(*auth.RoleIdentity); ok {
			return r, nil
		}
		return nil, errors.Errorf("can't convert to RoleIdentity, type %T", n.Node)
	}
	return &auth.RoleIdentity{Username: n.Symbols, Hostname: "%"}, nil
}

func (n *RoleOrPriv) ToPriv() (*PrivElem, error) {
	if n.Node != nil {
		if p, ok := n.Node.(*PrivElem); ok {
			return p, nil
		}
		return nil, errors.Errorf("can't convert to PrivElem, type %T", n.Node)
	}
	if len(n.Symbols) == 0 {
		return nil, errors.New("symbols should not be length 0")
	}
	return &PrivElem{Priv: mysql.ExtendedPriv, Name: n.Symbols}, nil
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	node

	Priv mysql.PrivilegeType
	Cols []*ColumnName
	Name string
}

// Restore implements Node interface.
func (n *PrivElem) Restore(ctx *format.RestoreCtx) error {
	if n.Priv == mysql.AllPriv {
		ctx.WriteKeyWord("ALL")
	} else if n.Priv == mysql.ExtendedPriv {
		ctx.WriteKeyWord(n.Name)
	} else {
		str, ok := mysql.Priv2Str[n.Priv]
		if !ok {
			return errors.New("Undefined privilege type")
		}
		ctx.WriteKeyWord(str)
	}
	if n.Cols != nil {
		ctx.WritePlain(" (")
		for i, v := range n.Cols {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PrivElem.Cols[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PrivElem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrivElem)
	for i, val := range n.Cols {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// ObjectTypeType is the type for object type.
type ObjectTypeType int

const (
	// ObjectTypeNone is for empty object type.
	ObjectTypeNone ObjectTypeType = iota + 1
	// ObjectTypeTable means the following object is a table.
	ObjectTypeTable
	// ObjectTypeFunction means the following object is a stored function.
	ObjectTypeFunction
	// ObjectTypeProcedure means the following object is a stored procedure.
	ObjectTypeProcedure
)

// Restore implements Node interface.
func (n ObjectTypeType) Restore(ctx *format.RestoreCtx) error {
	switch n {
	case ObjectTypeNone:
		// do nothing
	case ObjectTypeTable:
		ctx.WriteKeyWord("TABLE")
	case ObjectTypeFunction:
		ctx.WriteKeyWord("FUNCTION")
	case ObjectTypeProcedure:
		ctx.WriteKeyWord("PROCEDURE")
	default:
		return errors.New("Unsupported object type")
	}
	return nil
}

// GrantLevelType is the type for grant level.
type GrantLevelType int

const (
	// GrantLevelNone is the dummy const for default value.
	GrantLevelNone GrantLevelType = iota + 1
	// GrantLevelGlobal means the privileges are administrative or apply to all databases on a given server.
	GrantLevelGlobal
	// GrantLevelDB means the privileges apply to all objects in a given database.
	GrantLevelDB
	// GrantLevelTable means the privileges apply to all columns in a given table.
	GrantLevelTable
)

// GrantLevel is used for store the privilege scope.
type GrantLevel struct {
	Level     GrantLevelType
	DBName    string
	TableName string
}

// Restore implements Node interface.
func (n *GrantLevel) Restore(ctx *format.RestoreCtx) error {
	switch n.Level {
	case GrantLevelDB:
		if n.DBName == "" {
			ctx.WritePlain("*")
		} else {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".*")
		}
	case GrantLevelGlobal:
		ctx.WritePlain("*.*")
	case GrantLevelTable:
		if n.DBName != "" {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".")
		}
		ctx.WriteName(n.TableName)
	}
	return nil
}

// RevokeStmt is the struct for REVOKE statement.
type RevokeStmt struct {
	stmtNode

	Privs      []*PrivElem
	ObjectType ObjectTypeType
	Level      *GrantLevel
	Users      []*UserSpec
}

// Restore implements Node interface.
func (n *RevokeStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE ")
	for i, v := range n.Privs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeStmt.Privs[%d]", i)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore RevokeStmt.ObjectType")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore RevokeStmt.Level")
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeStmt.Users[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RevokeStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeStmt)
	for i, val := range n.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(n)
}

// RevokeStmt is the struct for REVOKE statement.
type RevokeRoleStmt struct {
	stmtNode

	Roles []*auth.RoleIdentity
	Users []*auth.UserIdentity
}

// Restore implements Node interface.
func (n *RevokeRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE ")
	for i, role := range n.Roles {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := role.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeRoleStmt.Roles[%d]", i)
		}
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore RevokeRoleStmt.Users[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RevokeRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeRoleStmt)
	return v.Leave(n)
}

// GrantStmt is the struct for GRANT statement.
type GrantStmt struct {
	stmtNode

	Privs                 []*PrivElem
	ObjectType            ObjectTypeType
	Level                 *GrantLevel
	Users                 []*UserSpec
	AuthTokenOrTLSOptions []*AuthTokenOrTLSOption
	WithGrant             bool
}

// Restore implements Node interface.
func (n *GrantStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT ")
	for i, v := range n.Privs {
		if i != 0 && v.Priv != 0 {
			ctx.WritePlain(", ")
		} else if v.Priv == 0 {
			ctx.WritePlain(" ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Privs[%d]", i)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore GrantStmt.ObjectType")
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore GrantStmt.Level")
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Users[%d]", i)
		}
	}
	if n.AuthTokenOrTLSOptions != nil {
		if len(n.AuthTokenOrTLSOptions) != 0 {
			ctx.WriteKeyWord(" REQUIRE ")
		}
		for i, option := range n.AuthTokenOrTLSOptions {
			if i != 0 {
				ctx.WriteKeyWord(" AND ")
			}
			if err := option.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantStmt.AuthTokenOrTLSOptions[%d]", i)
			}
		}
	}
	if n.WithGrant {
		ctx.WriteKeyWord(" WITH GRANT OPTION")
	}
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *GrantStmt) SecureText() string {
	text := n.text
	// Filter "identified by xxx" because it would expose password information.
	idx := strings.Index(strings.ToLower(text), "identified")
	if idx > 0 {
		text = text[:idx]
	}
	return text
}

// Accept implements Node Accept interface.
func (n *GrantStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantStmt)
	for i, val := range n.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(n)
}

// GrantProxyStmt is the struct for GRANT PROXY statement.
type GrantProxyStmt struct {
	stmtNode

	LocalUser     *auth.UserIdentity
	ExternalUsers []*auth.UserIdentity
	WithGrant     bool
}

// Accept implements Node Accept interface.
func (n *GrantProxyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantProxyStmt)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *GrantProxyStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT PROXY ON ")
	if err := n.LocalUser.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while restore GrantProxyStmt.LocalUser")
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.ExternalUsers {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantProxyStmt.ExternalUsers[%d]", i)
		}
	}
	if n.WithGrant {
		ctx.WriteKeyWord(" WITH GRANT OPTION")
	}
	return nil
}

// GrantRoleStmt is the struct for GRANT TO statement.
type GrantRoleStmt struct {
	stmtNode

	Roles []*auth.RoleIdentity
	Users []*auth.UserIdentity
}

// Accept implements Node Accept interface.
func (n *GrantRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantRoleStmt)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *GrantRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT ")
	if len(n.Roles) > 0 {
		for i, role := range n.Roles {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := role.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantRoleStmt.Roles[%d]", i)
			}
		}
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore GrantStmt.Users[%d]", i)
		}
	}
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *GrantRoleStmt) SecureText() string {
	text := n.text
	// Filter "identified by xxx" because it would expose password information.
	idx := strings.Index(strings.ToLower(text), "identified")
	if idx > 0 {
		text = text[:idx]
	}
	return text
}

// ShutdownStmt is a statement to stop the TiDB server.
// See https://dev.mysql.com/doc/refman/5.7/en/shutdown.html
type ShutdownStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *ShutdownStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SHUTDOWN")
	return nil
}

// Accept implements Node Accept interface.
func (n *ShutdownStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ShutdownStmt)
	return v.Leave(n)
}

// RestartStmt is a statement to restart the TiDB server.
// See https://dev.mysql.com/doc/refman/8.0/en/restart.html
type RestartStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *RestartStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESTART")
	return nil
}

// Accept implements Node Accept interface.
func (n *RestartStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RestartStmt)
	return v.Leave(n)
}

// HelpStmt is a statement for server side help
// See https://dev.mysql.com/doc/refman/8.0/en/help.html
type HelpStmt struct {
	stmtNode

	Topic string
}

// Restore implements Node interface.
func (n *HelpStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HELP ")
	ctx.WriteString(n.Topic)
	return nil
}

// Accept implements Node Accept interface.
func (n *HelpStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HelpStmt)
	return v.Leave(n)
}

// RenameUserStmt is a statement to rename a user.
// See http://dev.mysql.com/doc/refman/5.7/en/rename-user.html
type RenameUserStmt struct {
	stmtNode

	UserToUsers []*UserToUser
}

// Restore implements Node interface.
func (n *RenameUserStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RENAME USER ")
	for index, user2user := range n.UserToUsers {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := user2user.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore RenameUserStmt.UserToUsers")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RenameUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RenameUserStmt)

	for i, t := range n.UserToUsers {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.UserToUsers[i] = node.(*UserToUser)
	}
	return v.Leave(n)
}

// UserToUser represents renaming old user to new user used in RenameUserStmt.
type UserToUser struct {
	node
	OldUser *auth.UserIdentity
	NewUser *auth.UserIdentity
}

// Restore implements Node interface.
func (n *UserToUser) Restore(ctx *format.RestoreCtx) error {
	if err := n.OldUser.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore UserToUser.OldUser")
	}
	ctx.WriteKeyWord(" TO ")
	if err := n.NewUser.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore UserToUser.NewUser")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *UserToUser) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UserToUser)
	return v.Leave(n)
}

type BRIEKind uint8
type BRIEOptionType uint16

const (
	BRIEKindBackup BRIEKind = iota
	BRIEKindCancelJob
	BRIEKindStreamStart
	BRIEKindStreamMetaData
	BRIEKindStreamStatus
	BRIEKindStreamPause
	BRIEKindStreamResume
	BRIEKindStreamStop
	BRIEKindStreamPurge
	BRIEKindRestore
	BRIEKindRestorePIT
	BRIEKindShowJob
	BRIEKindShowQuery
	BRIEKindShowBackupMeta
	// common BRIE options
	BRIEOptionRateLimit BRIEOptionType = iota + 1
	BRIEOptionConcurrency
	BRIEOptionChecksum
	BRIEOptionSendCreds
	BRIEOptionCheckpoint
	BRIEOptionStartTS
	BRIEOptionUntilTS
	BRIEOptionChecksumConcurrency
	BRIEOptionEncryptionMethod
	BRIEOptionEncryptionKeyFile
	// backup options
	BRIEOptionBackupTimeAgo
	BRIEOptionBackupTS
	BRIEOptionBackupTSO
	BRIEOptionLastBackupTS
	BRIEOptionLastBackupTSO
	BRIEOptionGCTTL
	BRIEOptionCompressionLevel
	BRIEOptionCompression
	BRIEOptionIgnoreStats
	BRIEOptionLoadStats
	// restore options
	BRIEOptionOnline
	BRIEOptionFullBackupStorage
	BRIEOptionRestoredTS
	BRIEOptionWaitTiflashReady
	BRIEOptionWithSysTable
	// import options
	BRIEOptionAnalyze
	BRIEOptionBackend
	BRIEOptionOnDuplicate
	BRIEOptionSkipSchemaFiles
	BRIEOptionStrictFormat
	BRIEOptionTiKVImporter
	BRIEOptionResume
	// CSV options
	BRIEOptionCSVBackslashEscape
	BRIEOptionCSVDelimiter
	BRIEOptionCSVHeader
	BRIEOptionCSVNotNull
	BRIEOptionCSVNull
	BRIEOptionCSVSeparator
	BRIEOptionCSVTrimLastSeparators

	BRIECSVHeaderIsColumns = ^uint64(0)
)

type BRIEOptionLevel uint64

const (
	BRIEOptionLevelOff      BRIEOptionLevel = iota // equals FALSE
	BRIEOptionLevelRequired                        // equals TRUE
	BRIEOptionLevelOptional
)

func (kind BRIEKind) String() string {
	switch kind {
	case BRIEKindBackup:
		return "BACKUP"
	case BRIEKindRestore:
		return "RESTORE"
	case BRIEKindStreamStart:
		return "BACKUP LOGS"
	case BRIEKindStreamStop:
		return "STOP BACKUP LOGS"
	case BRIEKindStreamPause:
		return "PAUSE BACKUP LOGS"
	case BRIEKindStreamResume:
		return "RESUME BACKUP LOGS"
	case BRIEKindStreamStatus:
		return "SHOW BACKUP LOGS STATUS"
	case BRIEKindStreamMetaData:
		return "SHOW BACKUP LOGS METADATA"
	case BRIEKindStreamPurge:
		return "PURGE BACKUP LOGS"
	case BRIEKindRestorePIT:
		return "RESTORE POINT"
	case BRIEKindShowJob:
		return "SHOW BR JOB"
	case BRIEKindShowQuery:
		return "SHOW BR JOB QUERY"
	case BRIEKindCancelJob:
		return "CANCEL BR JOB"
	case BRIEKindShowBackupMeta:
		return "SHOW BACKUP METADATA"
	default:
		return ""
	}
}

func (kind BRIEOptionType) String() string {
	switch kind {
	case BRIEOptionRateLimit:
		return "RATE_LIMIT"
	case BRIEOptionConcurrency:
		return "CONCURRENCY"
	case BRIEOptionChecksum:
		return "CHECKSUM"
	case BRIEOptionSendCreds:
		return "SEND_CREDENTIALS_TO_TIKV"
	case BRIEOptionBackupTimeAgo, BRIEOptionBackupTS, BRIEOptionBackupTSO:
		return "SNAPSHOT"
	case BRIEOptionLastBackupTS, BRIEOptionLastBackupTSO:
		return "LAST_BACKUP"
	case BRIEOptionOnline:
		return "ONLINE"
	case BRIEOptionCheckpoint:
		return "CHECKPOINT"
	case BRIEOptionAnalyze:
		return "ANALYZE"
	case BRIEOptionBackend:
		return "BACKEND"
	case BRIEOptionOnDuplicate:
		return "ON_DUPLICATE"
	case BRIEOptionSkipSchemaFiles:
		return "SKIP_SCHEMA_FILES"
	case BRIEOptionStrictFormat:
		return "STRICT_FORMAT"
	case BRIEOptionTiKVImporter:
		return "TIKV_IMPORTER"
	case BRIEOptionResume:
		return "RESUME"
	case BRIEOptionCSVBackslashEscape:
		return "CSV_BACKSLASH_ESCAPE"
	case BRIEOptionCSVDelimiter:
		return "CSV_DELIMITER"
	case BRIEOptionCSVHeader:
		return "CSV_HEADER"
	case BRIEOptionCSVNotNull:
		return "CSV_NOT_NULL"
	case BRIEOptionCSVNull:
		return "CSV_NULL"
	case BRIEOptionCSVSeparator:
		return "CSV_SEPARATOR"
	case BRIEOptionCSVTrimLastSeparators:
		return "CSV_TRIM_LAST_SEPARATORS"
	case BRIEOptionFullBackupStorage:
		return "FULL_BACKUP_STORAGE"
	case BRIEOptionRestoredTS:
		return "RESTORED_TS"
	case BRIEOptionStartTS:
		return "START_TS"
	case BRIEOptionUntilTS:
		return "UNTIL_TS"
	case BRIEOptionGCTTL:
		return "GC_TTL"
	case BRIEOptionWaitTiflashReady:
		return "WAIT_TIFLASH_READY"
	case BRIEOptionWithSysTable:
		return "WITH_SYS_TABLE"
	case BRIEOptionIgnoreStats:
		return "IGNORE_STATS"
	case BRIEOptionLoadStats:
		return "LOAD_STATS"
	case BRIEOptionChecksumConcurrency:
		return "CHECKSUM_CONCURRENCY"
	case BRIEOptionCompressionLevel:
		return "COMPRESSION_LEVEL"
	case BRIEOptionCompression:
		return "COMPRESSION_TYPE"
	case BRIEOptionEncryptionMethod:
		return "ENCRYPTION_METHOD"
	case BRIEOptionEncryptionKeyFile:
		return "ENCRYPTION_KEY_FILE"
	default:
		return ""
	}
}

func (level BRIEOptionLevel) String() string {
	switch level {
	case BRIEOptionLevelOff:
		return "OFF"
	case BRIEOptionLevelOptional:
		return "OPTIONAL"
	case BRIEOptionLevelRequired:
		return "REQUIRED"
	default:
		return ""
	}
}

type BRIEOption struct {
	Tp        BRIEOptionType
	StrValue  string
	UintValue uint64
}

func (opt *BRIEOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(opt.Tp.String())
	ctx.WritePlain(" = ")
	switch opt.Tp {
	case BRIEOptionBackupTS, BRIEOptionLastBackupTS, BRIEOptionBackend, BRIEOptionOnDuplicate, BRIEOptionTiKVImporter, BRIEOptionCSVDelimiter, BRIEOptionCSVNull, BRIEOptionCSVSeparator, BRIEOptionFullBackupStorage, BRIEOptionRestoredTS, BRIEOptionStartTS, BRIEOptionUntilTS, BRIEOptionGCTTL, BRIEOptionCompression, BRIEOptionEncryptionMethod, BRIEOptionEncryptionKeyFile:
		ctx.WriteString(opt.StrValue)
	case BRIEOptionBackupTimeAgo:
		ctx.WritePlainf("%d ", opt.UintValue/1000)
		ctx.WriteKeyWord("MICROSECOND AGO")
	case BRIEOptionRateLimit:
		ctx.WritePlainf("%d ", opt.UintValue/1048576)
		ctx.WriteKeyWord("MB")
		ctx.WritePlain("/")
		ctx.WriteKeyWord("SECOND")
	case BRIEOptionCSVHeader:
		if opt.UintValue == BRIECSVHeaderIsColumns {
			ctx.WriteKeyWord("COLUMNS")
		} else {
			ctx.WritePlainf("%d", opt.UintValue)
		}
	case BRIEOptionChecksum, BRIEOptionAnalyze:
		// BACKUP/RESTORE doesn't support OPTIONAL value for now, should warn at executor
		ctx.WriteKeyWord(BRIEOptionLevel(opt.UintValue).String())
	default:
		ctx.WritePlainf("%d", opt.UintValue)
	}
	return nil
}

// BRIEStmt is a statement for backup, restore, import and export.
type BRIEStmt struct {
	stmtNode

	Kind    BRIEKind
	Schemas []string
	Tables  []*TableName
	Storage string
	JobID   int64
	Options []*BRIEOption
}

func (n *BRIEStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BRIEStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

func (n *BRIEStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.Kind.String())

	switch n.Kind {
	case BRIEKindRestore, BRIEKindBackup:
		switch {
		case len(n.Tables) != 0:
			ctx.WriteKeyWord(" TABLE ")
			for index, table := range n.Tables {
				if index != 0 {
					ctx.WritePlain(", ")
				}
				if err := table.Restore(ctx); err != nil {
					return errors.Annotatef(err, "An error occurred while restore BRIEStmt.Tables[%d]", index)
				}
			}
		case len(n.Schemas) != 0:
			ctx.WriteKeyWord(" DATABASE ")
			for index, schema := range n.Schemas {
				if index != 0 {
					ctx.WritePlain(", ")
				}
				ctx.WriteName(schema)
			}
		default:
			ctx.WriteKeyWord(" DATABASE")
			ctx.WritePlain(" *")
		}

		if n.Kind == BRIEKindBackup {
			ctx.WriteKeyWord(" TO ")
			ctx.WriteString(n.Storage)
		} else {
			ctx.WriteKeyWord(" FROM ")
			ctx.WriteString(n.Storage)
		}
	case BRIEKindCancelJob, BRIEKindShowJob, BRIEKindShowQuery:
		ctx.WritePlainf(" %d", n.JobID)
	case BRIEKindStreamStart:
		ctx.WriteKeyWord(" TO ")
		ctx.WriteString(n.Storage)
	case BRIEKindRestorePIT, BRIEKindStreamMetaData, BRIEKindShowBackupMeta, BRIEKindStreamPurge:
		ctx.WriteKeyWord(" FROM ")
		ctx.WriteString(n.Storage)
	}

	for _, opt := range n.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return err
		}
	}

	return nil
}

// RedactURL redacts the secret tokens in the URL. only S3 url need redaction for now.
// if the url is not a valid url, return the original string.
func RedactURL(str string) string {
	// FIXME: this solution is not scalable, and duplicates some logic from BR.
	u, err := url.Parse(str)
	if err != nil {
		return str
	}
	scheme := u.Scheme
	failpoint.Inject("forceRedactURL", func() {
		scheme = "s3"
	})
	switch strings.ToLower(scheme) {
	case "s3", "ks3":
		values := u.Query()
		for k := range values {
			// see below on why we normalize key
			// https://github.com/pingcap/tidb/blob/a7c0d95f16ea2582bb569278c3f829403e6c3a7e/br/pkg/storage/parse.go#L163
			normalizedKey := strings.ToLower(strings.ReplaceAll(k, "_", "-"))
			if normalizedKey == "access-key" || normalizedKey == "secret-access-key" || normalizedKey == "session-token" {
				values[k] = []string{"xxxxxx"}
			}
		}
		u.RawQuery = values.Encode()
	}
	return u.String()
}

// SecureText implements SensitiveStmtNode
func (n *BRIEStmt) SecureText() string {
	redactedStmt := &BRIEStmt{
		Kind:    n.Kind,
		Schemas: n.Schemas,
		Tables:  n.Tables,
		Storage: RedactURL(n.Storage),
		Options: n.Options,
	}

	var sb strings.Builder
	_ = redactedStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

type ImportIntoActionTp string

const (
	ImportIntoCancel ImportIntoActionTp = "cancel"
)

// ImportIntoActionStmt represent CANCEL IMPORT INTO JOB statement.
// will support pause/resume/drop later.
type ImportIntoActionStmt struct {
	stmtNode

	Tp    ImportIntoActionTp
	JobID int64
}

func (n *ImportIntoActionStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

func (n *ImportIntoActionStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Tp != ImportIntoCancel {
		return errors.Errorf("invalid IMPORT INTO action type: %s", n.Tp)
	}
	ctx.WriteKeyWord("CANCEL IMPORT JOB ")
	ctx.WritePlainf("%d", n.JobID)
	return nil
}

// Ident is the table identifier composed of schema name and table name.
type Ident struct {
	Schema CIStr
	Name   CIStr
}

// String implements fmt.Stringer interface.
func (i Ident) String() string {
	if i.Schema.O == "" {
		return i.Name.O
	}
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}

// SelectStmtOpts wrap around select hints and switches
type SelectStmtOpts struct {
	Distinct        bool
	SQLBigResult    bool
	SQLBufferResult bool
	SQLCache        bool
	SQLSmallResult  bool
	CalcFoundRows   bool
	StraightJoin    bool
	Priority        mysql.PriorityEnum
	TableHints      []*TableOptimizerHint
	ExplicitAll     bool
}

// TableOptimizerHint is Table level optimizer hint
type TableOptimizerHint struct {
	node
	// HintName is the name or alias of the table(s) which the hint will affect.
	// Table hints has no schema info
	// It allows only table name or alias (if table has an alias)
	HintName CIStr
	// HintData is the payload of the hint. The actual type of this field
	// is defined differently as according `HintName`. Define as following:
	//
	// Statement Execution Time Optimizer Hints
	// See https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html#optimizer-hints-execution-time
	// - MAX_EXECUTION_TIME  => uint64
	// - MEMORY_QUOTA        => int64
	// - QUERY_TYPE          => CIStr
	//
	// Time Range is used to hint the time range of inspection tables
	// e.g: select /*+ time_range('','') */ * from information_schema.inspection_result.
	// - TIME_RANGE          => ast.HintTimeRange
	// - READ_FROM_STORAGE   => CIStr
	// - USE_TOJA            => bool
	// - NTH_PLAN            => int64
	HintData interface{}
	// QBName is the default effective query block of this hint.
	QBName  CIStr
	Tables  []HintTable
	Indexes []CIStr
}

// HintTimeRange is the payload of `TIME_RANGE` hint
type HintTimeRange struct {
	From string
	To   string
}

// HintSetVar is the payload of `SET_VAR` hint
type HintSetVar struct {
	VarName string
	Value   string
}

// HintTable is table in the hint. It may have query block info.
type HintTable struct {
	DBName        CIStr
	TableName     CIStr
	QBName        CIStr
	PartitionList []CIStr
}

func (ht *HintTable) Restore(ctx *format.RestoreCtx) {
	if !ctx.Flags.HasWithoutSchemaNameFlag() {
		if ht.DBName.L != "" {
			ctx.WriteName(ht.DBName.String())
			ctx.WriteKeyWord(".")
		}
	}
	ctx.WriteName(ht.TableName.String())
	if ht.QBName.L != "" {
		ctx.WriteKeyWord("@")
		ctx.WriteName(ht.QBName.String())
	}
	if len(ht.PartitionList) > 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i, p := range ht.PartitionList {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(p.String())
		}
		ctx.WritePlain(")")
	}
}

// Restore implements Node interface.
func (n *TableOptimizerHint) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.HintName.String())
	ctx.WritePlain("(")
	if n.QBName.L != "" {
		if n.HintName.L != "qb_name" {
			ctx.WriteKeyWord("@")
		}
		ctx.WriteName(n.QBName.String())
	}
	if n.HintName.L == "qb_name" && len(n.Tables) == 0 {
		ctx.WritePlain(")")
		return nil
	}
	// Hints without args except query block.
	switch n.HintName.L {
	case "mpp_1phase_agg", "mpp_2phase_agg", "hash_agg", "stream_agg", "agg_to_cop", "read_consistent_replica", "no_index_merge", "ignore_plan_cache", "limit_to_cop", "straight_join", "merge", "no_decorrelate":
		ctx.WritePlain(")")
		return nil
	}
	if n.QBName.L != "" {
		ctx.WritePlain(" ")
	}
	// Hints with args except query block.
	switch n.HintName.L {
	case "max_execution_time":
		ctx.WritePlainf("%d", n.HintData.(uint64))
	case "resource_group":
		ctx.WriteName(n.HintData.(string))
	case "nth_plan":
		ctx.WritePlainf("%d", n.HintData.(int64))
	case "tidb_hj", "tidb_smj", "tidb_inlj", "hash_join", "hash_join_build", "hash_join_probe", "merge_join", "inl_join",
		"broadcast_join", "shuffle_join", "inl_hash_join", "inl_merge_join", "leading", "no_hash_join", "no_merge_join",
		"no_index_join", "no_index_hash_join", "no_index_merge_join":
		for i, table := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			table.Restore(ctx)
		}
	case "use_index", "ignore_index", "use_index_merge", "force_index", "order_index", "no_order_index":
		n.Tables[0].Restore(ctx)
		ctx.WritePlain(" ")
		for i, index := range n.Indexes {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(index.String())
		}
	case "qb_name":
		if len(n.Tables) > 0 {
			ctx.WritePlain(", ")
			for i, table := range n.Tables {
				if i != 0 {
					ctx.WritePlain(". ")
				}
				table.Restore(ctx)
			}
		}
	case "use_toja", "use_cascades":
		if n.HintData.(bool) {
			ctx.WritePlain("TRUE")
		} else {
			ctx.WritePlain("FALSE")
		}
	case "query_type":
		ctx.WriteKeyWord(n.HintData.(CIStr).String())
	case "memory_quota":
		ctx.WritePlainf("%d MB", n.HintData.(int64)/1024/1024)
	case "read_from_storage":
		ctx.WriteKeyWord(n.HintData.(CIStr).String())
		for i, table := range n.Tables {
			if i == 0 {
				ctx.WritePlain("[")
			}
			table.Restore(ctx)
			if i == len(n.Tables)-1 {
				ctx.WritePlain("]")
			} else {
				ctx.WritePlain(", ")
			}
		}
	case "time_range":
		hintData := n.HintData.(HintTimeRange)
		ctx.WriteString(hintData.From)
		ctx.WritePlain(", ")
		ctx.WriteString(hintData.To)
	case "set_var":
		hintData := n.HintData.(HintSetVar)
		ctx.WritePlain(hintData.VarName)
		ctx.WritePlain(" = ")
		ctx.WriteString(hintData.Value)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *TableOptimizerHint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableOptimizerHint)
	return v.Leave(n)
}

// TextString represent a string, it can be a binary literal.
type TextString struct {
	Value           string
	IsBinaryLiteral bool
}

type BinaryLiteral interface {
	ToString() string
}

// NewDecimal creates a types.Decimal value, it's provided by parser driver.
var NewDecimal func(string) (interface{}, error)

// NewHexLiteral creates a types.HexLiteral value, it's provided by parser driver.
var NewHexLiteral func(string) (interface{}, error)

// NewBitLiteral creates a types.BitLiteral value, it's provided by parser driver.
var NewBitLiteral func(string) (interface{}, error)

// SetResourceGroupStmt is a statement to set the resource group name for current session.
type SetResourceGroupStmt struct {
	stmtNode
	Name CIStr
}

func (n *SetResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET RESOURCE GROUP ")
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *SetResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetResourceGroupStmt)
	return v.Leave(n)
}

// CalibrateResourceType is the type for CalibrateResource statement.
type CalibrateResourceType int

// calibrate resource [ workload < TPCC | OLTP_READ_WRITE | OLTP_READ_ONLY | OLTP_WRITE_ONLY | TPCH_10> ]
const (
	WorkloadNone CalibrateResourceType = iota
	TPCC
	OLTPREADWRITE
	OLTPREADONLY
	OLTPWRITEONLY
	TPCH10
)

func (n CalibrateResourceType) Restore(ctx *format.RestoreCtx) error {
	switch n {
	case TPCC:
		ctx.WriteKeyWord(" WORKLOAD TPCC")
	case OLTPREADWRITE:
		ctx.WriteKeyWord(" WORKLOAD OLTP_READ_WRITE")
	case OLTPREADONLY:
		ctx.WriteKeyWord(" WORKLOAD OLTP_READ_ONLY")
	case OLTPWRITEONLY:
		ctx.WriteKeyWord(" WORKLOAD OLTP_WRITE_ONLY")
	case TPCH10:
		ctx.WriteKeyWord(" WORKLOAD TPCH_10")
	}
	return nil
}

// CalibrateResourceStmt is a statement to fetch the cluster RU capacity
type CalibrateResourceStmt struct {
	stmtNode
	DynamicCalibrateResourceOptionList []*DynamicCalibrateResourceOption
	Tp                                 CalibrateResourceType
}

// Restore implements Node interface.
func (n *CalibrateResourceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CALIBRATE RESOURCE")
	if err := n.Tp.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CalibrateResourceStmt.CalibrateResourceType")
	}
	for i, option := range n.DynamicCalibrateResourceOptionList {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing DynamicCalibrateResourceOption: [%v]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CalibrateResourceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CalibrateResourceStmt)
	for _, val := range n.DynamicCalibrateResourceOptionList {
		_, ok := val.Accept(v)
		if !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

type DynamicCalibrateType int

const (
	// specific time
	CalibrateStartTime = iota
	CalibrateEndTime
	CalibrateDuration
)

type DynamicCalibrateResourceOption struct {
	stmtNode
	Tp       DynamicCalibrateType
	StrValue string
	Ts       ExprNode
	Unit     TimeUnitType
}

func (n *DynamicCalibrateResourceOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case CalibrateStartTime:
		ctx.WriteKeyWord("START_TIME ")
		if err := n.Ts.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing DynamicCalibrateResourceOption StartTime")
		}
	case CalibrateEndTime:
		ctx.WriteKeyWord("END_TIME ")
		if err := n.Ts.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing DynamicCalibrateResourceOption EndTime")
		}
	case CalibrateDuration:
		ctx.WriteKeyWord("DURATION ")
		if len(n.StrValue) > 0 {
			ctx.WriteString(n.StrValue)
		} else {
			ctx.WriteKeyWord("INTERVAL ")
			if err := n.Ts.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore DynamicCalibrateResourceOption DURATION TS")
			}
			ctx.WritePlain(" ")
			ctx.WriteKeyWord(n.Unit.String())
		}
	default:
		return errors.Errorf("invalid DynamicCalibrateResourceOption: %d", n.Tp)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DynamicCalibrateResourceOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DynamicCalibrateResourceOption)
	if n.Ts != nil {
		node, ok := n.Ts.Accept(v)
		if !ok {
			return n, false
		}
		n.Ts = node.(ExprNode)
	}
	return v.Leave(n)
}

// DropQueryWatchStmt is a statement to drop a runaway watch item.
type DropQueryWatchStmt struct {
	stmtNode
	IntValue int64
}

func (n *DropQueryWatchStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("QUERY WATCH REMOVE ")
	ctx.WritePlainf("%d", n.IntValue)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropQueryWatchStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*DropQueryWatchStmt)
	return v.Leave(n)
}

// AddQueryWatchStmt is a statement to add a runaway watch item.
type AddQueryWatchStmt struct {
	stmtNode
	QueryWatchOptionList []*QueryWatchOption
}

func (n *AddQueryWatchStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("QUERY WATCH ADD")
	for i, option := range n.QueryWatchOptionList {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing QueryWatchOptionList: [%v]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AddQueryWatchStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*AddQueryWatchStmt)
	for _, val := range n.QueryWatchOptionList {
		_, ok := val.Accept(v)
		if !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

type QueryWatchOptionType int

const (
	QueryWatchResourceGroup QueryWatchOptionType = iota
	QueryWatchAction
	QueryWatchType
)

// QueryWatchOption is used for parsing manual management of watching runaway queries option.
type QueryWatchOption struct {
	stmtNode
	Tp                  QueryWatchOptionType
	ResourceGroupOption *QueryWatchResourceGroupOption
	ActionOption        *ResourceGroupRunawayActionOption
	TextOption          *QueryWatchTextOption
}

// Restore implements Node interface.
func (n *QueryWatchOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case QueryWatchResourceGroup:
		return n.ResourceGroupOption.restore(ctx)
	case QueryWatchAction:
		return n.ActionOption.Restore(ctx)
	case QueryWatchType:
		return n.TextOption.Restore(ctx)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *QueryWatchOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*QueryWatchOption)
	if n.ResourceGroupOption != nil && n.ResourceGroupOption.GroupNameExpr != nil {
		node, ok := n.ResourceGroupOption.GroupNameExpr.Accept(v)
		if !ok {
			return n, false
		}
		n.ResourceGroupOption.GroupNameExpr = node.(ExprNode)
	}
	if n.ActionOption != nil {
		node, ok := n.ActionOption.Accept(v)
		if !ok {
			return n, false
		}
		n.ActionOption = node.(*ResourceGroupRunawayActionOption)
	}
	if n.TextOption != nil {
		node, ok := n.TextOption.Accept(v)
		if !ok {
			return n, false
		}
		n.TextOption = node.(*QueryWatchTextOption)
	}
	return v.Leave(n)
}

func CheckQueryWatchAppend(ops []*QueryWatchOption, newOp *QueryWatchOption) bool {
	for _, op := range ops {
		if op.Tp == newOp.Tp {
			return false
		}
	}
	return true
}

// QueryWatchResourceGroupOption is used for parsing the query watch resource group name.
type QueryWatchResourceGroupOption struct {
	GroupNameStr  CIStr
	GroupNameExpr ExprNode
}

func (n *QueryWatchResourceGroupOption) restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESOURCE GROUP ")
	if n.GroupNameExpr != nil {
		if err := n.GroupNameExpr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing ExprValue: [%v]", n.GroupNameExpr)
		}
	} else {
		ctx.WriteName(n.GroupNameStr.String())
	}
	return nil
}

// QueryWatchTextOption is used for parsing the query watch text option.
type QueryWatchTextOption struct {
	node
	Type          RunawayWatchType
	PatternExpr   ExprNode
	TypeSpecified bool
}

// Restore implements Node interface.
func (n *QueryWatchTextOption) Restore(ctx *format.RestoreCtx) error {
	if n.TypeSpecified {
		ctx.WriteKeyWord("SQL TEXT ")
		ctx.WriteKeyWord(n.Type.String())
		ctx.WriteKeyWord(" TO ")
	} else {
		switch n.Type {
		case WatchSimilar:
			ctx.WriteKeyWord("SQL DIGEST ")
		case WatchPlan:
			ctx.WriteKeyWord("PLAN DIGEST ")
		}
	}
	if err := n.PatternExpr.Restore(ctx); err != nil {
		return errors.Annotatef(err, "An error occurred while splicing ExprValue: [%v]", n.PatternExpr)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *QueryWatchTextOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*QueryWatchTextOption)
	if n.PatternExpr != nil {
		node, ok := n.PatternExpr.Accept(v)
		if !ok {
			return n, false
		}
		n.PatternExpr = node.(ExprNode)
	}
	return v.Leave(n)
}
