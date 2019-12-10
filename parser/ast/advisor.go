// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/parser/format"
)

var _ StmtNode = &IndexAdviseStmt{}

// IndexAdviseStmt is used to advise indexes
type IndexAdviseStmt struct {
	stmtNode

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *MaxIndexNumClause
	LinesInfo   *LinesClause
}

// Restore implements Node Accept interface.
func (n *IndexAdviseStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("INDEX ADVISE ")
	if n.IsLocal {
		ctx.WriteKeyWord("LOCAL ")
	}
	ctx.WriteKeyWord("INFILE ")
	ctx.WriteString(n.Path)
	if n.MaxMinutes != UnspecifiedSize {
		ctx.WriteKeyWord(" MAX_MINUTES ")
		ctx.WritePlainf("%d", n.MaxMinutes)
	}
	if n.MaxIndexNum != nil {
		n.MaxIndexNum.Restore(ctx)
	}
	n.LinesInfo.Restore(ctx)
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexAdviseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexAdviseStmt)
	return v.Leave(n)
}

// MaxIndexNumClause represents 'maximum number of indexes' clause in index advise statement.
type MaxIndexNumClause struct {
	PerTable uint64
	PerDB    uint64
}

// Restore for max index num clause
func (n *MaxIndexNumClause) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(" MAX_IDXNUM")
	if n.PerTable != UnspecifiedSize {
		ctx.WriteKeyWord(" PER_TABLE ")
		ctx.WritePlainf("%d", n.PerTable)
	}
	if n.PerDB != UnspecifiedSize {
		ctx.WriteKeyWord(" PER_DB ")
		ctx.WritePlainf("%d", n.PerDB)
	}
	return nil
}
