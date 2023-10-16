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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// IndexAdviseExec represents a index advise executor.
type IndexAdviseExec struct {
	exec.BaseExecutor

	IsLocal         bool
	indexAdviseInfo *IndexAdviseInfo
}

// Next implements the Executor Next interface.
func (e *IndexAdviseExec) Next(context.Context, *chunk.Chunk) error {
	if !e.IsLocal {
		return errors.New("Index Advise: don't support load file without local field")
	}
	if e.indexAdviseInfo.Path == "" {
		return errors.New("Index Advise: infile path is empty")
	}
	if len(e.indexAdviseInfo.LinesTerminatedBy) == 0 {
		return errors.New("Index Advise: don't support advise index for SQL terminated by nil")
	}

	if val := e.Ctx().Value(IndexAdviseVarKey); val != nil {
		e.Ctx().SetValue(IndexAdviseVarKey, nil)
		return errors.New("Index Advise: previous index advise option isn't closed normally")
	}
	e.Ctx().SetValue(IndexAdviseVarKey, e.indexAdviseInfo)
	return nil
}

// Close implements the Executor Close interface.
func (*IndexAdviseExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (*IndexAdviseExec) Open(context.Context) error {
	return nil
}

// IndexAdviseInfo saves the information of index advise operation.
type IndexAdviseInfo struct {
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	core.LineFieldsInfo
	Ctx       sessionctx.Context
	StmtNodes [][]ast.StmtNode
	Result    *IndexAdvice
}

func (e *IndexAdviseInfo) getStmtNodes(data []byte) error {
	str := string(data)
	sqls := strings.Split(str, e.LinesTerminatedBy)

	j := 0
	for i, sql := range sqls {
		if sql != "\n" && sql != "" && strings.HasPrefix(sql, e.LinesStartingBy) {
			sqls[j] = sqls[i]
			j++
		}
	}
	sqls = sqls[:j]

	sv := e.Ctx.GetSessionVars()
	e.StmtNodes = make([][]ast.StmtNode, len(sqls))
	sqlParser := parser.New()
	for i, sql := range sqls {
		stmtNodes, warns, err := sqlParser.ParseSQL(sql)
		if err != nil {
			return err
		}
		for _, warn := range warns {
			sv.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
		}
		curStmtNodes := make([]ast.StmtNode, len(stmtNodes))
		copy(curStmtNodes, stmtNodes)
		e.StmtNodes[i] = curStmtNodes
	}
	return nil
}

func (e *IndexAdviseInfo) prepareInfo(data []byte) error {
	if e.MaxMinutes == 0 {
		return errors.New("Index Advise: the maximum execution time limit should be greater than 0")
	}
	if e.MaxIndexNum != nil {
		if e.MaxIndexNum.PerTable == 0 || e.MaxIndexNum.PerDB == 0 {
			return errors.New("Index Advise: the maximum number of indexes should be greater than 0")
		}
	}
	return e.getStmtNodes(data)
}

// GetIndexAdvice gets the index advice by workload file.
func (e *IndexAdviseInfo) GetIndexAdvice(data []byte) error {
	if err := e.prepareInfo(data); err != nil {
		return err
	}
	// TODO: Finish the index advise process. It will be done in another PR.
	return nil
}

// IndexAdvice represents the index advice.
type IndexAdvice struct {
	// TODO: Define index advice data structure and implements the ResultSet interface. It will be done in another PR
}

// IndexAdviseVarKeyType is a dummy type to avoid naming collision in context.
type IndexAdviseVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (IndexAdviseVarKeyType) String() string {
	return "index_advise_var"
}

// IndexAdviseVarKey is a variable key for index advise.
const IndexAdviseVarKey IndexAdviseVarKeyType = 0
