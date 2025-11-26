// Copyright 2020 PingCAP, Inc.
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
	"bufio"
	"bytes"
	"context"
	"math"
	"os"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// SelectIntoExec represents a SelectInto executor.
type SelectIntoExec struct {
	exec.BaseExecutor
	intoOpt *ast.SelectIntoOption
	core.LineFieldsInfo

	lineBuf   []byte
	realBuf   []byte
	fieldBuf  []byte
	escapeBuf []byte
	enclosed  bool
	writer    *bufio.Writer
	dstFile   *os.File
	chk       *chunk.Chunk
	started   bool
}

// Open implements the Executor Open interface.
func (s *SelectIntoExec) Open(ctx context.Context) error {
	if s.intoOpt.Tp == ast.SelectIntoOutfile {
		// MySQL-compatible behavior: allow files to be group-readable
		f, err := os.OpenFile(s.intoOpt.FileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0640) // #nosec G302
		if err != nil {
			return errors.Trace(err)
		}
		s.started = true
		s.dstFile = f
		s.writer = bufio.NewWriter(s.dstFile)
		s.chk = exec.TryNewCacheChunk(s.Children(0))
		s.lineBuf = make([]byte, 0, 1024)
		s.fieldBuf = make([]byte, 0, 64)
		s.escapeBuf = make([]byte, 0, 64)
	} else if s.intoOpt.Tp == ast.SelectIntoVars {
		if len(s.intoOpt.VarList) > 0 && len(s.intoOpt.ProcedureVarList) > 0 {
			return errors.New("User defined variable and procedure variable are not supported in same SelectInto clause")
		}
		varNum := len(s.intoOpt.VarList) + len(s.intoOpt.ProcedureVarList)
		if s.Children(0).Schema().Len() != varNum {
			return mysql.NewErr(mysql.ErrWrongNumberOfColumnsInSelect)
		}
		s.chk = exec.TryNewCacheChunk(s.Children(0))
	} else {
		return errors.New("unsupported SelectInto type")
	}

	return s.BaseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (s *SelectIntoExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if s.intoOpt.Tp == ast.SelectIntoOutfile {
		for {
			if err := exec.Next(ctx, s.Children(0), s.chk); err != nil {
				return err
			}
			if s.chk.NumRows() == 0 {
				break
			}
			if err := s.dumpToOutfile(); err != nil {
				return err
			}
		}
	} else if s.intoOpt.Tp == ast.SelectIntoVars {
		if err := exec.Next(ctx, s.Children(0), s.chk); err != nil {
			return err
		}
		if s.chk.NumRows() == 0 {
			s.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrSpFetchNoData)
			return nil
		} else if s.chk.NumRows() > 1 {
			return mysql.NewErr(mysql.ErrTooManyRows)
		}

		if err := s.assignToVars(s.chk.GetRow(0)); err != nil {
			return err
		}
	} else {
		return errors.New("unsupported SelectInto type")
	}
	return nil
}

func (*SelectIntoExec) considerEncloseOpt(et types.EvalType) bool {
	return et == types.ETString || et == types.ETDuration ||
		et == types.ETTimestamp || et == types.ETDatetime ||
		et == types.ETJson
}

func (s *SelectIntoExec) escapeField(f []byte) []byte {
	if len(s.FieldsEscapedBy) == 0 {
		return f
	}
	s.escapeBuf = s.escapeBuf[:0]
	for _, b := range f {
		escape := false
		switch {
		case b == 0:
			// we always escape 0
			escape = true
			b = '0'
		case b == s.FieldsEscapedBy[0] || (len(s.FieldsEnclosedBy) > 0 && b == s.FieldsEnclosedBy[0]):
			escape = true
		case !s.enclosed && len(s.FieldsTerminatedBy) > 0 && b == s.FieldsTerminatedBy[0]:
			// if field is enclosed, we only escape line terminator, otherwise both field and line terminator will be escaped
			escape = true
		case len(s.LinesTerminatedBy) > 0 && b == s.LinesTerminatedBy[0]:
			// we always escape line terminator
			escape = true
		}
		if escape {
			s.escapeBuf = append(s.escapeBuf, s.FieldsEscapedBy[0])
		}
		s.escapeBuf = append(s.escapeBuf, b)
	}
	return s.escapeBuf
}

func (s *SelectIntoExec) dumpToOutfile() error {
	encloseFlag := false
	var encloseByte byte
	encloseOpt := false
	if len(s.FieldsEnclosedBy) > 0 {
		encloseByte = s.FieldsEnclosedBy[0]
		encloseFlag = true
		encloseOpt = s.FieldsOptEnclosed
	}
	nullTerm := []byte("\\N")
	if len(s.FieldsEscapedBy) > 0 {
		nullTerm[0] = s.FieldsEscapedBy[0]
	} else {
		nullTerm = []byte("NULL")
	}

	cols := s.Children(0).Schema().Columns
	for i := 0; i < s.chk.NumRows(); i++ {
		row := s.chk.GetRow(i)
		s.lineBuf = s.lineBuf[:0]
		for j, col := range cols {
			if j != 0 {
				s.lineBuf = append(s.lineBuf, s.FieldsTerminatedBy...)
			}
			if row.IsNull(j) {
				s.lineBuf = append(s.lineBuf, nullTerm...)
				continue
			}
			et := col.GetType(s.Ctx().GetExprCtx().GetEvalCtx()).EvalType()
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
				s.enclosed = true
			} else {
				s.enclosed = false
			}
			s.fieldBuf = s.fieldBuf[:0]
			switch col.GetType(s.Ctx().GetExprCtx().GetEvalCtx()).GetType() {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeYear:
				s.fieldBuf = strconv.AppendInt(s.fieldBuf, row.GetInt64(j), 10)
			case mysql.TypeLonglong:
				if mysql.HasUnsignedFlag(col.GetType(s.Ctx().GetExprCtx().GetEvalCtx()).GetFlag()) {
					s.fieldBuf = strconv.AppendUint(s.fieldBuf, row.GetUint64(j), 10)
				} else {
					s.fieldBuf = strconv.AppendInt(s.fieldBuf, row.GetInt64(j), 10)
				}
			case mysql.TypeFloat:
				s.realBuf, s.fieldBuf = DumpRealOutfile(s.realBuf, s.fieldBuf, float64(row.GetFloat32(j)), col.RetType)
			case mysql.TypeDouble:
				s.realBuf, s.fieldBuf = DumpRealOutfile(s.realBuf, s.fieldBuf, row.GetFloat64(j), col.RetType)
			case mysql.TypeNewDecimal:
				s.fieldBuf = append(s.fieldBuf, row.GetMyDecimal(j).String()...)
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				s.fieldBuf = append(s.fieldBuf, row.GetBytes(j)...)
			case mysql.TypeBit:
				// bit value won't be escaped anyway (verified on MySQL, test case added)
				s.lineBuf = append(s.lineBuf, row.GetBytes(j)...)
			case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
				s.fieldBuf = append(s.fieldBuf, row.GetTime(j).String()...)
			case mysql.TypeDuration:
				s.fieldBuf = append(s.fieldBuf, row.GetDuration(j, col.GetType(s.Ctx().GetExprCtx().GetEvalCtx()).GetDecimal()).String()...)
			case mysql.TypeEnum:
				s.fieldBuf = append(s.fieldBuf, row.GetEnum(j).String()...)
			case mysql.TypeSet:
				s.fieldBuf = append(s.fieldBuf, row.GetSet(j).String()...)
			case mysql.TypeJSON:
				s.fieldBuf = append(s.fieldBuf, row.GetJSON(j).String()...)
			case mysql.TypeTiDBVectorFloat32:
				s.fieldBuf = append(s.fieldBuf, row.GetVectorFloat32(j).String()...)
			}

			switch col.GetType(s.Ctx().GetExprCtx().GetEvalCtx()).EvalType() {
			case types.ETString, types.ETJson:
				s.lineBuf = append(s.lineBuf, s.escapeField(s.fieldBuf)...)
			default:
				s.lineBuf = append(s.lineBuf, s.fieldBuf...)
			}
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
			}
		}
		s.lineBuf = append(s.lineBuf, s.LinesTerminatedBy...)
		if _, err := s.writer.Write(s.lineBuf); err != nil {
			return errors.Trace(err)
		}
	}
	s.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(uint64(s.chk.NumRows()))
	return nil
}

// Close implements the Executor Close interface.
func (s *SelectIntoExec) Close() error {
	if s.intoOpt.Tp == ast.SelectIntoOutfile {
		if !s.started {
			return nil
		}
		err1 := s.writer.Flush()
		err2 := s.dstFile.Close()
		err3 := s.BaseExecutor.Close()
		if err1 != nil {
			return errors.Trace(err1)
		} else if err2 != nil {
			return errors.Trace(err2)
		}
		return err3
	}
	return nil
}

const (
	expFormatBig   = 1e15
	expFormatSmall = 1e-15
)

// DumpRealOutfile dumps a real number to lineBuf.
func DumpRealOutfile(realBuf, lineBuf []byte, v float64, tp *types.FieldType) ([]byte, []byte) {
	prec := types.UnspecifiedLength
	if tp.GetDecimal() > 0 && tp.GetDecimal() != mysql.NotFixedDec {
		prec = tp.GetDecimal()
	}
	absV := math.Abs(v)
	if prec == types.UnspecifiedLength && (absV >= expFormatBig || (absV != 0 && absV < expFormatSmall)) {
		realBuf = strconv.AppendFloat(realBuf[:0], v, 'e', prec, 64)
		if idx := bytes.IndexByte(realBuf, '+'); idx != -1 {
			lineBuf = append(lineBuf, realBuf[:idx]...)
			lineBuf = append(lineBuf, realBuf[idx+1:]...)
		} else {
			lineBuf = append(lineBuf, realBuf...)
		}
	} else {
		lineBuf = strconv.AppendFloat(lineBuf, v, 'f', prec, 64)
	}
	return realBuf, lineBuf
}

func (s *SelectIntoExec) assignToVars(r chunk.Row) error {
	cols := s.Children(0).Schema().Columns
	if len(s.intoOpt.VarList) > 0 {
		return s.assignToUserDefinedVars(cols, r)
	} else if len(s.intoOpt.ProcedureVarList) > 0 {
		return s.assignToProcedureVars(cols, r)
	}
	return nil
}

func (s *SelectIntoExec) assignToUserDefinedVars(cols []*expression.Column, r chunk.Row) error {
	sessionVars := s.Ctx().GetSessionVars()
	for i, col := range cols {
		name := s.intoOpt.VarList[i].(*ast.VariableExpr).Name
		tp := col.GetType(s.Ctx().GetExprCtx().GetEvalCtx())
		d := r.GetDatum(i, tp)
		if d.IsNull() {
			sessionVars.UnsetUserVar(name)
		} else {
			sessionVars.SetUserVarVal(name, d)
		}
	}
	return nil
}

func (s *SelectIntoExec) assignToProcedureVars(cols []*expression.Column, r chunk.Row) error {
	sessionVars := s.Ctx().GetSessionVars()
	if !sessionVars.GetCallProcedure() {
		return errors.New("Select into procedure variable is only allowed in stored procedure")
	}

	for i, col := range cols {
		name := s.intoOpt.ProcedureVarList[i].(*ast.ColumnNameExpr).Name.Name.O
		_, _, notFind := sessionVars.GetProcedureVariable(name)
		if notFind {
			return plannererrors.ErrSpUndeclaredVar.GenWithStackByArgs(name)
		}
		tp := col.GetType(s.Ctx().GetExprCtx().GetEvalCtx())
		d := r.GetDatum(i, tp)
		core.UpdateVariableVar(name, d, sessionVars)
	}
	return nil
}
