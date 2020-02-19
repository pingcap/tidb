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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// SelectIntoExec represents a SelectInto executor.
type SelectIntoExec struct {
	baseExecutor
	intoOpt *ast.SelectIntoOption

	lineBuf []byte
	realBuf []byte
	writer  *bufio.Writer
	dstFile *os.File
	chk     *chunk.Chunk
	started bool
}

// Open implements the Executor Open interface.
func (s *SelectIntoExec) Open(ctx context.Context) error {
	// only 'select ... into outfile' is supported now
	if s.intoOpt.Tp != ast.SelectIntoOutfile {
		return errors.New("unsupported SelectInto type")
	}

	f, err := os.Create(s.intoOpt.FileName)
	if err != nil {
		return err
	}
	s.started = true
	s.dstFile = f
	s.writer = bufio.NewWriter(s.dstFile)
	s.chk = newFirstChunk(s.children[0])
	s.lineBuf = make([]byte, 0, 1024)
	return s.baseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (s *SelectIntoExec) Next(ctx context.Context, req *chunk.Chunk) error {
	for {
		if err := Next(ctx, s.children[0], s.chk); err != nil {
			return err
		}
		if s.chk.NumRows() == 0 {
			break
		}
		if err := s.dumpToOutfile(); err != nil {
			return err
		}
	}
	return nil
}

var (
	expFormatBig   = 1e15
	expFormatSmall = 1e-15
)

func (s *SelectIntoExec) considerEncloseOpt(et types.EvalType) bool {
	return et == types.ETString || et == types.ETDuration ||
		et == types.ETTimestamp || et == types.ETDatetime ||
		et == types.ETJson
}

func (s *SelectIntoExec) dumpToOutfile() error {
	lineTerm := "\n"
	if s.intoOpt.LinesInfo.Terminated != "" {
		lineTerm = s.intoOpt.LinesInfo.Terminated
	}
	fieldTerm := "\t"
	if s.intoOpt.FieldsInfo.Terminated != "" {
		fieldTerm = s.intoOpt.FieldsInfo.Terminated
	}
	encloseFlag := false
	var encloseByte byte
	encloseOpt := false
	if s.intoOpt.FieldsInfo.Enclosed != byte(0) {
		encloseByte = s.intoOpt.FieldsInfo.Enclosed
		encloseFlag = true
		encloseOpt = s.intoOpt.FieldsInfo.OptEnclosed
	}
	nullTerm := []byte("\\N")
	if s.intoOpt.FieldsInfo.Escaped != byte(0) {
		nullTerm[0] = s.intoOpt.FieldsInfo.Escaped
	}

	cols := s.children[0].Schema().Columns
	for i := 0; i < s.chk.NumRows(); i++ {
		row := s.chk.GetRow(i)
		s.lineBuf = s.lineBuf[:0]
		for j, col := range cols {
			if j != 0 {
				s.lineBuf = append(s.lineBuf, fieldTerm...)
			}
			if row.IsNull(j) {
				s.lineBuf = append(s.lineBuf, nullTerm...)
				continue
			}
			et := col.GetType().EvalType()
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
			}
			switch col.GetType().EvalType() {
			case types.ETInt:
				if mysql.HasUnsignedFlag(col.GetType().Flag) {
					s.lineBuf = strconv.AppendUint(s.lineBuf, row.GetUint64(j), 10)
				} else {
					s.lineBuf = strconv.AppendInt(s.lineBuf, row.GetInt64(j), 10)
				}
			case types.ETReal:
				prec := types.UnspecifiedLength
				if col.RetType.Decimal > 0 && int(col.RetType.Decimal) != mysql.NotFixedDec {
					prec = int(col.RetType.Decimal)
				}
				v := row.GetFloat64(j)
				absV := math.Abs(v)
				if prec == types.UnspecifiedLength && (absV >= expFormatBig || (absV != 0 && absV < expFormatSmall)) {
					s.realBuf = strconv.AppendFloat(s.realBuf[:0], v, 'e', prec, 64)
					if idx := bytes.IndexByte(s.realBuf, '+'); idx != -1 {
						s.lineBuf = append(s.lineBuf, s.realBuf[:idx]...)
						s.lineBuf = append(s.lineBuf, s.realBuf[idx+1:]...)
					} else {
						s.lineBuf = append(s.lineBuf, s.realBuf...)
					}
				} else {
					s.lineBuf = strconv.AppendFloat(s.lineBuf, v, 'f', prec, 64)
				}
			case types.ETDecimal:
				s.lineBuf = append(s.lineBuf, row.GetMyDecimal(j).String()...)
			case types.ETString:
				s.lineBuf = append(s.lineBuf, row.GetBytes(j)...)
			case types.ETDatetime:
				s.lineBuf = append(s.lineBuf, row.GetTime(j).String()...)
			case types.ETTimestamp:
				s.lineBuf = append(s.lineBuf, row.GetTime(j).String()...)
			case types.ETDuration:
				s.lineBuf = append(s.lineBuf, row.GetDuration(j, col.GetType().Decimal).String()...)
			case types.ETJson:
				s.lineBuf = append(s.lineBuf, row.GetJSON(j).String()...)
			}
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
			}
		}
		s.lineBuf = append(s.lineBuf, lineTerm...)
		if _, err := s.writer.Write(s.lineBuf); err != nil {
			return err
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (s *SelectIntoExec) Close() error {
	if !s.started {
		return nil
	}
	err1 := s.writer.Flush()
	err2 := s.dstFile.Close()
	err3 := s.baseExecutor.Close()
	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}
	return err3
}
