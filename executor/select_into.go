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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

// SelectIntoExec represents a SelectInto executor.
type SelectIntoExecCompressed struct {
	baseExecutor
	intoOpt  *ast.SelectIntoOption
	recordID uint64
}

type SuccessRes struct {
	TotalRow  uint64 `json:"total_rows"`
	TotalTime uint64 `json:"run_time"`
}

type SelectIntoTaskArgs struct {
	SelSQL     string `json:"sql"`
	FileName   string `json:"filename"`
	FieldTerm  string `json:"separator"`
	Enclosed   byte   `json:"delimiter"`
	Terminated string `json:"terminator"`
}

func (s *SelectIntoExecCompressed) Open(ctx context.Context) error {
	var err error
	s.recordID, err = s.addTask(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *SelectIntoExecCompressed) Next(ctx context.Context, req *chunk.Chunk) error {
	sql := new(strings.Builder)
	sql.Reset()
	sqlexec.MustFormatSQL(sql, "select TIMESTAMPDIFF(SECOND,live_time,now()),status,"+
		"error_message,response from  %n.%n where id=%?", mysql.SystemDB, mysql.TidbExternalTask, s.recordID)
	isFirstTime := true
	for {
		if isFirstTime {
			time.Sleep(time.Second * 1)
			isFirstTime = false
		} else {
			time.Sleep(time.Second * 5)
		}
		taskStatus, err := s.waitTaskEnd(ctx, sql)
		if err != nil {
			return err
		}
		if taskStatus {
			break
		}
	}
	return nil
}

func (s *SelectIntoExecCompressed) Close() error {

	return nil
}

func (s *SelectIntoExecCompressed) getRecordStatus(ctx context.Context,
	sqlExecutor sqlexec.SQLExecutor, sql *strings.Builder) (bool, error) {
	var lastLiveTimeSeconds int64
	var status string
	var response string
	var errmsg string

	rs, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		return false, err
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	req := rs.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	if err != nil {
		return false, err
	}
	if req.NumRows() == 0 {
		return false, fmt.Errorf("Export data task %d not found", s.recordID)
	}
	if req.NumRows() != 1 {
		return false, fmt.Errorf("Multiple export data task %d found", s.recordID)
	}
	row := iter.Begin()
	//get live_time
	if !row.IsNull(0) {
		lastLiveTimeSeconds = row.GetInt64(0)
	}
	//get status
	if !row.IsNull(1) {
		status = strings.ToLower(row.GetString(1))
	}
	// If no keepalive information is written within 1 minutes,
	//the task is considered to have timed out
	if lastLiveTimeSeconds > 60 && (!(status == "error" || status == "success")) {
		return true, fmt.Errorf("Export data task %d keep alive timed out", s.recordID)
	}
	//get errmsg
	if !row.IsNull(2) {
		errmsg = row.GetString(2)
	}
	if status == "error" {
		return true, fmt.Errorf("Export data fail ,%s", errmsg)
	} else if status == "success" {
		var sr SuccessRes
		if !row.IsNull(3) {
			response = row.GetString(3)
		}
		err = json.Unmarshal([]byte(response), &sr)
		if err != nil {
			return true, fmt.Errorf("Export data fail ,unmarshal response fail,%s", err.Error())
		}
		s.ctx.GetSessionVars().StmtCtx.AddAffectedRows(sr.TotalRow)
		return true, nil
	}
	return false, nil
}

func (s *SelectIntoExecCompressed) waitTaskEnd(ctx context.Context, sql *strings.Builder) (bool, error) {
	sysSession, err := s.getSysSession()
	if err != nil {
		return false, err
	}
	defer s.releaseSysSession(ctx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return false, err
	}
	taskStatus, err := s.getRecordStatus(ctx, sqlExecutor, sql)
	if err != nil {
		return false, err
	}
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return false, err
	}
	return taskStatus, nil
}

func (s *SelectIntoExecCompressed) generateArgs(ctx context.Context) (string, error) {
	sita := SelectIntoTaskArgs{
		SelSQL:     s.intoOpt.SelectSQL,
		FileName:   s.intoOpt.FileName,
		FieldTerm:  s.intoOpt.FieldsInfo.Terminated,
		Enclosed:   s.intoOpt.FieldsInfo.Enclosed,
		Terminated: s.intoOpt.LinesInfo.Terminated,
	}
	args, err := json.Marshal(sita)
	return util.String(args), err
}

func (s *SelectIntoExecCompressed) addTask(ctx context.Context) (uint64, error) {
	sysSession, err := s.getSysSession()
	if err != nil {
		return 0, err
	}
	defer s.releaseSysSession(ctx, sysSession)
	owner := config.GetGlobalConfig().AdvertiseAddress + ":" + strconv.Itoa(int(config.GetGlobalConfig().Port))
	args, err := s.generateArgs(ctx)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Error occur when generate select into  task args %s", err))
	}
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	if _, err := sqlExecutor.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return 0, err
	}
	sql := new(strings.Builder)
	sql.Reset()
	sqlexec.MustFormatSQL(sql, "insert into %n.%n "+
		" (command,owner,args) values ('dumpling',%?,%?)", mysql.SystemDB, mysql.TidbExternalTask, owner, args)
	if _, err := sqlExecutor.ExecuteInternal(ctx, sql.String()); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", sql))
		return 0, err
	}
	lastInsertID := sysSession.GetSessionVars().StmtCtx.LastInsertID
	if _, err := sqlExecutor.ExecuteInternal(ctx, "commit"); err != nil {
		return 0, err
	}
	return lastInsertID, nil
}

// SelectIntoExec represents a SelectInto executor.
type SelectIntoExec struct {
	baseExecutor
	intoOpt *ast.SelectIntoOption

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
	// only 'select ... into outfile' is supported now
	if s.intoOpt.Tp != ast.SelectIntoOutfile {
		return errors.New("unsupported SelectInto type")
	}

	// MySQL-compatible behavior: allow files to be group-readable
	f, err := os.OpenFile(s.intoOpt.FileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0640) // #nosec G302
	if err != nil {
		return errors.Trace(err)
	}
	s.started = true
	s.dstFile = f
	s.writer = bufio.NewWriter(s.dstFile)
	s.chk = tryNewCacheChunk(s.children[0])
	s.lineBuf = make([]byte, 0, 1024)
	s.fieldBuf = make([]byte, 0, 64)
	s.escapeBuf = make([]byte, 0, 64)
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

func (s *SelectIntoExec) considerEncloseOpt(et types.EvalType) bool {
	return et == types.ETString || et == types.ETDuration ||
		et == types.ETTimestamp || et == types.ETDatetime ||
		et == types.ETJson
}

func (s *SelectIntoExec) escapeField(f []byte) []byte {
	if s.intoOpt.FieldsInfo.Escaped == 0 {
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
		case b == s.intoOpt.FieldsInfo.Escaped || b == s.intoOpt.FieldsInfo.Enclosed:
			escape = true
		case !s.enclosed && len(s.intoOpt.FieldsInfo.Terminated) > 0 && b == s.intoOpt.FieldsInfo.Terminated[0]:
			// if field is enclosed, we only escape line terminator, otherwise both field and line terminator will be escaped
			escape = true
		case len(s.intoOpt.LinesInfo.Terminated) > 0 && b == s.intoOpt.LinesInfo.Terminated[0]:
			// we always escape line terminator
			escape = true
		}
		if escape {
			s.escapeBuf = append(s.escapeBuf, s.intoOpt.FieldsInfo.Escaped)
		}
		s.escapeBuf = append(s.escapeBuf, b)
	}
	return s.escapeBuf
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
	} else {
		nullTerm = []byte("NULL")
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
				s.enclosed = true
			} else {
				s.enclosed = false
			}
			s.fieldBuf = s.fieldBuf[:0]
			switch col.GetType().GetType() {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeYear:
				s.fieldBuf = strconv.AppendInt(s.fieldBuf, row.GetInt64(j), 10)
			case mysql.TypeLonglong:
				if mysql.HasUnsignedFlag(col.GetType().GetFlag()) {
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
				s.fieldBuf = append(s.fieldBuf, row.GetDuration(j, col.GetType().GetDecimal()).String()...)
			case mysql.TypeEnum:
				s.fieldBuf = append(s.fieldBuf, row.GetEnum(j).String()...)
			case mysql.TypeSet:
				s.fieldBuf = append(s.fieldBuf, row.GetSet(j).String()...)
			case mysql.TypeJSON:
				s.fieldBuf = append(s.fieldBuf, row.GetJSON(j).String()...)
			}

			switch col.GetType().EvalType() {
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
		s.lineBuf = append(s.lineBuf, lineTerm...)
		if _, err := s.writer.Write(s.lineBuf); err != nil {
			return errors.Trace(err)
		}
	}
	s.ctx.GetSessionVars().StmtCtx.AddAffectedRows(uint64(s.chk.NumRows()))
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
		return errors.Trace(err1)
	} else if err2 != nil {
		return errors.Trace(err2)
	}
	return err3
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
