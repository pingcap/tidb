package executor

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sem"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// parseAuditLogBatchSize is the batch size of audit-log lines for a worker to parse, exported for testing.
var parseAuditLogBatchSize = 64

const (
	auditLogTimeFormat = "2006/01/02 15:04:05.000 -07:00"

	auditLogTime         = "TIME"
	auditLogID           = "ID"
	auditLogEvent        = "EVENT"
	auditLogUser         = "USER"
	auditLogRoles        = "ROLES"
	auditLogConnectionID = "CONNECTION_ID"
	auditLogTables       = "TABLES"
	auditLogStatusCode   = "STATUS_CODE"
	auditLogReason       = "REASON"

	auditLogCurrentDB     = "CURRENT_DB"
	auditLogSQLText       = "SQL_TEXT"
	auditLogExecuteParams = "EXECUTE_PARAMS"
	auditLogAffectRows    = "AFFECTED_ROWS"

	auditLogConnectionType = "CONNECTION_TYPE"
	auditLogPid            = "PID"
	auditLogServerVersion  = "SERVER_VERSION"
	auditLogSSLVersion     = "SSL_VERSION"
	auditLogHostIP         = "HOST_IP"
	auditLogHostPort       = "HOST_PORT"
	auditLogClientIP       = "CLIENT_IP"
	auditLogClientPort     = "CLIENT_PORT"
	auditLogAuthMethod     = "AUTH_METHOD"
	auditLogConnAttrs      = "CONN_ATTRS"

	auditLogAuditOPTarget = "AUDIT_OP_TARGET"
	auditLogAuditOPArgs   = "AUDIT_OP_ARGS"

	auditLogInfo = "INFO"
)

type auditLog struct {
	Time         string `json:"TIME"`
	ID           string `json:"ID"`
	Event        string `json:"EVENT"`
	User         string `json:"USER"`
	Roles        string `json:"ROLES"`
	ConnectionID string `json:"CONNECTION_ID"`
	Tables       string `json:"TABLES"`
	StatusCode   string `json:"STATUS_CODE"`
	Reason       string `json:"REASON"`

	CurrentDB     string `json:"CURRENT_DB"`
	SQLTest       string `json:"SQL_TEXT"`
	ExecuteParams string `json:"EXECUTE_PARAMS"`
	AffectedRows  string `json:"AFFECTED_ROWS"`

	ConnectionType string `json:"CONNECTION_TYPE"`
	Pid            string `json:"PID"`
	ServerVersion  string `json:"SERVER_VERSION"`
	SslVersion     string `json:"SSL_VERSION"`
	HostIP         string `json:"HOST_IP"`
	HostPort       string `json:"HOST_PORT"`
	ClientIP       string `json:"CLIENT_IP"`
	ClientPort     string `json:"CLIENT_PORT"`
	AuthMethod     string `json:"AUTH_METHOD"`
	ConnAttrs      string `json:"CONN_ATTRS"`

	AuditOPTarget string `json:"AUDIT_OP_TARGET"`
	AuditOPArgs   string `json:"AUDIT_OP_ARGS"`

	Info string `json:"INFO"`
}

// auditLogRetriever is used to read audit log data.
type auditLogRetriever struct {
	table                 *model.TableInfo
	outputCols            []*model.ColumnInfo
	initialized           bool
	extractor             *plannercore.AuditLogExtractor
	files                 []logFile
	fileIdx               int
	fileLine              int
	checker               *auditLogChecker
	columnValueFactoryMap map[string]auditLogColumnValueFactory
	instanceFactory       func([]types.Datum)

	taskList      chan auditLogTask
	stats         *auditLogRuntimeStats
	memTracker    *memory.Tracker
	lastFetchSize int64
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	auditLog      string
	logType       string
}

func (e *auditLogRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if !e.initialized {
		err := e.initialize(ctx, sctx)
		if err != nil {
			return nil, err
		}
		ctx, e.cancel = context.WithCancel(ctx)
		e.initializeAsyncParsing(ctx, sctx)
	}
	m := privilege.GetPrivilegeManager(sctx)
	u := e.checker.user
	if u != nil && m != nil {
		priv := audit.PrivAuditAdmin
		if sem.IsEnabled() {
			priv = audit.PrivRestrictedAuditAdmin
		}
		if !m.RequestDynamicVerificationWithUser(priv, false, u) {
			return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(priv)
		}
	}
	return e.dataForAuditLog(ctx)
}

func (e *auditLogRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	var err error
	// initialize column value factories.
	e.columnValueFactoryMap = make(map[string]auditLogColumnValueFactory, len(e.outputCols))
	for idx, col := range e.outputCols {
		if col.Name.O == util.ClusterTableInstanceColumnName {
			e.instanceFactory, err = getInstanceColumnValueFactory(sctx, idx)
			if err != nil {
				return err
			}
			continue
		}
		factory, err := e.getColumnValueFactoryByNameAuditLog(col.Name.O, idx)
		if err != nil {
			return err
		}
		if factory == nil {
			panic(fmt.Sprintf("should never happen, should register new column %v into getColumnValueFactoryByNameAuditLog function", col.Name.O))
		}
		e.columnValueFactoryMap[col.Name.O] = factory
	}
	// initialize checker.
	e.checker = &auditLogChecker{user: sctx.GetSessionVars().User}
	e.stats = &auditLogRuntimeStats{}
	if e.extractor != nil {
		e.checker.enableTimeCheck = e.extractor.Enable
		for _, tr := range e.extractor.TimeRanges {
			startTime := types.NewTime(types.FromGoTime(tr.StartTime.In(sctx.GetSessionVars().Location())), mysql.TypeDatetime, types.MaxFsp)
			endTime := types.NewTime(types.FromGoTime(tr.EndTime.In(sctx.GetSessionVars().Location())), mysql.TypeDatetime, types.MaxFsp)
			timeRange := &timeRange{
				startTime: startTime,
				endTime:   endTime,
			}
			e.checker.timeRanges = append(e.checker.timeRanges, timeRange)
		}
	} else {
		e.extractor = &plannercore.AuditLogExtractor{}
	}
	e.initialized = true
	// todo : change to global var.
	var auditLogFileName string
	if strings.ToUpper(e.logType) == "JSON" {
		auditLogFileName = e.auditLog + ".json"
	} else {
		auditLogFileName = e.auditLog
	}
	e.files, err = e.getAllFiles(ctx, sctx, auditLogFileName)
	if e.extractor.Desc {
		slices.Reverse(e.files)
	}
	return err
}

func (e *auditLogRetriever) close() error {
	for _, f := range e.files {
		err := f.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close audit log file failed.", zap.Error(err))
		}
	}
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

type parsedAuditLog struct {
	rows [][]types.Datum
	err  error
}

func (e *auditLogRetriever) getNextFile() *logFile {
	if e.fileIdx >= len(e.files) {
		return nil
	}
	ret := &e.files[e.fileIdx]
	file := e.files[e.fileIdx].file
	e.fileIdx++
	if e.stats != nil {
		stat, err := file.Stat()
		if err == nil {
			// ignore the err will be ok.
			e.stats.readFileSize += stat.Size()
			e.stats.readFileNum++
		}
	}
	return ret
}

func (e *auditLogRetriever) getPreviousReader() (*bufio.Reader, error) {
	fileIdx := e.fileIdx
	// fileIdx refer to the next file which should be read
	// so we need to set fileIdx to fileIdx - 2 to get the previous file.
	fileIdx = fileIdx - 2
	if fileIdx < 0 {
		return nil, nil
	}
	file := e.files[fileIdx]
	_, err := file.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	var reader *bufio.Reader
	if !file.compressed {
		reader = bufio.NewReader(file.file)
	} else {
		gr, err := gzip.NewReader(file.file)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gr)
	}
	return reader, nil
}

func (e *auditLogRetriever) getNextReader() (*bufio.Reader, error) {
	file := e.getNextFile()
	if file == nil {
		return nil, nil
	}
	var reader *bufio.Reader
	if !file.compressed {
		reader = bufio.NewReader(file.file)
	} else {
		gr, err := gzip.NewReader(file.file)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gr)
	}
	return reader, nil
}

func (e *auditLogRetriever) parseDataForAuditLog(ctx context.Context, sctx sessionctx.Context) {
	defer e.wg.Done()
	reader, _ := e.getNextReader()
	if reader == nil {
		close(e.taskList)
		return
	}
	e.parseAuditLog(ctx, sctx, reader, parseAuditLogBatchSize)
}

func (e *auditLogRetriever) dataForAuditLog(ctx context.Context) ([][]types.Datum, error) {
	var (
		task auditLogTask
		ok   bool
	)
	e.memConsume(-e.lastFetchSize)
	e.lastFetchSize = 0
	for {
		select {
		case task, ok = <-e.taskList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if !ok {
			return nil, nil
		}
		result := <-task.resultCh
		rows, err := result.rows, result.err
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		if e.instanceFactory != nil {
			for i := range rows {
				e.instanceFactory(rows[i])
			}
		}
		e.lastFetchSize = calculateDatumsSize(rows)
		return rows, nil
	}
}

type auditLogChecker struct {
	// Below fields is used to check privileges
	user *auth.UserIdentity
	// Below fields is used to check audit log time valid.
	enableTimeCheck bool
	timeRanges      []*timeRange
}

func (sc *auditLogChecker) isTimeValid(t types.Time) bool {
	for _, tr := range sc.timeRanges {
		if sc.enableTimeCheck && (t.Compare(tr.startTime) >= 0 && t.Compare(tr.endTime) <= 0) {
			return true
		}
	}
	return !sc.enableTimeCheck
}

type auditLogTask struct {
	resultCh chan parsedAuditLog
}

type auditLogBlock []string

func (e *auditLogRetriever) getBatchLog(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	var line string
	log := make([]string, 0, num)
	var err error
	for i := 0; i < num; i++ {
		for {
			if isCtxDone(ctx) {
				return nil, ctx.Err()
			}
			e.fileLine++
			lineByte, err := getOneLine(reader)
			if err != nil {
				if err == io.EOF {
					e.fileLine = 0
					newReader, err := e.getNextReader()
					if newReader == nil || err != nil {
						return [][]string{log}, err
					}
					offset.length = len(log)
					reader.Reset(newReader)
					continue
				}
				return [][]string{log}, err
			}
			line = string(hack.String(lineByte))
			log = append(log, line)
		}
	}
	return [][]string{log}, err
}

func (e *auditLogRetriever) getBatchLogForReversedScan(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	// reader maybe change when read previous file.
	inputReader := reader
	defer func() {
		newReader, _ := e.getNextReader()
		if newReader != nil {
			inputReader.Reset(newReader)
		}
	}()
	var line string
	var logs []auditLogBlock
	var log []string
	var err error
	scanPreviousFile := false
	for {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		e.fileLine++
		lineByte, err := getOneLine(reader)
		if err != nil {
			if err == io.EOF {
				if len(log) == 0 {
					decomposedAuditLogTasks := decomposeToAuditLogTasks(logs, num)
					offset.length = len(decomposedAuditLogTasks)
					return decomposedAuditLogTasks, nil
				}
				e.fileLine = 0
				reader, err = e.getPreviousReader()
				if reader == nil || err != nil {
					return decomposeToAuditLogTasks(logs, num), nil
				}
				scanPreviousFile = true
				continue
			}
			return nil, err
		}
		line = string(hack.String(lineByte))
		log = append(log, line)

		logs = append(logs, log)
		if scanPreviousFile {
			break
		}
		log = make([]string, 0, 8)
	}
	return decomposeToAuditLogTasks(logs, num), err
}

func decomposeToAuditLogTasks(logs []auditLogBlock, num int) [][]string {
	if len(logs) == 0 {
		return nil
	}

	//In reversed scan, We should reverse the blocks.
	last := len(logs) - 1
	for i := 0; i < len(logs)/2; i++ {
		logs[i], logs[last-i] = logs[last-i], logs[i]
	}

	decomposedAuditLogTasks := make([][]string, 0)
	log := make([]string, 0, num*len(logs[0]))
	for i := range logs {
		log = append(log, logs[i]...)
		if i > 0 && i%num == 0 {
			decomposedAuditLogTasks = append(decomposedAuditLogTasks, log)
			log = make([]string, 0, len(log))
		}
	}
	if len(log) > 0 {
		decomposedAuditLogTasks = append(decomposedAuditLogTasks, log)
	}
	return decomposedAuditLogTasks
}

func (e *auditLogRetriever) parseAuditLog(ctx context.Context, sctx sessionctx.Context, reader *bufio.Reader, logNum int) {
	defer close(e.taskList)
	offset := offset{offset: 0, length: 0}
	// To limit the num of go routine
	concurrent := sctx.GetSessionVars().Concurrency.DistSQLScanConcurrency()
	ch := make(chan int, concurrent)
	if e.stats != nil {
		e.stats.concurrent = concurrent
	}
	defer close(ch)
	for {
		startTime := time.Now()
		var logs [][]string
		var err error
		if !e.extractor.Desc {
			logs, err = e.getBatchLog(ctx, reader, &offset, logNum)
		} else {
			logs, err = e.getBatchLogForReversedScan(ctx, reader, &offset, logNum)
		}
		if err != nil {
			t := auditLogTask{}
			t.resultCh = make(chan parsedAuditLog, 1)
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.sendParsedAuditLogCh(t, parsedAuditLog{nil, err})
		}
		if len(logs) == 0 || len(logs[0]) == 0 {
			break
		}
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		failpoint.Inject("mockReadAuditLogAudit", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value(signalsKey{}).([]chan int)
				signals[0] <- 1
				<-signals[1]
			}
		})
		for i := range logs {
			log := logs[i]
			t := auditLogTask{}
			t.resultCh = make(chan parsedAuditLog, 1)
			start := offset
			ch <- 1
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				result, err := e.parseLog(ctx, sctx, log, start)
				e.sendParsedAuditLogCh(t, parsedAuditLog{result, err})
				<-ch
			}()
			offset.offset = e.fileLine
			offset.length = 0
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (e *auditLogRetriever) sendParsedAuditLogCh(t auditLogTask, re parsedAuditLog) {
	select {
	case t.resultCh <- re:
	default:
		return
	}
}

func concatParts(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, "=")
}

func (e *auditLogRetriever) getRowText(line string, al *auditLog) {
	fields := strings.Split(line[1:len(line)-1], "] [")
	al.Time = fields[0]

	for i := 1; i < len(fields); i++ {
		parts := strings.Split(fields[i], "=")
		if len(parts) > 1 {
			switch parts[0] {
			case auditLogID:
				al.ID = concatParts(parts[1:])
			case auditLogEvent:
				al.Event = strings.TrimSuffix(strings.TrimPrefix(concatParts(parts[1:]), "\"["), "]\"")
			case auditLogUser:
				al.User = concatParts(parts[1:])
			case auditLogRoles:
				al.Roles = strings.TrimSuffix(strings.TrimPrefix(concatParts(parts[1:]), "\"["), "]\"")
			case auditLogConnectionID:
				al.ConnectionID = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogTables:
				al.Tables = strings.TrimSuffix(strings.TrimPrefix(concatParts(parts[1:]), "\"["), "]\"")
			case auditLogStatusCode:
				al.StatusCode = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogReason:
				al.Reason = strings.Trim(concatParts(parts[1:]), "\"")
			case auditLogCurrentDB:
				al.CurrentDB = concatParts(parts[1:])
			case auditLogSQLText:
				al.SQLTest = strings.Trim(concatParts(parts[1:]), "\"")
			case auditLogExecuteParams:
				al.ExecuteParams = strings.TrimSuffix(strings.TrimPrefix(concatParts(parts[1:]), "\"["), "]\"")
			case auditLogAffectRows:
				al.AffectedRows = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogConnectionType:
				al.ConnectionType = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogPid:
				al.Pid = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogServerVersion:
				al.ServerVersion = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogSSLVersion:
				al.SslVersion = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogHostIP:
				al.HostIP = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogHostPort:
				al.HostPort = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogClientIP:
				al.ClientIP = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogClientPort:
				al.ClientPort = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogAuthMethod:
				al.AuthMethod = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogConnAttrs:
				al.ConnAttrs = strings.Trim(concatParts(parts[1:]), "\"")
			case auditLogAuditOPTarget:
				al.AuditOPTarget = strings.TrimSpace(concatParts(parts[1:]))
			case auditLogAuditOPArgs:
				al.AuditOPArgs = strings.TrimSuffix(strings.TrimPrefix(concatParts(parts[1:]), "\"["), "]\"")
			case auditLogInfo:
				al.Info = concatParts(parts[1:])
			}
		} else if fields[i] != "INFO" {
			logutil.BgLogger().Warn("Invalid audit log record", zap.String("record", fields[i]))
		}
	}
}

func (e *auditLogRetriever) getRowJSON(line string, al *auditLog) {
	err := json.Unmarshal([]byte(line), al)
	if err != nil {
		logutil.BgLogger().Warn("parse audit log panic", zap.Error(err), zap.String("stack", line))
	}
}

func (e *auditLogRetriever) setRowValues(sctx sessionctx.Context, al *auditLog, fileLine int) []types.Datum {
	row := make([]types.Datum, len(e.outputCols))
	tz := sctx.GetSessionVars().Location()

	fields := []struct {
		field string
		value string
	}{
		// General Information
		{field: auditLogTime, value: al.Time},
		{field: auditLogID, value: al.ID},
		{field: auditLogEvent, value: al.Event},
		{field: auditLogUser, value: al.User},
		{field: auditLogRoles, value: al.Roles},
		{field: auditLogConnectionID, value: al.ConnectionID},
		{field: auditLogTables, value: al.Tables},
		{field: auditLogStatusCode, value: al.StatusCode},
		{field: auditLogReason, value: al.Reason},

		// SQL Information
		{field: auditLogCurrentDB, value: al.CurrentDB},
		{field: auditLogSQLText, value: al.SQLTest},
		{field: auditLogExecuteParams, value: al.ExecuteParams},
		{field: auditLogAffectRows, value: al.AffectedRows},

		// Connection Information
		{field: auditLogConnectionType, value: al.ConnectionType},
		{field: auditLogPid, value: al.Pid},
		{field: auditLogServerVersion, value: al.ServerVersion},
		{field: auditLogSSLVersion, value: al.SslVersion},
		{field: auditLogHostIP, value: al.HostIP},
		{field: auditLogHostPort, value: al.HostPort},
		{field: auditLogClientIP, value: al.ClientIP},
		{field: auditLogClientPort, value: al.ClientPort},
		{field: auditLogAuthMethod, value: al.AuthMethod},
		{field: auditLogConnAttrs, value: al.ConnAttrs},

		// Audit Information
		{field: auditLogAuditOPTarget, value: al.AuditOPTarget},
		{field: auditLogAuditOPArgs, value: al.AuditOPArgs},

		// Security Information
		{field: auditLogInfo, value: al.Info},
	}

	for _, field := range fields {
		if valid := e.setColumnValue(sctx, row, tz, field.field, field.value, e.checker, fileLine); !valid {
			return nil
		}
	}
	return row
}

func (e *auditLogRetriever) parseLog(ctx context.Context, sctx sessionctx.Context, log []string, offset offset) (data [][]types.Datum, err error) {
	start := time.Now()
	logSize := calculateLogSize(log)
	defer e.memConsume(-logSize)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Warn("parse audit log panic", zap.Error(err), zap.String("stack", string(buf)))
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.parseLog, int64(time.Since(start)))
		}
	}()
	e.memConsume(logSize)
	failpoint.Inject("errorMockParseAuditLogPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	var row []types.Datum
	for index, line := range log {
		var al = &auditLog{}
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		fileLine := getLineIndex(offset, index)
		if strings.ToUpper(e.logType) == "JSON" {
			e.getRowJSON(line, al)
		} else {
			e.getRowText(line, al)
		}
		row = e.setRowValues(sctx, al, fileLine)
		if row != nil {
			data = append(data, row)
			e.setDefaultValue(row)
			e.memConsume(types.EstimatedMemUsage(row, 1))
		}
	}
	return data, nil
}

func (e *auditLogRetriever) setColumnValue(sctx sessionctx.Context, row []types.Datum, tz *time.Location, field, value string, checker *auditLogChecker, lineNum int) bool {
	factory := e.columnValueFactoryMap[field]
	if factory == nil {
		// Fix issue 34320, when aduit log time is not in the output columns, the time filter condition is mistakenly discard.
		if field == auditLogTime && checker != nil {
			t, err := time.Parse(auditLogTimeFormat, value)
			if err != nil {
				err = fmt.Errorf("Parse audit log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
				sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				return false
			}
			timeValue := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
			return checker.isTimeValid(timeValue)
		}
		return true
	}
	valid, err := factory(row, value, tz, checker)
	if err != nil {
		err = fmt.Errorf("Parse audit log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return true
	}
	return valid
}

func (e *auditLogRetriever) setDefaultValue(row []types.Datum) {
	for i := range row {
		if !row[i].IsNull() {
			continue
		}
		row[i] = table.GetZeroValue(e.outputCols[i])
	}
}

type auditLogColumnValueFactory func(row []types.Datum, value string, tz *time.Location, checker *auditLogChecker) (valid bool, err error)

func (e *auditLogRetriever) getColumnValueFactoryByNameAuditLog(colName string, columnIdx int) (auditLogColumnValueFactory, error) {
	switch colName {
	case auditLogTime:
		return func(row []types.Datum, value string, tz *time.Location, checker *auditLogChecker) (bool, error) {
			t, err := time.Parse(auditLogTimeFormat, value)
			if err != nil {
				return false, err
			}
			timeValue := types.NewTime(types.FromGoTime(t.In(tz)), mysql.TypeTimestamp, types.MaxFsp)
			if checker != nil {
				valid := checker.isTimeValid(timeValue)
				if !valid {
					return valid, nil
				}
			}
			row[columnIdx] = types.NewTimeDatum(timeValue)
			return true, nil
		}, nil
	case auditLogConnectionID, auditLogStatusCode, auditLogPid, auditLogHostPort, auditLogClientPort, auditLogAffectRows, auditLogID, auditLogConnectionType, auditLogServerVersion, auditLogSSLVersion, auditLogHostIP, auditLogInfo,
		auditLogAuthMethod, auditLogConnAttrs, auditLogClientIP, auditLogEvent, auditLogUser, auditLogRoles,
		auditLogCurrentDB, auditLogTables, auditLogSQLText, auditLogExecuteParams, auditLogReason, auditLogAuditOPArgs, auditLogAuditOPTarget:
		return func(row []types.Datum, value string, tz *time.Location, checker *auditLogChecker) (valid bool, err error) {
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	default:
		return func(row []types.Datum, value string, tz *time.Location, checker *auditLogChecker) (valid bool, err error) {
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewUintDatum(v)
			return true, nil
		}, nil
	}
}

// getAllFiles is used to get all audit-log needed to parse, it is exported for test.
func (e *auditLogRetriever) getAllFiles(ctx context.Context, sctx sessionctx.Context, logFilePath string) ([]logFile, error) {
	totalFileNum := 0
	if e.stats != nil {
		startTime := time.Now()
		defer func() {
			e.stats.initialize = time.Since(startTime)
			e.stats.totalFileNum = totalFileNum
		}()
	}
	var logFiles []logFile
	logDir := filepath.Dir(logFilePath)
	ext := filepath.Ext(logFilePath)
	prefix := logFilePath[:len(logFilePath)-len(ext)]
	handleErr := func(err error) error {
		// Ignore the error and append warning for usability.
		if err != io.EOF {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	walkFn := func(path string, info os.DirEntry) error {
		if info.IsDir() {
			return nil
		}
		// All rotated log files have the same prefix with the original file.
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		compressed := strings.HasSuffix(path, ".gz")
		if isCtxDone(ctx) {
			return ctx.Err()
		}
		totalFileNum++
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return handleErr(err)
		}
		skip := false
		defer func() {
			if !skip {
				terror.Log(file.Close())
			}
		}()
		// Get the file start time.
		fileStartTime, err := e.getFileStartTime(ctx, file, compressed)
		if err != nil {
			return handleErr(err)
		}
		start := types.NewTime(types.FromGoTime(fileStartTime), mysql.TypeDatetime, types.MaxFsp)
		if e.checker.enableTimeCheck {
			notInAllTimeRanges := true
			for _, tr := range e.checker.timeRanges {
				if start.Compare(tr.endTime) <= 0 {
					notInAllTimeRanges = false
					break
				}
			}
			if notInAllTimeRanges {
				return nil
			}
		}

		// If we want to get the end time from a compressed file,
		// we need uncompress the whole file which is very slow and consume a lot of memory.
		if !compressed {
			// Get the file end time.
			fileEndTime, err := e.getFileEndTime(ctx, file)
			if err != nil {
				return handleErr(err)
			}
			if e.checker.enableTimeCheck {
				end := types.NewTime(types.FromGoTime(fileEndTime), mysql.TypeDatetime, types.MaxFsp)
				inTimeRanges := false
				for _, tr := range e.checker.timeRanges {
					if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
						inTimeRanges = true
						break
					}
				}
				if !inTimeRanges {
					return nil
				}
			}
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:       file,
			start:      types.NewTime(types.FromGoTime(fileStartTime), mysql.TypeDatetime, types.MaxFsp),
			compressed: compressed,
		})
		skip = true
		return nil
	}
	for _, file := range files {
		err := walkFn(filepath.Join(logDir, file.Name()), file)
		if err != nil {
			return nil, err
		}
	}
	// Sort by start time
	slices.SortFunc(logFiles, func(i, j logFile) int {
		return i.start.Compare(j.start)
	})
	// Assume no time range overlap in log files and remove unnecessary log files for compressed files.
	var ret []logFile
	for i, file := range logFiles {
		if i == len(logFiles)-1 || !file.compressed || !e.checker.enableTimeCheck {
			ret = append(ret, file)
			continue
		}
		start := logFiles[i].start
		// use next file.start as endTime
		end := logFiles[i+1].start
		inTimeRanges := false
		for _, tr := range e.checker.timeRanges {
			if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
				inTimeRanges = true
				break
			}
		}
		if inTimeRanges {
			ret = append(ret, file)
		}
	}
	return ret, err
}

func (*auditLogRetriever) getFileStartTime(ctx context.Context, file *os.File, compressed bool) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	var reader *bufio.Reader
	if !compressed {
		reader = bufio.NewReader(file)
	} else {
		gr, err := gzip.NewReader(file)
		if err != nil {
			return t, err
		}
		reader = bufio.NewReader(gr)
	}
	lineByte, err := getOneLine(reader)
	if err != nil {
		return t, err
	}
	line := string(lineByte)

	fields := strings.Split(line, "] [")
	if len(fields) <= 0 || len(fields[0]) <= 10 {
		return t, errors.Errorf("malform audit query file %v", file.Name())
	}
	return parseTime(string([]byte(fields[0])[1:]))
}

func (e *auditLogRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return e.stats
}

type auditLogRuntimeStats struct {
	totalFileNum int
	readFileNum  int
	readFile     time.Duration
	initialize   time.Duration
	readFileSize int64
	parseLog     int64
	concurrent   int
}

// String implements the RuntimeStats interface.
func (s *auditLogRuntimeStats) String() string {
	return fmt.Sprintf("initialize: %s, read_file: %s, parse_log: {time:%s, concurrency:%v}, total_file: %v, read_file: %v, read_size: %s",
		execdetails.FormatDuration(s.initialize), execdetails.FormatDuration(s.readFile),
		execdetails.FormatDuration(time.Duration(s.parseLog)), s.concurrent,
		s.totalFileNum, s.readFileNum, memory.FormatBytes(s.readFileSize))
}

// Merge implements the RuntimeStats interface.
func (s *auditLogRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*auditLogRuntimeStats)
	if !ok {
		return
	}
	s.totalFileNum += tmp.totalFileNum
	s.readFileNum += tmp.readFileNum
	s.readFile += tmp.readFile
	s.initialize += tmp.initialize
	s.readFileSize += tmp.readFileSize
	s.parseLog += tmp.parseLog
}

// Clone implements the RuntimeStats interface.
func (s *auditLogRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := *s
	return &newRs
}

// Tp implements the RuntimeStats interface.
func (s *auditLogRuntimeStats) Tp() int {
	return execdetails.TpAuditLogRuntimeStat
}

func (e *auditLogRetriever) getFileEndTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time

	stat, err := file.Stat()
	if err != nil {
		return t, err
	}
	endCursor := stat.Size()

	lines, readBytes, err := readLastLines(ctx, file, endCursor)
	if err != nil {
		return t, err
	}
	// read out the file
	if readBytes == 0 {
		return t, errors.Errorf("invalid audit log file %v", file.Name())
	}

	line := lines[len(lines)-1]
	if len(line) == 0 && len(lines) >= 2 {
		line = lines[len(lines)-2]
	}

	fields := strings.Split(line, "] [")
	if len(fields) <= 0 || len(fields[0]) <= 10 {
		return t, errors.Errorf("malform audit query file %v", file.Name())
	}
	return parseTime(string([]byte(fields[0])[1:]))
}

func (e *auditLogRetriever) initializeAsyncParsing(ctx context.Context, sctx sessionctx.Context) {
	e.taskList = make(chan auditLogTask, 1)
	e.wg.Add(1)
	go e.parseDataForAuditLog(ctx, sctx)
}

func (e *auditLogRetriever) memConsume(bytes int64) {
	if e.memTracker != nil {
		e.memTracker.Consume(bytes)
	}
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(auditLogTimeFormat, s)
	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
	}
	return t, err
}
