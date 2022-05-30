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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func parseLog(retriever *slowQueryRetriever, sctx sessionctx.Context, reader *bufio.Reader) ([][]types.Datum, error) {
	retriever.taskList = make(chan slowLogTask, 100)
	ctx := context.Background()
	retriever.parseSlowLog(ctx, sctx, reader, 64)
	task, ok := <-retriever.taskList
	if !ok {
		return nil, nil
	}
	var rows [][]types.Datum
	var err error
	result := <-task.resultCh
	rows, err = result.rows, result.err
	return rows, err
}

func newSlowQueryRetriever() (*slowQueryRetriever, error) {
	newISBuilder, err := infoschema.NewBuilder(nil, nil).InitWithDBInfos(nil, nil, 0)
	if err != nil {
		return nil, err
	}
	is := newISBuilder.Build()
	tbl, err := is.TableByName(util.InformationSchemaName, model.NewCIStr(infoschema.TableSlowQuery))
	if err != nil {
		return nil, err
	}
	return &slowQueryRetriever{outputCols: tbl.Meta().Columns}, nil
}

func parseSlowLog(sctx sessionctx.Context, reader *bufio.Reader) ([][]types.Datum, error) {
	retriever, err := newSlowQueryRetriever()
	if err != nil {
		return nil, err
	}
	// Ignore the error is ok for test.
	terror.Log(retriever.initialize(context.Background(), sctx))
	rows, err := parseLog(retriever, sctx, reader)
	return rows, err
}

func TestParseSlowLogPanic(t *testing.T) {
	slowLogStr :=
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# User@Host: root[root] @ localhost [127.0.0.1]
# Query_time: 0.216905
# Cop_time: 0.38 Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Plan_from_binding: true
# Succ: false
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 1;
use test;
select * from t;`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/errorMockParseSlowLogPanic", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/errorMockParseSlowLogPanic"))
	}()
	reader := bufio.NewReader(bytes.NewBufferString(slowLogStr))
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	sctx := mock.NewContext()
	sctx.GetSessionVars().TimeZone = loc
	_, err = parseSlowLog(sctx, reader)
	require.Error(t, err)
	require.Equal(t, err.Error(), "panic test")
}

func TestParseSlowLogFile(t *testing.T) {
	slowLogStr :=
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# User@Host: root[root] @ localhost [127.0.0.1]
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 0.216905
# Cop_time: 0.38 Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Rocksdb_delete_skipped_count: 10 Rocksdb_key_skipped_count: 10 Rocksdb_block_cache_hit_count: 10 Rocksdb_block_read_count: 10 Rocksdb_block_read_byte: 100
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2
# Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2
# Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Plan_from_binding: true
# Succ: false
# IsExplicitTxn: true
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 1;
use test;
select * from t;`
	reader := bufio.NewReader(bytes.NewBufferString(slowLogStr))
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	ctx := mock.NewContext()
	ctx.GetSessionVars().TimeZone = loc
	rows, err := parseSlowLog(ctx, reader)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	recordString := ""
	for i, value := range rows[0] {
		str, err := value.ToString()
		require.NoError(t, err)
		if i > 0 {
			recordString += ","
		}
		recordString += str
	}
	expectRecordString := `2019-04-28 15:24:04.309074,` +
		`405888132465033227,root,localhost,0,57,0.12,0.216905,` +
		`0,0,0,0,0,0,0,0,0,0,0,0,,0,0,0,0,0,0,0.38,0.021,0,0,0,1,637,0,10,10,10,10,100,,,1,42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772,t1:1,t2:2,` +
		`0.1,0.2,0.03,127.0.0.1:20160,0.05,0.6,0.8,0.0.0.0:20160,70724,65536,0,0,0,0,0,` +
		`Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2 Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2 Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2,` +
		`0,0,1,0,1,1,0,,60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4,` +
		`update t set i = 1;,select * from t;`
	require.Equal(t, expectRecordString, recordString)

	// Issue 20928
	reader = bufio.NewReader(bytes.NewBufferString(slowLogStr))
	rows, err = parseSlowLog(ctx, reader)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	recordString = ""
	for i, value := range rows[0] {
		str, err := value.ToString()
		require.NoError(t, err)
		if i > 0 {
			recordString += ","
		}
		recordString += str
	}
	expectRecordString = `2019-04-28 15:24:04.309074,` +
		`405888132465033227,root,localhost,0,57,0.12,0.216905,` +
		`0,0,0,0,0,0,0,0,0,0,0,0,,0,0,0,0,0,0,0.38,0.021,0,0,0,1,637,0,10,10,10,10,100,,,1,42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772,t1:1,t2:2,` +
		`0.1,0.2,0.03,127.0.0.1:20160,0.05,0.6,0.8,0.0.0.0:20160,70724,65536,0,0,0,0,0,` +
		`Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2 Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2 Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2,` +
		`0,0,1,0,1,1,0,,60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4,` +
		`update t set i = 1;,select * from t;`
	require.Equal(t, expectRecordString, recordString)

	// fix sql contain '# ' bug
	slowLog := bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
select a# from t;
# Time: 2019-01-24T22:32:29.313255+08:00
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Succ: false
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	require.NoError(t, err)

	// test for time format compatibility.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2019-04-24-19:41:21.716221 +0800
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	rows, err = parseSlowLog(ctx, reader)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	t0Str, err := rows[0][0].ToString()
	require.NoError(t, err)
	require.Equal(t, t0Str, "2019-04-28 15:24:04.309074")
	t1Str, err := rows[1][0].ToString()
	require.NoError(t, err)
	require.Equal(t, t1Str, "2019-04-24 19:41:21.716221")

	// Add parse error check.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Succ: abc
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	require.NoError(t, err)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, warnings[0].Err.Error(), "Parse slow log at line 2, failed field is Succ, failed value is abc, error is strconv.ParseBool: parsing \"abc\": invalid syntax")
}

// It changes variable.MaxOfMaxAllowedPacket, so must be stayed in SerialSuite.
func TestParseSlowLogFileSerial(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	ctx := mock.NewContext()
	ctx.GetSessionVars().TimeZone = loc
	// test for bufio.Scanner: token too long.
	slowLog := bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2019-04-24-19:41:21.716221 +0800
`)
	originValue := variable.MaxOfMaxAllowedPacket
	variable.MaxOfMaxAllowedPacket = 65536
	sql := strings.Repeat("x", int(variable.MaxOfMaxAllowedPacket+1))
	slowLog.WriteString(sql)
	reader := bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	require.Error(t, err)
	require.EqualError(t, err, "single line length exceeds limit: 65536")

	variable.MaxOfMaxAllowedPacket = originValue
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	require.NoError(t, err)
}

func TestSlowLogParseTime(t *testing.T) {
	t1Str := "2019-01-24T22:32:29.313255+08:00"
	t2Str := "2019-01-24T22:32:29.313255"
	t1, err := ParseTime(t1Str)
	require.NoError(t, err)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	t2, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", t2Str, loc)
	require.NoError(t, err)
	require.Equal(t, t1.Unix(), t2.Unix())
	t1Format := t1.In(loc).Format(logutil.SlowLogTimeFormat)
	require.Equal(t, t1Format, t1Str)
}

// TestFixParseSlowLogFile bugfix
// sql select * from INFORMATION_SCHEMA.SLOW_QUERY limit 1;
// ERROR 1105 (HY000): string "2019-05-12-11:23:29.61474688" doesn't has a prefix that matches format "2006-01-02-15:04:05.999999999 -0700", err: parsing time "2019-05-12-11:23:29.61474688" as "2006-01-02-15:04:05.999999999 -0700": cannot parse "" as "-0700"
func TestFixParseSlowLogFile(t *testing.T) {
	slowLog := bytes.NewBufferString(
		`# Time: 2019-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8
# Mem_max: 70724
select * from t
# Time: 2019-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8
# Mem_max: 70724
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
select * from t;`)
	scanner := bufio.NewReader(slowLog)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	ctx := mock.NewContext()
	ctx.GetSessionVars().TimeZone = loc
	_, err = parseSlowLog(ctx, scanner)
	require.NoError(t, err)

	// Test parser error.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227#
select * from t;
`)
	scanner = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, scanner)
	require.NoError(t, err)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, warnings[0].Err.Error(), "Parse slow log at line 2, failed field is Txn_start_ts, failed value is 405888132465033227#, error is strconv.ParseUint: parsing \"405888132465033227#\": invalid syntax")
}

func TestSlowQueryRetriever(t *testing.T) {
	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;`
	logData2 := `
# Time: 2020-02-16T18:00:01.000000+08:00
select 3;
# Time: 2020-02-16T18:00:05.000000+08:00
select 4;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 5;
# Time: 2020-02-17T18:00:05.000000+08:00
select 6;
# Time: 2020-04-15T18:00:05.299063744+08:00
select 7;`
	logData := []string{logData0, logData1, logData2, logData3}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3}
	prepareLogs(t, logData, fileNames)
	defer func() {
		removeFiles(fileNames)
	}()

	cases := []struct {
		startTime string
		endTime   string
		files     []string
		querys    []string
	}{
		{
			startTime: "2020-02-15T18:00:00.000000+08:00",
			endTime:   "2020-02-17T20:00:00.000000+08:00",
			files:     []string{fileName1, fileName2, fileName3},
			querys: []string{
				"select 1;",
				"select 2;",
				"select 3;",
				"select 4;",
				"select 5;",
				"select 6;",
			},
		},
		{
			startTime: "2020-02-15T18:00:02.000000+08:00",
			endTime:   "2020-02-16T20:00:00.000000+08:00",
			files:     []string{fileName1, fileName2, fileName3},
			querys: []string{
				"select 2;",
				"select 3;",
				"select 4;",
				"select 5;",
			},
		},
		{
			startTime: "2020-02-16T18:00:03.000000+08:00",
			endTime:   "2020-02-16T18:59:00.000000+08:00",
			files:     []string{fileName2},
			querys: []string{
				"select 4;",
			},
		},
		{
			startTime: "2020-02-16T18:00:03.000000+08:00",
			endTime:   "2020-02-16T20:00:00.000000+08:00",
			files:     []string{fileName2, fileName3},
			querys: []string{
				"select 4;",
				"select 5;",
			},
		},
		{
			startTime: "2020-02-16T19:00:00.000000+08:00",
			endTime:   "2020-02-17T17:00:00.000000+08:00",
			files:     []string{fileName3},
			querys: []string{
				"select 5;",
			},
		},
		{
			startTime: "2010-01-01T00:00:00.000000+08:00",
			endTime:   "2010-01-01T01:00:00.000000+08:00",
			files:     []string{},
		},
		{
			startTime: "2020-03-01T00:00:00.000000+08:00",
			endTime:   "2010-03-01T01:00:00.000000+08:00",
			files:     []string{},
		},
		{
			startTime: "",
			endTime:   "",
			files:     []string{fileName3},
			querys: []string{
				"select 5;",
				"select 6;",
				"select 7;",
			},
		},
		{
			startTime: "2020-04-15T18:00:05.299063744+08:00",
			endTime:   "2020-04-15T18:00:05.299063744+08:00",
			files:     []string{fileName3},
			querys: []string{
				"select 7;",
			},
		},
	}

	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	sctx := mock.NewContext()
	sctx.GetSessionVars().TimeZone = loc
	sctx.GetSessionVars().SlowQueryFile = fileName3
	for i, cas := range cases {
		extractor := &plannercore.SlowQueryExtractor{Enable: len(cas.startTime) > 0 && len(cas.endTime) > 0}
		if extractor.Enable {
			startTime, err := ParseTime(cas.startTime)
			require.NoError(t, err)
			endTime, err := ParseTime(cas.endTime)
			require.NoError(t, err)
			extractor.TimeRanges = []*plannercore.TimeRange{{StartTime: startTime, EndTime: endTime}}
		}
		retriever, err := newSlowQueryRetriever()
		require.NoError(t, err)
		retriever.extractor = extractor
		err = retriever.initialize(context.Background(), sctx)
		require.NoError(t, err)
		comment := fmt.Sprintf("case id: %v", i)
		require.Equal(t, len(retriever.files), len(cas.files), comment)
		if len(retriever.files) > 0 {
			reader := bufio.NewReader(retriever.files[0].file)
			rows, err := parseLog(retriever, sctx, reader)
			require.NoError(t, err)
			require.Equal(t, len(rows), len(cas.querys), comment)
			for i, row := range rows {
				require.Equal(t, row[len(row)-1].GetString(), cas.querys[i], comment)
			}
		}

		for i, file := range retriever.files {
			require.Equal(t, file.file.Name(), cas.files[i])
			require.NoError(t, file.file.Close())
		}
		require.NoError(t, retriever.close())
	}
}

func TestSplitbyColon(t *testing.T) {
	cases := []struct {
		line   string
		fields []string
		values []string
	}{
		{
			"",
			[]string{},
			[]string{},
		},
		{
			"123a",
			[]string{},
			[]string{"123a"},
		},
		{
			"1a: 2b",
			[]string{"1a"},
			[]string{"2b"},
		},
		{
			"1a: [2b 3c] 4d: 5e",
			[]string{"1a", "4d"},
			[]string{"[2b 3c]", "5e"},
		},
		{
			"1a: [2b,3c] 4d: 5e",
			[]string{"1a", "4d"},
			[]string{"[2b,3c]", "5e"},
		},
		{

			"Time: 2021-09-08T14:39:54.506967433+08:00",
			[]string{"Time"},
			[]string{"2021-09-08T14:39:54.506967433+08:00"},
		},
	}
	for _, c := range cases {
		resFields, resValues := splitByColon(c.line)
		require.Equal(t, c.fields, resFields)
		require.Equal(t, c.values, resValues)
	}
}

func TestBatchLogForReversedScan(t *testing.T) {
	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;
# Time: 2020-02-15T20:00:05.000000+08:00`
	logData2 := `select 3;
# Time: 2020-02-16T18:00:01.000000+08:00
select 4;
# Time: 2020-02-16T18:00:05.000000+08:00
select 5;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 6;
# Time: 2020-02-17T18:00:05.000000+08:00
select 7;
# Time: 2020-04-15T18:00:05.299063744+08:00`
	logData4 := `select 8;
# Time: 2020-04-15T19:00:05.299063744+08:00
select 9;`
	logData := []string{logData0, logData1, logData2, logData3, logData4}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-2020-02-17T19-04-05.01.log"
	fileName4 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}
	prepareLogs(t, logData, fileNames)
	defer func() {
		removeFiles(fileNames)
	}()

	cases := []struct {
		startTime string
		endTime   string
		files     []string
		logs      [][]string
	}{
		{
			startTime: "2020-02-15T18:00:00.000000+08:00",
			endTime:   "2020-02-15T19:00:00.000000+08:00",
			files:     []string{fileName1},
			logs: [][]string{
				{"# Time: 2020-02-15T19:00:05.000000+08:00",
					"select 2;",
					"# Time: 2020-02-15T18:00:01.000000+08:00",
					"select 1;"},
			},
		},
		{
			startTime: "2020-02-15T20:00:05.000000+08:00",
			endTime:   "2020-02-17T19:00:00.000000+08:00",
			files:     []string{fileName1, fileName2, fileName3},
			logs: [][]string{
				{"# Time: 2020-02-17T18:00:05.000000+08:00",
					"select 7;",
					"# Time: 2020-02-16T19:00:00.000000+08:00",
					"select 6;",
					"# Time: 2020-02-16T18:00:05.000000+08:00",
					"select 5;",
					"# Time: 2020-02-16T18:00:01.000000+08:00",
					"select 4;",
					"# Time: 2020-02-16T18:00:01.000000+08:00",
					"select 3;"},
			},
		},
		{
			startTime: "2020-02-16T19:00:00.000000+08:00",
			endTime:   "2020-04-15T20:00:00.000000+08:00",
			files:     []string{fileName3, fileName4},
			logs: [][]string{
				{"# Time: 2020-04-15T19:00:05.299063744+08:00",
					"select 9;",
					"Time: 2020-04-15T18:00:05.299063744+08:00",
					"select 8;",
					"# Time: 2020-02-17T18:00:05.000000+08:00",
					"select 7;",
					"# Time: 2020-02-16T19:00:00.000000+08:00",
					"select 6;"},
			},
		},
	}

	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	sctx := mock.NewContext()
	sctx.GetSessionVars().TimeZone = loc
	sctx.GetSessionVars().SlowQueryFile = fileName3
	for i, cas := range cases {
		extractor := &plannercore.SlowQueryExtractor{Enable: len(cas.startTime) > 0 && len(cas.endTime) > 0, Desc: true}
		if extractor.Enable {
			startTime, err := ParseTime(cas.startTime)
			require.NoError(t, err)
			endTime, err := ParseTime(cas.endTime)
			require.NoError(t, err)
			extractor.TimeRanges = []*plannercore.TimeRange{{StartTime: startTime, EndTime: endTime}}
		}
		retriever, err := newSlowQueryRetriever()
		require.NoError(t, err)
		retriever.extractor = extractor
		sctx.GetSessionVars().SlowQueryFile = fileName4
		err = retriever.initialize(context.Background(), sctx)
		require.NoError(t, err)
		comment := fmt.Sprintf("case id: %v", i)
		if len(retriever.files) > 0 {
			reader := bufio.NewReader(retriever.files[0].file)
			offset := &offset{length: 0, offset: 0}
			rows, err := retriever.getBatchLogForReversedScan(context.Background(), reader, offset, 3)
			require.NoError(t, err)
			for _, row := range rows {
				for j, log := range row {
					require.Equal(t, log, cas.logs[0][j], comment)
				}
			}
		}
		require.NoError(t, retriever.close())
	}
}

func TestCancelParseSlowLog(t *testing.T) {
	fileName := "tidb-slow-2020-02-14T19-04-05.01.log"
	slowLog := `# Time: 2019-04-28T15:24:04.309074+08:00
select * from t;`
	prepareLogs(t, []string{slowLog}, []string{fileName})
	defer func() {
		removeFiles([]string{fileName})
	}()
	sctx := mock.NewContext()
	sctx.GetSessionVars().SlowQueryFile = fileName

	retriever, err := newSlowQueryRetriever()
	require.NoError(t, err)
	var signal1, signal2 = make(chan int, 1), make(chan int, 1)
	ctx := context.WithValue(context.Background(), "signals", []chan int{signal1, signal2})
	ctx, cancel := context.WithCancel(ctx)
	err = failpoint.Enable("github.com/pingcap/tidb/executor/mockReadSlowLogSlow", "return(true)")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockReadSlowLogSlow"))
	}()
	go func() {
		_, err := retriever.retrieve(ctx, sctx)
		require.Errorf(t, err, "context canceled")
	}()
	// Wait for parseSlowLog going to add tasks.
	<-signal1
	// Cancel the retriever and then dataForSlowLog exits.
	cancel()
	// Assume that there are already unprocessed tasks.
	retriever.taskList <- slowLogTask{}
	// Let parseSlowLog continue.
	signal2 <- 1
	// parseSlowLog should exit immediately.
	time.Sleep(1 * time.Second)
	require.False(t, checkGoroutineExists("parseSlowLog"))
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	err := profile.WriteTo(buf, 1)
	if err != nil {
		panic(err)
	}
	str := buf.String()
	return strings.Contains(str, keyword)
}

func prepareLogs(t *testing.T, logData []string, fileNames []string) {
	writeFile := func(file string, data string) {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	for i, log := range logData {
		writeFile(fileNames[i], log)
	}
}

func removeFiles(fileNames []string) {
	for _, fileName := range fileNames {
		os.Remove(fileName)
	}
}
