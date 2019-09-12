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

package infoschema_test

import (
	"bufio"
	"bytes"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
)

func (s *testSuite) TestParseSlowLogFile(c *C) {
	slowLog := bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Succ: false
# Prev_stmt: update t set i = 1;
select * from t;`)
	reader := bufio.NewReader(slowLog)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	rows, err := infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, IsNil)
	c.Assert(len(rows), Equals, 1)
	recordString := ""
	for i, value := range rows[0] {
		str, err := value.ToString()
		c.Assert(err, IsNil)
		if i > 0 {
			recordString += ","
		}
		recordString += str
	}
	expectRecordString := "2019-04-28 15:24:04.309074,405888132465033227,,,0,0.216905,0.021,0,0,1,637,0,,,1,42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772,t1:1,t2:2,0.1,0.2,0.03,127.0.0.1:20160,0.05,0.6,0.8,0.0.0.0:20160,70724,0,,update t set i = 1;,select * from t;"
	c.Assert(expectRecordString, Equals, recordString)

	// fix sql contain '# ' bug
	slowLog = bytes.NewBufferString(
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
	_, err = infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, IsNil)

	// test for time format compatibility.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2019-04-24-19:41:21.716221 +0800
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	rows, err = infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, IsNil)
	c.Assert(len(rows) == 2, IsTrue)
	t0Str, err := rows[0][0].ToString()
	c.Assert(err, IsNil)
	c.Assert(t0Str, Equals, "2019-04-28 15:24:04.309074")
	t1Str, err := rows[1][0].ToString()
	c.Assert(err, IsNil)
	c.Assert(t1Str, Equals, "2019-04-24 19:41:21.716221")

	// test for bufio.Scanner: token too long.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2019-04-24-19:41:21.716221 +0800
`)
	originValue := variable.MaxOfMaxAllowedPacket
	variable.MaxOfMaxAllowedPacket = 65536
	sql := strings.Repeat("x", int(variable.MaxOfMaxAllowedPacket+1))
	slowLog.WriteString(sql)
	reader = bufio.NewReader(slowLog)
	_, err = infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "single line length exceeds limit: 65536")

	variable.MaxOfMaxAllowedPacket = originValue
	reader = bufio.NewReader(slowLog)
	_, err = infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, IsNil)

	// Add parse error check.
	slowLog = bytes.NewBufferString(
		`# Time: 2019-04-28T15:24:04.309074+08:00
# Succ: abc
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	_, err = infoschema.ParseSlowLog(loc, reader)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "parse slow log failed `Succ` error: strconv.ParseBool: parsing \"abc\": invalid syntax")
}

func (s *testSuite) TestSlowLogParseTime(c *C) {
	t1Str := "2019-01-24T22:32:29.313255+08:00"
	t2Str := "2019-01-24T22:32:29.313255"
	t1, err := infoschema.ParseTime(t1Str)
	c.Assert(err, IsNil)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	t2, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", t2Str, loc)
	c.Assert(err, IsNil)
	c.Assert(t1.Unix(), Equals, t2.Unix())
	t1Format := t1.In(loc).Format(logutil.SlowLogTimeFormat)
	c.Assert(t1Format, Equals, t1Str)
}

// TestFixParseSlowLogFile bugfix
// sql select * from INFORMATION_SCHEMA.SLOW_QUERY limit 1;
// ERROR 1105 (HY000): string "2019-05-12-11:23:29.61474688" doesn't has a prefix that matches format "2006-01-02-15:04:05.999999999 -0700", err: parsing time "2019-05-12-11:23:29.61474688" as "2006-01-02-15:04:05.999999999 -0700": cannot parse "" as "-0700"
func (s *testSuite) TestFixParseSlowLogFile(c *C) {
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
select * from t;`)
	scanner := bufio.NewReader(slowLog)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	_, err = infoschema.ParseSlowLog(loc, scanner)
	c.Assert(err, IsNil)
}
