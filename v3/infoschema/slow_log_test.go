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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v3/infoschema"
	"github.com/pingcap/tidb/v3/util/logutil"
)

func (s *testSuite) TestParseSlowLogFile(c *C) {
	slowLog := bytes.NewBufferString(
		`# Time: 2019-01-24-22:32:29.313255 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
select * from t;`)
	scanner := bufio.NewScanner(slowLog)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	rows, err := infoschema.ParseSlowLog(loc, scanner)
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
	expectRecordString := "2019-01-24 22:32:29.313255,405888132465033227,,0,0.216905,0.021,0,0,1,637,0,,,1,42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772,select * from t;"
	c.Assert(expectRecordString, Equals, recordString)
}

func (s *testSuite) TestSlowLogParseTime(c *C) {
	t1Str := "2019-01-24-22:32:29.313255 +0800"
	t2Str := "2019-01-24-22:32:29.313255"
	t1, err := infoschema.ParseTime(t1Str)
	c.Assert(err, IsNil)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	t2, err := time.ParseInLocation("2006-01-02-15:04:05.999999999", t2Str, loc)
	c.Assert(err, IsNil)
	c.Assert(t1.Unix(), Equals, t2.Unix())
	t1Format := t1.In(loc).Format(logutil.SlowLogTimeFormat)
	c.Assert(t1Format, Equals, t1Str)
}
