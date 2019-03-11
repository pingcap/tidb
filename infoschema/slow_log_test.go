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

package infoschema

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/logutil"
)

func TestParseSlowLogFile(t *testing.T) {
	slowLog := bytes.NewBufferString(
		`# Time: 2019-01-24-22:32:29.313255 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
select * from t;`)
	scanner := bufio.NewScanner(slowLog)
	rows, err := parseSlowLog(scanner)
	if err != nil {
		t.Fatalf("parse slow log failed")
	}
	if len(rows) != 1 {
		t.Fatalf("parse slow log failed")
	}
	recordString := ""
	for i, value := range rows[0] {
		str, err := value.ToString()
		if err != nil {
			t.Fatalf("parse slow log failed")
		}
		if i > 0 {
			recordString += ","
		}
		recordString += str
	}
	expectRecordString := "2019-01-24 22:32:29.313255,405888132465033227,,0,0.216905,0.021,0,0,1,637,0,,,1,select * from t;"
	if expectRecordString != recordString {
		t.Fatalf("parse slow log failed, expect: %v\nbut got: %v\n", expectRecordString, recordString)
	}
}

func TestParseTime(t *testing.T) {
	t1Str := "2019-01-24-22:32:29.313255 +0800"
	t2Str := "2019-01-24-22:32:29.313255"
	t1, err := parseTime(t1Str)
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("parse time failed")
	}
	t2, err := time.ParseInLocation("2006-01-02-15:04:05.999999999", t2Str, loc)
	if t1.Unix() != t2.Unix() {
		t.Fatalf("parse time failed, %v != %v", t1.Unix(), t2.Unix())
	}
	t1Format := t1.In(loc).Format(logutil.SlowLogTimeFormat)
	if t1Format != t1Str {
		t.Fatalf("parse time failed, %v != %v", t1Format, t1Str)
	}
}
