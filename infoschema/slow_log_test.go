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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
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
	row := rows[0]
	t1 := types.Time{
		Time: types.FromDate(2019, 01, 24, 22, 32, 29, 313255),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	if logTime, ok := row["Time"]; !ok || logTime.GetMysqlTime() != t1 {
		t.Fatalf("parse slow log failed")
	}
	if ts, ok := row["Txn_start_ts"]; !ok || ts.GetUint64() != 405888132465033227 {
		t.Fatalf("parse slow log failed")
	}
	if queryTime, ok := row["Query_time"]; !ok || queryTime.GetFloat64() != 0.216905 {
		t.Fatalf("parse slow log failed")
	}
	if ProcessTime, ok := row["Process_time"]; !ok || ProcessTime.GetFloat64() != 0.021 {
		t.Fatalf("parse slow log failed")
	}
	if requestCount, ok := row["Request_count"]; !ok || requestCount.GetUint64() != 1 {
		t.Fatalf("parse slow log failed")
	}
	if totalKeys, ok := row["Total_keys"]; !ok || totalKeys.GetUint64() != 637 {
		t.Fatalf("parse slow log failed")
	}
	if processedKeys, ok := row["Processed_keys"]; !ok || processedKeys.GetUint64() != 436 {
		t.Fatalf("parse slow log failed")
	}
	if isInternal, ok := row["Is_internal"]; !ok || isInternal.GetInt64() != 1 {
		t.Fatalf("parse slow log failed")
	}
	if sql, ok := row["Query"]; !ok || sql.GetString() != "select * from t;" {
		t.Fatalf("parse slow log failed")
	}
}
