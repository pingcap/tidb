// Copyright 2023 PingCAP, Inc.
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

package loadremotetest

import (
	"fmt"

	"github.com/pingcap/tidb/testkit"
)

func (s *mockGCSSuite) TestLoadCSV() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (i INT, s varchar(32));")

	// no-new-line-at-end
	sql := fmt.Sprintf(`LOAD DATA REMOTE INFILE 'gcs://test-bucket/no-new-line-at-end.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"100 test100",
		"101 \"",
		"102 😄😄😄😄😄",
		"104 ",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// new-line-at-end
	sql = fmt.Sprintf(`LOAD DATA REMOTE INFILE 'gcs://test-bucket/new-line-at-end.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"100 test100",
		"101 \"",
		"102 😄😄😄😄😄",
		"104 ",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// can't read file at tidb-server
	sql = "LOAD DATA REMOTE INFILE '/etc/passwd' INTO TABLE load_csv.t;"
	s.tk.MustContainErrMsg(sql, "don't support load data from tidb-server")
}

func (s *mockGCSSuite) TestIgnoreNLines() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (s varchar(32), i INT);")

	sql := fmt.Sprintf(`LOAD DATA REMOTE INFILE 'gcs://test-bucket/ignore-lines-bad-syntax.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"b 2",
		"c 3",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA REMOTE INFILE 'gcs://test-bucket/ignore-lines-bad-syntax.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 100 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows())
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// test IGNORE N LINES will directly find (line) terminator without checking it's inside quotes

	sql = fmt.Sprintf(`LOAD DATA REMOTE INFILE 'gcs://test-bucket/count-terminator-inside-quotes.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 2 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"b\n 2",
		"c 3",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")
}
