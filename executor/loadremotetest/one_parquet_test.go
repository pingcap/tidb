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
	_ "embed"
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/testkit"
)

//go:embed test.parquet
var content []byte

func (s *mockGCSSuite) TestLoadParquet() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (" +
		"id INT, val1 INT, val2 VARCHAR(20), " +
		"d1 DECIMAL(10, 0), d2 DECIMAL(10, 2), d3 DECIMAL(8, 8)," +
		"d4 DECIMAL(20, 0), d5 DECIMAL(36, 0), d6 DECIMAL(28, 8));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load-parquet",
			Name:       "p",
		},
		Content: content,
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load-parquet/p?endpoint=%s'
		FORMAT 'parquet' INTO TABLE load_csv.t;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 1 0 123 1.23 0.00000001 1234567890 123 1.23000000",
		"2 123456 0 123456 9999.99 0.12345678 99999999999999999999 999999999999999999999999999999999999 99999999999999999999.99999999",
		"3 123456 0 -123456 -9999.99 -0.12340000 -99999999999999999999 -999999999999999999999999999999999999 -99999999999999999999.99999999",
		"4 1 0 123 1.23 0.00000001 1234567890 123 1.23000000",
		"5 123456 0 123456 9999.99 0.12345678 12345678901234567890 123456789012345678901234567890123456 99999999999999999999.99999999",
		"6 123456 0 -123456 -9999.99 -0.12340000 -12345678901234567890 -123456789012345678901234567890123456 -99999999999999999999.99999999",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")
}
