// Copyright 2024 PingCAP, Inc.
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

package analyze

import (
	"io"
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeVirtualColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (id bigint NOT NULL,c1 varchar(50) NOT NULL ,c2 int DEFAULT NULL ,c3 json DEFAULT NULL ,c4 varchar(255) GENERATED ALWAYS AS (json_unquote(json_extract(c3, '$.oppositePlaceId'))) VIRTUAL ,PRIMARY KEY (id),UNIQUE KEY idx_unique (c1,c2)) ;`)
	tk.MustExec("analyze table t1 all columns")
}

func TestAnalyzeWithSpecificData(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// https://github.com/pingcap/tidb/issues/57448

	datapath := "./testdata/test_data.csv"
	content, err := os.ReadFile(datapath)
	require.NoError(t, err)
	var reader io.ReadCloser = mydump.NewStringReader(string(content))
	var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (
		r io.ReadCloser, err error,
	) {
		return reader, nil
	}
	ctx := tk.Session().(sessionctx.Context)
	ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
	tk.MustExec(" CREATE TABLE t1 (COL102 float DEFAULT NULL,COL103 float DEFAULT NULL,COL1 float GENERATED ALWAYS AS (COL102 % 10) STORED,COL2 varchar(20) DEFAULT NULL,COL4 datetime DEFAULT NULL,COL3 bigint DEFAULT NULL,COL5 float DEFAULT NULL, KEY UK_COL1 (COL1) /*!80000 INVISIBLE */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES (COL102,COL103,COL1,COL2,COL4,COL3,COL5) ")
	tk.MustExec("analyze table t1")
}
