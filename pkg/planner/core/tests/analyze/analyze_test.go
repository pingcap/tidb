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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
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
	tk.MustExec("CREATE TABLE t1 (`COL102` float DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO t1 (COL102) VALUES (4),(-6),(4),(0),(-6),(4),(6),(0),(0),(-2),(0),(8),(-6),(4),(4),(-6),(4),(6),(6),(-2),(-8),(-2),(6),(8),(-6),(-4),(0),(0),(0),(-4),(-4),(-8),(4),(2),(0),(2),(-8),(8),(-4),(-8),(-4),(-8),(-4),(2),(4),(4),(2),(4),(0),(-4),(-2),(2),(-8),(-2),(4),(37055),(2),(0),(-4),(-8),(8),(6),(2),(-8),(8),(0),(-2),(4),(8),(0),(8),(6),(-8),(-2),(0),(-6),(-2),(0),(-2),(2),(0),(-4),(-4),(6),(8),(-8),(4),(4),(-8),(2),(-2),(8),(6),(8),(-8),(8),(0),(-2),(-6),(-6),(-4),(0);")
	tk.MustExec("analyze table t1")
}
