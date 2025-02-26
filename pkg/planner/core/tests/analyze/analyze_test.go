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
