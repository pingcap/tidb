// Copyright 2025 PingCAP, Inc.
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

package logicalplan

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestGroupBySchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE mysql_3 (
    col_int_auto_increment INT(10) AUTO_INCREMENT,
    col_pk_char CHAR(60) NOT NULL,
    col_pk_date DATE NOT NULL,
    col_datetime DATETIME,
    col_int INT,
    col_date DATE,
    PRIMARY KEY (col_int_auto_increment, col_pk_char, col_datetime, col_int, col_date)
);`)
	tk.MustQuery(`explain format='brief' SELECT *
FROM mysql_3 t1
WHERE EXISTS
    (SELECT DISTINCT a1.*
     FROM mysql_3 a1
     WHERE (a1.col_pk_char NOT IN
              (SELECT a1.col_pk_char
               FROM mysql_3 a1 NATURAL
               RIGHT JOIN mysql_3 a2
               WHERE t1.col_pk_date IS NULL
               GROUP BY a1.col_pk_char)) )`).Check(testkit.Rows("TableDual 0.00 root  rows:0"))
}
