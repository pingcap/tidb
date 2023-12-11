// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package explain

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestIssue47331(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1(
		id1 varchar(2) DEFAULT '00',
		id2 varchar(30) NOT NULL,
		id3 datetime DEFAULT NULL,
		id4 varchar(100) NOT NULL DEFAULT 'ecifdata',
		id5 datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		id6 int(11) DEFAULT NULL,
		id7 int(11) DEFAULT NULL,
		UNIQUE KEY UI_id2 (id2),
		KEY ix_id1 (id1)
	)`)
	tk.MustExec("drop table if exists t2")
	tk.MustExec(`create table t2(
		id10 varchar(40) NOT NULL,
		id2 varchar(30) NOT NULL,
		KEY IX_id2 (id2),
		PRIMARY KEY (id10)
	)`)
	tk.MustExec("drop table if exists t3")
	tk.MustExec(`create table t3(
		id20 varchar(40) DEFAULT NULL,
		UNIQUE KEY IX_id20 (id20)
	)`)
	tk.MustExec(`
		explain
		UPDATE t1 a
		SET a.id1 = '04',
			a.id3 = CURRENT_TIMESTAMP,
			a.id4 = SUBSTRING_INDEX(USER(), '@', 1),
			a.id5 = CURRENT_TIMESTAMP
		WHERE a.id1 = '03'
			AND a.id6 - IFNULL(a.id7, 0) =
				(
					SELECT COUNT(1)
					FROM t2 b, t3 c
					WHERE b.id10 = c.id20
						AND b.id2 = a.id2
						AND b.id2 in (
							SELECT rn.id2
							FROM t1 rn
							WHERE rn.id1 = '03'
						)
				);
	`)
}
