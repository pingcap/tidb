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

package adminpause

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
)

// AgeMax limits the max number of tuple generated for t_user.age
const AgeMax int = 120

// TestTableUser indicates the columns of table `t_user` to generate tuples
type TestTableUser struct {
	id          int
	tenant      string
	name        string
	age         int
	province    string
	city        string
	phone       string
	createdTime time.Time
	updatedTime time.Time
}

// Frequently referenced definition of `table` in all test cases among `admin pause test cases`
const adminPauseTestTable string = "t_user"
const adminPauseTestTableStmt string = `CREATE TABLE if not exists ` + adminPauseTestTable + ` (
	id int(11) NOT NULL AUTO_INCREMENT,
	tenant varchar(128) NOT NULL,
	name varchar(128) NOT NULL,
	age int(11) NOT NULL,
	province varchar(32) NOT NULL DEFAULT '',
	city varchar(32) NOT NULL DEFAULT '',
	phone varchar(16) NOT NULL DEFAULT '',
	created_time datetime NOT NULL,
	updated_time datetime NOT NULL
  );`

const adminPauseTestPartitionTable string = "t_user_partition"
const adminPauseTestPartitionTableStmt string = `CREATE TABLE if not exists ` + adminPauseTestPartitionTable + ` (
		  id int(11) NOT NULL AUTO_INCREMENT,
		  tenant varchar(128) NOT NULL,
		  name varchar(128) NOT NULL,
		  age int(11) NOT NULL,
		  province varchar(32) NOT NULL DEFAULT '',
		  city varchar(32) NOT NULL DEFAULT '',
		  phone varchar(16) NOT NULL DEFAULT '',
		  created_time datetime NOT NULL,
		  updated_time datetime NOT NULL
		) partition by range( age ) (
		  partition p0 values less than (20),
		  partition p1 values less than (40),
		  partition p2 values less than (60),
		  partition p3 values less than (80),
		  partition p4 values less than (100),
		  partition p5 values less than (120),
		  partition p6 values less than (160));`

func generateString(letterRunes []rune, length int) (string, error) {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b), nil
}

func generateName(length int) (string, error) {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	return generateString(letterRunes, length)
}

func generatePhone(length int) (string, error) {
	var numberRunes = []rune("0123456789")
	return generateString(numberRunes, length)
}

func (tu *TestTableUser) generateAttributes(id int) (err error) {
	tu.id = id // keep the linter quiet
	tu.tenant, err = generateName(rand.Intn(127))
	if err != nil {
		return err
	}
	tu.name, err = generateName(rand.Intn(127))
	if err != nil {
		return err
	}

	tu.age = rand.Intn(AgeMax)

	tu.province, err = generateName(rand.Intn(32))
	if err != nil {
		return err
	}
	tu.city, err = generateName(rand.Intn(32))
	if err != nil {
		return err
	}
	tu.phone, err = generatePhone(14)
	if err != nil {
		return err
	}
	tu.createdTime = time.Now()
	tu.updatedTime = time.Now()

	return nil
}

func (tu *TestTableUser) insertStmt(tableName string, count int) string {
	sql := fmt.Sprintf("INSERT INTO %s(tenant, name, age, province, city, phone, created_time, updated_time) VALUES ", tableName)
	for n := 0; n < count; n++ {
		_ = tu.generateAttributes(n)
		sql += fmt.Sprintf("('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s')",
			tu.tenant, tu.name, tu.age, tu.province, tu.city, tu.phone, tu.createdTime, tu.updatedTime)
		if n != count-1 {
			sql += ", "
		}
	}
	return sql
}

func generateTblUser(tk *testkit.TestKit, rowCount int) error {
	tk.MustExec(adminPauseTestTableStmt)
	if rowCount == 0 {
		return nil
	}
	tu := &TestTableUser{}
	tk.MustExec(tu.insertStmt(adminPauseTestTable, rowCount))
	return nil
}

func generateTblUserParition(tk *testkit.TestKit) error {
	tk.MustExec(adminPauseTestPartitionTableStmt)
	return nil
}
