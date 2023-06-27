package internal

import (
	"fmt"

	"github.com/pingcap/tidb/testkit"
)

// FillData fill data into table
func FillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
}
