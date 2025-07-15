package main

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/store/mockstore"
)

func main() {
	store := testkit.CreateMockStore(nil, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(nil, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")

	// Test signed INT with value > MaxInt32
	maxInt32Plus1 := "2147483648" // math.MaxInt32 + 1
	sql := fmt.Sprintf("CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT = %s", maxInt32Plus1)
	
	err := tk.ExecToErr(sql)
	if err != nil {
		fmt.Printf("Error occurred: %v\n", err)
	} else {
		fmt.Printf("No error - AUTO_INCREMENT value %s was accepted\n", maxInt32Plus1)
		tk.MustExec("DROP TABLE t1")
	}
}