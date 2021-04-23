// +build gofuzz

package fuzz

import (
	"os"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	// to pin dep in go.mod
	_ "github.com/oraluben/go-fuzz/go-fuzz-dep"

	"github.com/pingcap/tidb/tidb-server/internal"
)

var conn *sql.DB = nil
var err error

func init() {
	os.Args = []string{os.Args[0]}

	go internal.Main()

	for i := 0; i < 5; i++ {
		conn, err = sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
	}
	if err != nil {
		panic("TiDB not up after 5 seconds")
	}
}

// Fuzz is the required name by go-fuzz
func Fuzz(raw []byte) int {
	query := string(raw)

	_, err = conn.Exec(query)

	if err != nil {
		return 0
	}

	return 1
}
