// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/peterh/liner"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/printer"
)

var (
	logLevel = flag.String("L", "error", "log level")
	store    = flag.String("store", "goleveldb", "the name for the registered storage, e.g. memory, goleveldb, boltdb")
	dbPath   = flag.String("dbpath", "test", "db path")
	dbName   = flag.String("dbname", "test", "default db name")
	lease    = flag.Int("lease", 1, "schema lease seconds, very dangerous to change only if you know what you do")

	line        *liner.State
	historyPath = "/tmp/tidb_interpreter"
)

func openHistory() {
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
}

func saveHistory() {
	if f, err := os.Create(historyPath); err == nil {
		line.WriteHistory(f)
		f.Close()
	}
}

func executeLine(tx *sql.Tx, txnLine string) error {
	start := time.Now()
	if tidb.IsQuery(txnLine) {
		rows, err := tx.Query(txnLine)
		elapsed := time.Since(start).Seconds()
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return errors.Trace(err)
		}

		values := make([][]byte, len(cols))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		var datas [][]string
		for rows.Next() {
			err := rows.Scan(scanArgs...)
			if err != nil {
				return errors.Trace(err)
			}

			data := make([]string, len(cols))
			for i, value := range values {
				if value == nil {
					data[i] = "NULL"
				} else {
					data[i] = string(value)
				}
			}

			datas = append(datas, data)
		}

		// For `cols` and `datas[i]` always has the same length,
		// no need to check return validity.
		result, _ := printer.GetPrintResult(cols, datas)
		fmt.Printf("%s", result)

		switch len(datas) {
		case 0:
			fmt.Printf("Empty set")
		case 1:
			fmt.Printf("1 row in set")
		default:
			fmt.Printf("%v rows in set", len(datas))
		}
		fmt.Printf(" (%.2f sec)\n", elapsed)
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
	} else {
		// TODO: last insert id
		res, err := tx.Exec(txnLine)
		elapsed := time.Since(start).Seconds()
		if err != nil {
			return errors.Trace(err)
		}
		cnt, err := res.RowsAffected()
		if err != nil {
			return errors.Trace(err)
		}
		switch cnt {
		case 0, 1:
			fmt.Printf("Query OK, %d row affected", cnt)
		default:
			fmt.Printf("Query OK, %d rows affected", cnt)
		}
		fmt.Printf(" (%.2f sec)\n", elapsed)
	}
	return nil
}

func mayExit(err error, l string) bool {
	if terror.ErrorEqual(err, liner.ErrPromptAborted) || terror.ErrorEqual(err, io.EOF) {
		fmt.Println("\nBye")
		saveHistory()
		return true
	}
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	return false
}

func readStatement(prompt string) (string, error) {
	var ret string
	for {
		l, err := line.Prompt(prompt)
		if err != nil {
			return "", err
		}
		if strings.HasSuffix(l, ";") == false {
			ret += l + "\n"
			prompt = "   -> "
			continue
		}
		return ret + l, nil
	}
}

func main() {
	printer.PrintTiDBInfo()

	flag.Parse()
	log.SetLevelByString(*logLevel)
	// support for signal notify
	runtime.GOMAXPROCS(runtime.NumCPU())

	line = liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	openHistory()

	tidb.SetSchemaLease(time.Duration(*lease) * time.Second)

	// use test as default DB.
	mdb, err := sql.Open(tidb.DriverName, *store+"://"+*dbPath+"/"+*dbName)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	for {
		l, err := readStatement("tidb> ")
		if mayExit(err, l) {
			return
		}
		line.AppendHistory(l)

		// if we're in transaction
		if strings.HasPrefix(l, "BEGIN") || strings.HasPrefix(l, "begin") {
			tx, err := mdb.Begin()
			if err != nil {
				log.Error(errors.ErrorStack(err))
				continue
			}
			for {
				txnLine, err := readStatement(">> ")
				if mayExit(err, txnLine) {
					return
				}
				line.AppendHistory(txnLine)

				if !strings.HasSuffix(txnLine, ";") {
					txnLine += ";"
				}

				if strings.HasPrefix(txnLine, "COMMIT") || strings.HasPrefix(txnLine, "commit") {
					err = tx.Commit()
					if err != nil {
						log.Error(errors.ErrorStack(err))
						tx.Rollback()
					}
					break
				}
				// normal sql statement
				err = executeLine(tx, txnLine)
				if err != nil {
					log.Error(errors.ErrorStack(err))
					tx.Rollback()
					break
				}
			}
		} else {
			tx, err := mdb.Begin()
			if err != nil {
				log.Error(errors.ErrorStack(err))
				continue
			}
			err = executeLine(tx, l)
			if err != nil {
				log.Error(errors.ErrorStack(err))
				tx.Rollback()
				continue
			}
			err = tx.Commit()
			if err != nil {
				log.Error(errors.ErrorStack(err))
			}
		}
	}
}
