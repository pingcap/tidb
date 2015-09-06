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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/peterh/liner"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/util/errors2"
	"github.com/pingcap/tidb/util/printer"
)

var (
	logLevel = flag.String("L", "error", "log level")
	store    = flag.String("store", "goleveldb", "the name for the registered storage, e.g. memory, goleveldb, boltdb")
	dbPath   = flag.String("dbpath", "test", "db path")

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
	if tidb.IsQuery(txnLine) {
		rows, err := tx.Query(txnLine)
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

		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
	} else {
		// TODO: rows affected and last insert id
		_, err := tx.Exec(txnLine)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func mayExit(err error, line string) {
	if errors2.ErrorEqual(err, liner.ErrPromptAborted) || errors2.ErrorEqual(err, io.EOF) {
		fmt.Println("\nBye")
		saveHistory()
		os.Exit(0)
	}
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
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

	mdb, err := sql.Open(tidb.DriverName, *store+"://"+*dbPath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	for {
		l, err := readStatement("tidb> ")
		mayExit(err, l)
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
				mayExit(err, txnLine)
				line.AppendHistory(txnLine)

				if !strings.HasSuffix(txnLine, ";") {
					txnLine += ";"
				}

				if strings.HasPrefix(txnLine, "COMMIT") || strings.HasPrefix(txnLine, "commit") {
					err := tx.Commit()
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
			} else {
				tx.Commit()
			}
		}
	}
}
