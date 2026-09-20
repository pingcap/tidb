// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type statement struct {
	sql           string
	expectedError uint16
}

func main() {
	port := flag.String("port", "4000", "TiDB MySQL protocol port")
	testPath := flag.String("test", "", "integration test input file")
	resultPath := flag.String("result", "", "integration test expected result file")
	record := flag.Bool("record", false, "record the actual result")
	flag.Parse()

	if *testPath == "" || *resultPath == "" {
		fatalf("both -test and -result are required")
	}

	statements, err := readStatements(*testPath)
	if err != nil {
		fatalf("read test file: %v", err)
	}

	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%s)/test?timeout=5s&readTimeout=5s&writeTimeout=5s", *port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatalf("open TiDB connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatalf("connect to TiDB: %v", err)
	}

	var actual strings.Builder
	for _, stmt := range statements {
		actual.WriteString(stmt.sql)
		actual.WriteByte('\n')

		_, err := db.ExecContext(context.Background(), stmt.sql)
		if stmt.expectedError == 0 {
			if err != nil {
				fatalf("execute %q: %v", stmt.sql, err)
			}
			continue
		}

		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) {
			fatalf("execute %q: expected MySQL error %d, got %v", stmt.sql, stmt.expectedError, err)
		}
		if mysqlErr.Number != stmt.expectedError {
			fatalf("execute %q: expected MySQL error %d, got %d", stmt.sql, stmt.expectedError, mysqlErr.Number)
		}
		actual.WriteString(mysqlErr.Error())
		actual.WriteByte('\n')
	}

	if *record {
		if err := os.WriteFile(*resultPath, []byte(actual.String()), 0o644); err != nil {
			fatalf("record result file: %v", err)
		}
		return
	}

	expected, err := os.ReadFile(*resultPath)
	if err != nil {
		fatalf("read result file: %v", err)
	}
	if actual.String() != string(expected) {
		fatalf("result mismatch\nexpected:\n%s\nactual:\n%s", expected, actual.String())
	}
}

func readStatements(path string) ([]statement, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var (
		statements    []statement
		pendingError  uint16
		statementText strings.Builder
	)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "-- error ") {
			code, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "-- error ")), 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parse expected error %q: %w", line, err)
			}
			pendingError = uint16(code)
			continue
		}

		if statementText.Len() > 0 {
			statementText.WriteByte(' ')
		}
		statementText.WriteString(line)
		if !strings.HasSuffix(line, ";") {
			continue
		}
		statements = append(statements, statement{sql: statementText.String(), expectedError: pendingError})
		statementText.Reset()
		pendingError = 0
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if statementText.Len() != 0 {
		return nil, errors.New("unterminated SQL statement")
	}
	if len(statements) == 0 {
		return nil, errors.New("test file contains no SQL statements")
	}
	return statements, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
