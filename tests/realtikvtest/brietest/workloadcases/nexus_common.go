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

package workloadcases

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/tidb/pkg/testkit/brhelper/workload"
)

type nexusTableState struct {
	Name      string   `json:"name"`
	NextColID int      `json:"next_col_id,omitempty"`
	Cols      []string `json:"cols,omitempty"`
}

type nexusState struct {
	Suffix string `json:"suffix"`
	DB     string `json:"db"`
	N      int    `json:"n"`

	Ticked      int               `json:"ticked"`
	NextTableID int               `json:"next_table_id"`
	Tables      []nexusTableState `json:"tables"`

	Checksums map[string]workload.TableChecksum `json:"checksums,omitempty"`
	LogDone   bool                              `json:"log_done"`
}

func nexusDefaultN(n int) int {
	if n <= 0 {
		return 50
	}
	return n
}

func nexusHalf(n int) int {
	h := n / 2
	if h <= 0 {
		return 1
	}
	return h
}

func nexusTableName(id int) string {
	return fmt.Sprintf("t_%d", id)
}

func nexusExecDDL(ctx context.Context, db *sql.DB, tick int, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
	return err
}

func nexusCreateTable(ctx context.Context, db *sql.DB, tick int, schema, table string) error {
	stmt := "CREATE TABLE IF NOT EXISTS " + workload.QTable(schema, table) + " (" +
		"id BIGINT PRIMARY KEY AUTO_INCREMENT," +
		"v BIGINT," +
		"s VARCHAR(64) NOT NULL" +
		")"
	return nexusExecDDL(ctx, db, tick, stmt)
}

func nexusInsertRow(ctx context.Context, db *sql.DB, schema, table string, tick int) error {
	_, err := db.ExecContext(ctx, "INSERT INTO "+workload.QTable(schema, table)+" (v,s) VALUES (?,?)",
		int64(tick), fmt.Sprintf("%s_%d", table, tick),
	)
	return err
}

func nexusRecordChecksums(ctx context.Context, db *sql.DB, schema string, tables []nexusTableState) (map[string]workload.TableChecksum, error) {
	out := make(map[string]workload.TableChecksum, len(tables))
	for _, t := range tables {
		sum, err := workload.AdminChecksumTable(ctx, db, schema, t.Name)
		if err != nil {
			return nil, err
		}
		out[t.Name] = sum
	}
	return out, nil
}
