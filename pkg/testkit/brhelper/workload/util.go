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

package workload

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

func RandSuffix() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func QIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func QTable(schema, table string) string {
	return QIdent(schema) + "." + QIdent(table)
}

func ExecAll(ctx context.Context, db *sql.DB, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func SchemaExists(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", schema).Scan(&n)
	return n > 0, err
}

func TableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema, table).Scan(&n)
	return n > 0, err
}

func ColumnExists(ctx context.Context, db *sql.DB, schema, table, column string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?", schema, table, column).Scan(&n)
	return n > 0, err
}

func IndexExists(ctx context.Context, db *sql.DB, schema, table, index string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?", schema, table, index).Scan(&n)
	return n > 0, err
}

func TiFlashReplicaCount(ctx context.Context, db *sql.DB, schema, table string) (int, error) {
	var n sql.NullInt64
	err := db.QueryRowContext(ctx, "SELECT REPLICA_COUNT FROM information_schema.TIFLASH_REPLICA WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema, table).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !n.Valid {
		return 0, nil
	}
	return int(n.Int64), nil
}

type TableChecksum struct {
	TotalKvs         string `json:"total_kvs,omitempty"`
	TotalBytes       string `json:"total_bytes,omitempty"`
	ChecksumCRC64Xor string `json:"checksum_crc64_xor,omitempty"`
}

func (c *TableChecksum) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*c = TableChecksum{TotalKvs: s}
		return nil
	}
	type alias TableChecksum
	var v alias
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*c = TableChecksum(v)
	return nil
}

func AdminChecksumTable(ctx context.Context, db *sql.DB, schema, table string) (TableChecksum, error) {
	rows, err := db.QueryContext(ctx, "ADMIN CHECKSUM TABLE "+QTable(schema, table))
	if err != nil {
		return TableChecksum{}, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return TableChecksum{}, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return TableChecksum{}, err
		}
		return TableChecksum{}, fmt.Errorf("checksum: no rows returned")
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return TableChecksum{}, err
	}

	var out TableChecksum
	for i, c := range cols {
		v := strings.TrimSpace(string(raw[i]))
		switch {
		case strings.EqualFold(c, "Total_kvs"):
			out.TotalKvs = v
		case strings.EqualFold(c, "Total_bytes"):
			out.TotalBytes = v
		case strings.EqualFold(c, "Checksum_crc64_xor"):
			out.ChecksumCRC64Xor = v
		}
	}

	var missing []string
	if out.TotalKvs == "" {
		missing = append(missing, "Total_kvs")
	}
	if out.TotalBytes == "" {
		missing = append(missing, "Total_bytes")
	}
	if out.ChecksumCRC64Xor == "" {
		missing = append(missing, "Checksum_crc64_xor")
	}
	if len(missing) > 0 {
		return TableChecksum{}, fmt.Errorf("checksum: column(s) not found: %v; columns=%v", missing, cols)
	}
	return out, nil
}

func Require(cond bool, format string, args ...any) error {
	if cond {
		return nil
	}
	return fmt.Errorf(format, args...)
}

func EveryNTick(tick int, n int) bool {
	return n > 0 && tick%n == 0
}
