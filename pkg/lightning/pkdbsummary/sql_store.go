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

package pkdbsummary

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// SQLStore persists changed source units in a sidecar table in the checkpoint
// schema. Existing checkpoint columns and normal import-control writes remain
// unchanged; explicit table resets also remove their corresponding source units.
type SQLStore struct {
	db    *sql.DB
	table string
}

// NewSQLStore creates a store; Load initializes the optional sidecar table.
func NewSQLStore(db *sql.DB, schema string) *SQLStore {
	return &SQLStore{db: db, table: "`" + strings.ReplaceAll(schema, "`", "``") + "`.pkdb_import_statistics_v1"}
}

// Load reconstructs a ledger from its metadata and independent chunk entries.
func (s *SQLStore) Load(ctx context.Context) ([]byte, error) {
	_, err := s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+s.table+" (id BINARY(32) PRIMARY KEY, unit_key TEXT NOT NULL, statistics LONGBLOB NOT NULL)")
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT unit_key,statistics FROM "+s.table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metadata []byte
	units := make(map[string]json.RawMessage)
	for rows.Next() {
		var key string
		var data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return nil, err
		}
		if key == "" {
			metadata = append([]byte(nil), data...)
		} else {
			units[key] = append(json.RawMessage(nil), data...)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &root); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("invalid import statistics metadata")
	}
	root["units"], err = json.Marshal(units)
	if err != nil {
		return nil, err
	}
	return json.Marshal(root)
}

// Save replaces a complete document, as required by the base Store interface.
func (s *SQLStore) Save(ctx context.Context, data []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return err
	}
	var rawUnits map[string]json.RawMessage
	if err := json.Unmarshal(root["units"], &rawUnits); err != nil {
		return err
	}
	entries := make(map[string][]byte, len(rawUnits))
	for key, unit := range rawUnits {
		entries[key] = unit
	}
	delete(root, "units")
	metadata, err := json.Marshal(root)
	if err != nil {
		return err
	}
	return s.SaveEntries(ctx, metadata, entries, true)
}

// SaveEntries commits one coherent metadata/dirty-unit snapshot. Hash keys keep
// arbitrarily long source paths out of index length limits; full keys are stored
// separately and reconstructed during Load.
func (s *SQLStore) SaveEntries(ctx context.Context, metadata []byte, entries map[string][]byte, reset bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Commit/statement errors remain primary; rollback after Commit returns ErrTxDone.
	defer func() { _ = tx.Rollback() }()
	if reset {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table); err != nil {
			return err
		}
	}
	query := "REPLACE INTO " + s.table + " (id,unit_key,statistics) VALUES (?,?,?)"
	write := func(key string, data []byte) error {
		id := sha256.Sum256([]byte(key))
		_, err := tx.ExecContext(ctx, query, id[:], key, data)
		return err
	}
	if err := write("", metadata); err != nil {
		return err
	}
	for key, data := range entries {
		if err := write(key, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RemoveTables discards source units for explicitly reset tables in the caller's
// checkpoint transaction. Other units and task metadata remain unchanged. The
// optional sidecar is not created; the caller can ignore a missing-table error.
func (s *SQLStore) RemoveTables(ctx context.Context, tx *sql.Tx, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	removed := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		removed[table] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, "SELECT unit_key,statistics FROM "+s.table+" WHERE unit_key <> '' FOR UPDATE")
	if err != nil {
		return err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		var data []byte
		if err := rows.Scan(&key, &data); err != nil {
			return err
		}
		var entry unit
		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}
		if _, ok := removed[entry.Table]; ok {
			keys = append(keys, key)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Finish the result set before issuing writes on the same SQL connection.
	if err := rows.Close(); err != nil {
		return err
	}
	// Bound parameters while avoiding one network round trip per removed chunk.
	// Every batch remains in the caller's coherent control/statistics transaction.
	const deleteBatchSize = 256
	for start := 0; start < len(keys); start += deleteBatchSize {
		end := min(start+deleteBatchSize, len(keys))
		args := make([]any, 0, end-start)
		for _, key := range keys[start:end] {
			id := sha256.Sum256([]byte(key))
			args = append(args, id[:])
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table+" WHERE id IN ("+placeholders+")", args...); err != nil {
			return err
		}
	}
	return nil
}
