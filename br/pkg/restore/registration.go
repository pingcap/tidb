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

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

const (
	RegistrationDBName    string = "__TiDB_BR_Temporary_Restore_Registration_DB"
	RegistrationTableName        = "restore_registry"

	// createRegistrationTableSQL is the SQL to create the registration table
	createRegistrationTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		filter_strings TEXT NOT NULL,
		start_ts BIGINT UNSIGNED NOT NULL,
		restore_ts BIGINT UNSIGNED NOT NULL,
		upstream_cluster_id BIGINT UNSIGNED,
		register_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`

	// insertRegistrationSQL is the SQL to insert a new registration
	insertRegistrationSQL = `INSERT INTO %s.%s
		(filter_strings, start_ts, restore_ts, upstream_cluster_id)
		VALUES (?, ?, ?, ?)`

	// queryLastInsertIDSQL is the SQL to get the last inserted ID
	queryLastInsertIDSQL = `SELECT LAST_INSERT_ID()`

	// deleteRegistrationSQL is the SQL to delete a registration
	deleteRegistrationSQL = `DELETE FROM %s.%s WHERE id = ?`

	// selectRegistrationSQL is the SQL to get a specific registration
	selectRegistrationSQL = `SELECT
		id, filter_strings, start_ts, restore_ts, upstream_cluster_id, register_time
		FROM %s.%s WHERE id = ?`

	// selectRegistrationsByMaxIDSQL is the SQL to get all registrations with ID less than or equal to the given max ID
	selectRegistrationsByMaxIDSQL = `SELECT
		id, filter_strings, start_ts, restore_ts, upstream_cluster_id, register_time
		FROM %s.%s
		WHERE id <= ?
		ORDER BY id ASC`

	// lookupRestoreIDSQL is the SQL to find a restore ID matching specific criteria
	lookupRestoreIDSQL = `SELECT id FROM %s.%s
		WHERE filter_strings = ?
		AND start_ts = ?
		AND restore_ts = ?
		AND (upstream_cluster_id = ? OR (upstream_cluster_id IS NULL AND ? IS NULL))
		ORDER BY id DESC LIMIT 1`
)

// RegistrationInfo contains information about a registered restore
type RegistrationInfo struct {
	// filter patterns
	FilterStrings []string

	// time range for restore
	StartTS   uint64
	RestoreTS uint64

	// identifier of the upstream cluster
	UpstreamClusterID uint64
}

// String returns a string representation of the RegistrationInfo
func (info RegistrationInfo) String() string {
	return fmt.Sprintf("Filters: %v, Range: %d->%d, UpstreamClusterID: %d",
		info.FilterStrings, info.StartTS, info.RestoreTS, info.UpstreamClusterID)
}

// Registry manages registrations of restore tasks
type Registry struct {
	se glue.Session
}

// NewRestoreRegistry creates a new registry using TiDB's session
func NewRestoreRegistry(g glue.Glue, dom *domain.Domain) (*Registry, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Registry{
		se: se,
	}, nil
}

// Close releases resources used by the registry
func (r *Registry) Close() {
	if r.se != nil {
		r.se.Close()
		r.se = nil
	}
}

// createTableIfNotExist ensures that the registration table exists
func (r *Registry) createTableIfNotExist(ctx context.Context) error {
	createStmt := fmt.Sprintf(createRegistrationTableSQL, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, "CREATE DATABASE IF NOT EXISTS %n;", RegistrationDBName); err != nil {
		return errors.Annotatef(err, "failed to create registration database")
	}

	if err := r.se.ExecuteInternal(ctx, createStmt); err != nil {
		return errors.Annotatef(err, "failed to create registration table")
	}

	log.Info("ensured restore registration table exists",
		zap.String("db", RegistrationDBName),
		zap.String("table", RegistrationTableName))

	return nil
}

// registerRestore stores a restore filter registration in the database and returns the assigned ID
func (r *Registry) registerRestore(ctx context.Context, info RegistrationInfo) (uint64, error) {
	// convert filter strings to a comma-separated string
	filterStrings := strings.Join(info.FilterStrings, ",")
	insertStmt := fmt.Sprintf(insertRegistrationSQL, RegistrationDBName, RegistrationTableName)

	// Get the session's executor for transaction management
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()

	// Start a transaction to ensure atomicity
	_, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		"BEGIN",
	)
	if err != nil {
		return 0, errors.Annotate(err, "failed to begin transaction for registration")
	}

	// Ensure proper transaction cleanup
	var restoreID uint64
	defer func() {
		if err != nil {
			// If any error occurred, roll back the transaction
			execCtx.ExecRestrictedSQL(
				kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
				nil,
				"ROLLBACK",
			)
		}
	}()

	// Execute the insert statement within the transaction
	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		insertStmt,
		filterStrings,
		info.StartTS,
		info.RestoreTS,
		info.UpstreamClusterID,
	)
	if err != nil {
		return 0, errors.Annotate(err, "failed to store registration")
	}

	// Retrieve the auto-generated ID within the same transaction
	rows, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		queryLastInsertIDSQL,
	)
	if err != nil {
		return 0, errors.Annotate(err, "failed to get last insert ID")
	}

	if len(rows) == 0 {
		err = errors.New("failed to retrieve last insert ID: no rows returned")
		return 0, err
	}

	restoreID = rows[0].GetUint64(0)
	if restoreID == 0 {
		err = errors.New("failed to retrieve valid last insert ID")
		return 0, err
	}

	// Commit the transaction
	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		"COMMIT",
	)
	if err != nil {
		return 0, errors.Annotate(err, "failed to commit transaction for registration")
	}

	log.Info("registered restore",
		zap.Uint64("restore_id", restoreID),
		zap.Strings("filters", info.FilterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restore_ts", info.RestoreTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID))

	return restoreID, nil
}

// Unregister removes a restore registration
func (r *Registry) Unregister(ctx context.Context, restoreID uint64) error {
	deleteStmt := fmt.Sprintf(deleteRegistrationSQL, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, deleteStmt, restoreID); err != nil {
		return errors.Annotatef(err, "failed to unregister restore %d", restoreID)
	}

	log.Info("unregistered restore filter", zap.Uint64("restore_id", restoreID))
	return nil
}

// IsRegistrationDBName checks if the database name is the registration database
func IsRegistrationDBName(dbname ast.CIStr) bool {
	return dbname.O == RegistrationDBName
}

// GetRegistrationsByMaxID returns all registrations with IDs smaller than or equal to maxID
func (r *Registry) GetRegistrationsByMaxID(ctx context.Context, maxID uint64) ([]RegistrationInfo, error) {
	// Ensure the table exists before querying
	if err := r.createTableIfNotExist(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	selectStmt := fmt.Sprintf(selectRegistrationsByMaxIDSQL, RegistrationDBName, RegistrationTableName)
	registrations := make([]RegistrationInfo, 0)

	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		selectStmt,
		maxID,
	)
	if errSQL != nil {
		return nil, errors.Annotatef(errSQL, "failed to query registrations with max ID %d", maxID)
	}

	// Process all registrations from the results
	for _, row := range rows {
		var (
			filterStrings     = row.GetString(1)
			startTS           = row.GetUint64(2)
			restoreTS         = row.GetUint64(3)
			upstreamClusterID = row.GetUint64(4)
		)

		info := RegistrationInfo{
			FilterStrings:     strings.Split(filterStrings, ","),
			StartTS:           startTS,
			RestoreTS:         restoreTS,
			UpstreamClusterID: upstreamClusterID,
		}

		registrations = append(registrations, info)

	}

	return registrations, nil
}

// LookupRestoreID finds the ID of a registration that matches the given criteria
func (r *Registry) LookupRestoreID(ctx context.Context, info RegistrationInfo) (uint64, error) {
	// Ensure the table exists before querying
	if err := r.createTableIfNotExist(ctx); err != nil {
		return 0, errors.Trace(err)
	}

	// Convert filter strings to comma-separated string for matching
	filterStrings := strings.Join(info.FilterStrings, ",")
	lookupStmt := fmt.Sprintf(lookupRestoreIDSQL, RegistrationDBName, RegistrationTableName)

	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		lookupStmt,
		filterStrings,
		info.StartTS,
		info.RestoreTS,
		info.UpstreamClusterID,
		info.UpstreamClusterID,
	)
	if err != nil {
		return 0, errors.Annotate(err, "failed to lookup restore ID")
	}

	if len(rows) == 0 {
		return 0, errors.New("no matching registration found")
	}

	restoreID := rows[0].GetUint64(0)
	if restoreID == 0 {
		return 0, errors.New("invalid restore ID (0) returned from lookup")
	}

	log.Info("found matching restore registration",
		zap.Uint64("restore_id", restoreID),
		zap.Strings("filters", info.FilterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restore_ts", info.RestoreTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID))

	return restoreID, nil
}

// GetOrCreateRegistration looks up a registration that matches the given criteria
// If no match is found, it creates a new registration
// Returns: the restore ID and a boolean indicating if a new registration was created
// The boolean is true if a new registration was created, false if an existing one was found
func (r *Registry) GetOrCreateRegistration(ctx context.Context, info RegistrationInfo) (uint64, error) {
	// Try to find existing registration first
	restoreID, err := r.LookupRestoreID(ctx, info)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if restoreID > 0 {
		// Found existing registration
		log.Info("using existing restore registration",
			zap.Uint64("restore_id", restoreID),
			zap.Strings("filters", info.FilterStrings),
			zap.Uint64("start_ts", info.StartTS),
			zap.Uint64("restore_ts", info.RestoreTS),
			zap.Uint64("upstream_cluster_id", info.UpstreamClusterID))
		return restoreID, nil
	}

	// Not found, create a new registration
	log.Info("no matching registration found, creating new one",
		zap.Strings("filters", info.FilterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restore_ts", info.RestoreTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID))

	// Create new registration
	restoreID, err = r.registerRestore(ctx, info)
	if err != nil {
		return 0, errors.Annotate(err, "failed to create new registration")
	}

	return restoreID, nil
}
