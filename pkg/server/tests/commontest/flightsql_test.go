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

package commontest

import (
	"crypto/x509"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

// setupFlightSQLCerts creates and configures session token signing certificates.
// This is required for all FlightSQL tests that use authentication.
func setupFlightSQLCerts(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "flight_cert.pem")
	keyPath := filepath.Join(tempDir, "flight_key.pem")
	err := util.CreateCertificates(certPath, keyPath, 1024, x509.RSA, x509.UnknownSignatureAlgorithm)
	require.NoError(t, err)

	// Configure session token signing
	cfg := config.GetGlobalConfig()
	cfg.Security.SessionTokenSigningCert = certPath
	cfg.Security.SessionTokenSigningKey = keyPath
}

// TestFlightSQLBasicAuth tests end-to-end authentication flow for Apache Arrow Flight SQL.
// This test demonstrates:
// 1. Creating a user via MySQL protocol
// 2. Authenticating via Flight SQL handshake
// 3. Receiving and using session tokens
// 4. Executing queries with token-based authentication
func TestFlightSQLBasicAuth(t *testing.T) {
	// Setup session token signing certificates
	setupFlightSQLCerts(t)

	// Create and start TiDB server with Flight SQL enabled
	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test user with password and test table via MySQL protocol
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		// Create user with default auth plugin (mysql_native_password)
		dbt.MustExec(`CREATE USER 'flighttest'@'%' IDENTIFIED BY 'test123'`)
		dbt.MustExec(`GRANT ALL ON test.* TO 'flighttest'@'%'`)
		dbt.MustExec(`CREATE TABLE test.test (id INT PRIMARY KEY)`)
		dbt.MustExec(`INSERT INTO test.test VALUES (1),(2),(3)`)
	})

	flightPort := ts.Server.FlightSQLPort()
	require.NotZero(t, flightPort, "FlightSQL server should be running")

	// Test 1: Root user with no password
	t.Run("RootUser", func(t *testing.T) {
		dsn := fmt.Sprintf("flightsql://root@localhost:%d", flightPort)
		db, err := sql.Open("flightsql", dsn)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.Query("SELECT * FROM test.test")
		require.NoError(t, err, "Query should succeed with root user")
		defer rows.Close()

		// Verify data
		actualIDs := []int{}
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			actualIDs = append(actualIDs, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{1, 2, 3}, actualIDs)
	})

	// Test 2: User with password
	t.Run("UserWithPassword", func(t *testing.T) {
		dsn := fmt.Sprintf("flightsql://flighttest:test123@localhost:%d", flightPort)
		db, err := sql.Open("flightsql", dsn)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.Query("SELECT * FROM test.test")
		require.NoError(t, err, "Query should succeed with valid credentials")
		defer rows.Close()

		// Verify data
		actualIDs := []int{}
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			actualIDs = append(actualIDs, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{1, 2, 3}, actualIDs)
	})

	// Test 3: Invalid password
	t.Run("InvalidPassword", func(t *testing.T) {
		badDSN := fmt.Sprintf("flightsql://flighttest:wrongpassword@localhost:%d", flightPort)
		badDB, err := sql.Open("flightsql", badDSN)
		require.NoError(t, err)
		defer badDB.Close()

		_, err = badDB.Query("SELECT 1")
		require.Error(t, err, "Query with invalid password should fail")
	})
}

// TestFlightSQLSessionToken tests that session tokens work correctly.
func TestFlightSQLSessionToken(t *testing.T) {
	// Setup session token signing certificates
	setupFlightSQLCerts(t)

	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test user
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'tokentest'@'%' IDENTIFIED BY 'pass123'`)
		dbt.MustExec(`GRANT ALL ON test.* TO 'tokentest'@'%'`)
	})

	// Connect with valid credentials
	dsn := fmt.Sprintf("flightsql://tokentest:pass123@localhost:%d", ts.Server.FlightSQLPort())
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Execute multiple queries - session token should be reused
	for i := 0; i < 5; i++ {
		rows, err := db.Query("SELECT 1")
		require.NoError(t, err, "Query should succeed with session token")
		rows.Close()
	}

	// TODO: Test token expiration after 1 minute
	// This would require waiting or using failpoint to mock time
}

// TestFlightSQLNoAuth tests that unauthenticated requests are rejected.
func TestFlightSQLNoAuth(t *testing.T) {
	// Setup session token signing certificates
	setupFlightSQLCerts(t)

	ts := servertestkit.CreateTidbTestSuite(t)

	// Try to connect without credentials
	// Note: DSN without username/password
	dsn := fmt.Sprintf("flightsql://localhost:%d", ts.Server.FlightSQLPort())
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Try to execute query without authentication
	_, err = db.Query("SELECT 1")
	require.Error(t, err, "Query should fail without authentication")
}

// TestFlightSQLDataTypes tests that various MySQL data types are correctly converted to Arrow types.
func TestFlightSQLDataTypes(t *testing.T) {
	setupFlightSQLCerts(t)
	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test table with various data types
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE TABLE test.types_test (
			id INT PRIMARY KEY,
			tiny_int TINYINT,
			small_int SMALLINT,
			medium_int MEDIUMINT,
			int_col INT,
			big_int BIGINT,
			unsigned_int INT UNSIGNED,
			float_col FLOAT,
			double_col DOUBLE,
			decimal_col DECIMAL(10, 2),
			varchar_col VARCHAR(100),
			text_col TEXT,
			date_col DATE,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP,
			json_col JSON,
			blob_col BLOB
		)`)

		dbt.MustExec(`INSERT INTO test.types_test VALUES (
			1,
			127,
			32767,
			8388607,
			2147483647,
			9223372036854775807,
			4294967295,
			3.14,
			2.718281828,
			123.45,
			'test varchar',
			'test text',
			'2024-01-15',
			'2024-01-15 10:30:00',
			'2024-01-15 10:30:00',
			'{"key": "value"}',
			'binary data'
		)`)
	})

	flightPort := ts.Server.FlightSQLPort()
	require.NotZero(t, flightPort)

	// Connect with root user
	dsn := fmt.Sprintf("flightsql://root@localhost:%d", flightPort)
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Query the data
	rows, err := db.Query("SELECT * FROM test.types_test WHERE id = 1")
	require.NoError(t, err, "Query failed: %v", err)
	defer rows.Close()

	// Get column types
	colTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, colTypes, 17)

	// Verify we can read the row
	if !rows.Next() {
		require.NoError(t, rows.Err(), "Failed to read row")
		require.Fail(t, "No rows returned from query")
	}

	// Scan values
	var (
		id, tinyInt, smallInt, mediumInt, intCol                     int
		bigInt, unsignedInt                                           int64
		floatCol                                                      float32
		doubleCol, decimalCol                                         float64
		varcharCol, textCol, dateCol, datetimeCol, timestampCol       string
		jsonCol, blobCol                                              []byte
	)

	err = rows.Scan(&id, &tinyInt, &smallInt, &mediumInt, &intCol, &bigInt, &unsignedInt,
		&floatCol, &doubleCol, &decimalCol, &varcharCol, &textCol,
		&dateCol, &datetimeCol, &timestampCol, &jsonCol, &blobCol)
	require.NoError(t, err)

	// Verify values
	require.Equal(t, 1, id)
	require.Equal(t, 127, tinyInt)
	require.Equal(t, 32767, smallInt)
	require.Equal(t, 2147483647, intCol)
	require.Greater(t, bigInt, int64(0))
	require.Equal(t, "test varchar", varcharCol)
	require.Equal(t, "test text", textCol)
}

// TestFlightSQLNullValues tests that NULL values are correctly handled.
func TestFlightSQLNullValues(t *testing.T) {
	setupFlightSQLCerts(t)
	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test table with nullable columns
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE TABLE test.null_test (
			id INT PRIMARY KEY,
			nullable_int INT,
			nullable_varchar VARCHAR(100),
			nullable_date DATE
		)`)

		// Insert row with NULLs
		dbt.MustExec(`INSERT INTO test.null_test VALUES (1, NULL, NULL, NULL)`)
		// Insert row with values
		dbt.MustExec(`INSERT INTO test.null_test VALUES (2, 42, 'test', '2024-01-15')`)
	})

	dsn := fmt.Sprintf("flightsql://root@localhost:%d", ts.Server.FlightSQLPort())
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Query rows with NULLs
	rows, err := db.Query("SELECT * FROM test.null_test ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	// First row: all nulls
	require.True(t, rows.Next())
	var (
		id                                          int
		nullableInt                                 sql.NullInt64
		nullableVarchar                             sql.NullString
		nullableDate                                sql.NullString
	)
	err = rows.Scan(&id, &nullableInt, &nullableVarchar, &nullableDate)
	require.NoError(t, err)
	require.Equal(t, 1, id)
	require.False(t, nullableInt.Valid)
	require.False(t, nullableVarchar.Valid)
	require.False(t, nullableDate.Valid)

	// Second row: all values
	require.True(t, rows.Next())
	err = rows.Scan(&id, &nullableInt, &nullableVarchar, &nullableDate)
	require.NoError(t, err)
	require.Equal(t, 2, id)
	require.True(t, nullableInt.Valid)
	require.Equal(t, int64(42), nullableInt.Int64)
	require.True(t, nullableVarchar.Valid)
	require.Equal(t, "test", nullableVarchar.String)
	require.True(t, nullableDate.Valid)
}

// TestFlightSQLPreparedStatement tests prepared statement functionality.
// Note: Parameter binding is not yet fully implemented, so we test basic prepared statement creation
func TestFlightSQLPreparedStatement(t *testing.T) {
	setupFlightSQLCerts(t)
	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test table via MySQL protocol
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE TABLE test.prep_test (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)`)
		dbt.MustExec(`INSERT INTO test.prep_test VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'charlie', 300)`)
	})

	dsn := fmt.Sprintf("flightsql://root@localhost:%d?timeout=10s", ts.Server.FlightSQLPort())
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Create prepared statement without parameters
	t.Run("PrepareSelectNoParams", func(t *testing.T) {
		stmt, err := db.Prepare("SELECT id, name, value FROM test.prep_test WHERE id = 2")
		require.NoError(t, err, "Failed to prepare statement")
		defer stmt.Close()

		// Execute prepared statement
		rows, err := stmt.Query()
		require.NoError(t, err, "Failed to execute prepared statement")
		defer rows.Close()

		// Verify result
		require.True(t, rows.Next(), "Expected at least one row")
		var id int
		var value int64
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		require.Equal(t, 2, id)
		require.Equal(t, "bob", name)
		require.Equal(t, int64(200), value)
	})

	// Test 2: Prepared statement with single parameter
	t.Run("PrepareWithParameter", func(t *testing.T) {
		stmt, err := db.Prepare("SELECT id, name, value FROM test.prep_test WHERE id = ?")
		require.NoError(t, err, "Failed to prepare statement with placeholder")
		defer stmt.Close()

		// Execute with parameter
		rows, err := stmt.Query(2)
		require.NoError(t, err, "Failed to execute prepared statement with parameter")
		defer rows.Close()

		// Verify result
		require.True(t, rows.Next(), "Expected at least one row")
		var id int
		var value int64
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		require.Equal(t, 2, id)
		require.Equal(t, "bob", name)
		require.Equal(t, int64(200), value)
	})

	// Test 3: Prepared INSERT with parameters
	t.Run("PrepareInsertWithParameters", func(t *testing.T) {
		stmt, err := db.Prepare("INSERT INTO test.prep_test VALUES (?, ?, ?)")
		require.NoError(t, err, "Failed to prepare INSERT statement")
		defer stmt.Close()

		// Execute with parameters
		result, err := stmt.Exec(4, "david", 400)
		require.NoError(t, err, "Failed to execute prepared INSERT")
		_ = result

		// Verify the insert worked
		rows, err := db.Query("SELECT name, value FROM test.prep_test WHERE id = 4")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		var value int64
		err = rows.Scan(&name, &value)
		require.NoError(t, err)
		require.Equal(t, "david", name)
		require.Equal(t, int64(400), value)
	})

	// Test 4: Multiple executions with different parameters
	t.Run("MultipleExecutionsWithParams", func(t *testing.T) {
		stmt, err := db.Prepare("SELECT name FROM test.prep_test WHERE id = ?")
		require.NoError(t, err)
		defer stmt.Close()

		// Execute with different parameters
		testCases := []struct {
			id           int
			expectedName string
		}{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}

		for _, tc := range testCases {
			rows, err := stmt.Query(tc.id)
			require.NoError(t, err, "Failed to query with id=%d", tc.id)

			require.True(t, rows.Next(), "Expected result for id=%d", tc.id)
			var name string
			err = rows.Scan(&name)
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, name, "Wrong name for id=%d", tc.id)
			rows.Close()
		}
	})
}

// TestFlightSQLStatementUpdate tests non-prepared DML statements (INSERT, UPDATE, DELETE).
func TestFlightSQLStatementUpdate(t *testing.T) {
	setupFlightSQLCerts(t)
	ts := servertestkit.CreateTidbTestSuite(t)

	// Create test table via MySQL protocol
	ts.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE TABLE test.update_test (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value INT
		)`)
		dbt.MustExec(`INSERT INTO test.update_test VALUES (1, 'initial', 100)`)
	})

	dsn := fmt.Sprintf("flightsql://root@localhost:%d?timeout=10s", ts.Server.FlightSQLPort())
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Direct INSERT
	t.Run("DirectInsert", func(t *testing.T) {
		result, err := db.Exec("INSERT INTO test.update_test VALUES (2, 'second', 200)")
		require.NoError(t, err, "Failed to execute INSERT")
		_ = result // TODO: Verify affected rows when implemented

		// Verify the insert worked
		rows, err := db.Query("SELECT name FROM test.update_test WHERE id = 2")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		require.Equal(t, "second", name)
	})

	// Test 2: Direct UPDATE
	t.Run("DirectUpdate", func(t *testing.T) {
		result, err := db.Exec("UPDATE test.update_test SET name = 'updated' WHERE id = 1")
		require.NoError(t, err, "Failed to execute UPDATE")
		_ = result

		// Verify the update worked
		rows, err := db.Query("SELECT name FROM test.update_test WHERE id = 1")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		require.Equal(t, "updated", name)
	})

	// Test 3: Direct DELETE
	t.Run("DirectDelete", func(t *testing.T) {
		result, err := db.Exec("DELETE FROM test.update_test WHERE id = 2")
		require.NoError(t, err, "Failed to execute DELETE")
		_ = result

		// Verify the delete worked
		rows, err := db.Query("SELECT COUNT(*) FROM test.update_test WHERE id = 2")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var count int64
		err = rows.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})
}
