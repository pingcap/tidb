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
