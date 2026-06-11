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

package startertest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

const (
	envStarterDSN              = "TIDB_STARTER_TEST_DSN"
	envStarterStatusURL        = "TIDB_STARTER_STATUS_URL"
	envStarterPDStatusURL      = "TIDB_STARTER_PD_STATUS_URL"
	envStarterMaxAllowedPacket = "TIDB_STARTER_MAX_ALLOWED_PACKET"
	envStarterTiKVWorkerURL    = "TIDB_STARTER_TIKV_WORKER_URL"
	envStarterKeyspaceName     = "TIDB_STARTER_KEYSPACE_NAME"
	envStarterStandbyActivated = "TIDB_STARTER_ACTIVATED_FROM_STANDBY"
	envStarterActivateExportID = "TIDB_STARTER_ACTIVATE_EXPORT_ID"
	envStarterRunExitWaitTest  = "TIDB_STARTER_RUN_EXIT_WAIT_TEST"
	envStarterKeyspaceObs      = "TIDB_STARTER_KEYSPACE_OBSERVABILITY"
	envStarterMetaTenant       = "TIDB_STARTER_KEYSPACE_META_TENANT"
	envStarterMetaProject      = "TIDB_STARTER_KEYSPACE_META_PROJECT"

	starterServiceScope = "dxf_service"
)

type starterLabelRule struct {
	ID string `json:"id"`
}

type starterPoolStatus struct {
	State        string `json:"state"`
	KeyspaceName string `json:"keyspace_name"`
	ExportID     string `json:"export_id"`
}

type starterKeyspaceObservabilityField struct {
	Source       string `json:"source"`
	MetricLabel  string `json:"metric-label"`
	SlowLogField string `json:"slow-log-field"`
	StmtLogField string `json:"stmt-log-field"`
	Required     bool   `json:"required"`
}

type starterAutoIDOwnerStatus struct {
	IsOwner *bool `json:"is_owner"`
}

func TestExternalStarterConfigEndpoint(t *testing.T) {
	statusURL := requireStarterStatusURL(t)
	expectedMaxAllowedPacket := requireStarterMaxAllowedPacket(t)
	expectedTiKVWorkerURL := requireStarterTiKVWorkerURL(t)
	expectedKeyspace := requireStarterKeyspaceName(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL+"/config", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var cfg struct {
		DeployMode       string `json:"deploy-mode"`
		MaxAllowedPacket uint64 `json:"max-allowed-packet"`
		KeyspaceName     string `json:"keyspace-name"`
		Store            string `json:"store"`
		TiKVWorkerURL    string `json:"tikv-worker-url"`
		Standby          struct {
			EnableZeroBackend bool `json:"enable-zero-backend"`
		} `json:"standby"`
		Instance struct {
			TiDBServiceScope string `json:"tidb_service_scope"`
		} `json:"instance"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&cfg))

	require.Equal(t, "starter", cfg.DeployMode)
	require.EqualValues(t, expectedMaxAllowedPacket, cfg.MaxAllowedPacket)
	require.Equal(t, expectedKeyspace, cfg.KeyspaceName)
	require.Equal(t, "tikv", cfg.Store)
	require.Equal(t, expectedTiKVWorkerURL, cfg.TiKVWorkerURL)
	requireHostPort(t, cfg.TiKVWorkerURL)
	require.True(t, cfg.Standby.EnableZeroBackend)
	require.Equal(t, starterServiceScope, cfg.Instance.TiDBServiceScope)
}

func TestExternalStarterStandbyActivationStatusIncludesExportID(t *testing.T) {
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	expectedKeyspace := requireStarterKeyspaceName(t)
	expectedExportID := requireStarterActivateExportID(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/status")
	require.Equal(t, http.StatusOK, statusCode)
	var status starterPoolStatus
	require.NoError(t, json.Unmarshal(body, &status))
	require.Equal(t, "activated", status.State)
	require.Equal(t, expectedKeyspace, status.KeyspaceName)
	require.Equal(t, expectedExportID, status.ExportID)

	statusCode, body = getStarterStatusPath(ctx, t, statusURL, "/config")
	require.Equal(t, http.StatusOK, statusCode)
	var cfg struct {
		StarterParams struct {
			ExportID string `json:"export-id"`
		} `json:"starter-params"`
	}
	require.NoError(t, json.Unmarshal(body, &cfg))
	require.Equal(t, expectedExportID, cfg.StarterParams.ExportID)
}

func TestExternalStarterKeyspaceObservabilityFromActivationMetadata(t *testing.T) {
	requireStarterKeyspaceObservability(t)
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	expectedKeyspace := requireStarterKeyspaceName(t)
	expectedTenant := requireStarterMetaTenant(t)
	expectedProject := requireStarterMetaProject(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/config")
	require.Equal(t, http.StatusOK, statusCode)
	var cfg struct {
		KeyspaceName          string `json:"keyspace-name"`
		KeyspaceObservability struct {
			Fields []starterKeyspaceObservabilityField `json:"fields"`
		} `json:"keyspace-observability"`
	}
	require.NoError(t, json.Unmarshal(body, &cfg))
	require.Equal(t, expectedKeyspace, cfg.KeyspaceName)
	requireStarterObservabilityField(t, cfg.KeyspaceObservability.Fields, starterKeyspaceObservabilityField{
		Source:       "tenant",
		MetricLabel:  "keyspace_meta_tenant",
		SlowLogField: "Keyspace_meta_tenant",
		StmtLogField: "tenant",
		Required:     true,
	})
	requireStarterObservabilityField(t, cfg.KeyspaceObservability.Fields, starterKeyspaceObservabilityField{
		Source:       "project",
		MetricLabel:  "keyspace_meta_project",
		SlowLogField: "Keyspace_meta_project",
		StmtLogField: "project",
		Required:     true,
	})

	db := openStarterDB(t)
	require.NoError(t, db.PingContext(ctx))
	require.Equal(t, "1", queryString(ctx, t, db, "select 1"))

	statusCode, body = getStarterStatusPath(ctx, t, statusURL, "/metrics")
	require.Equal(t, http.StatusOK, statusCode)
	metrics := string(body)
	require.Contains(t, metrics, fmt.Sprintf(`keyspace_name="%s"`, expectedKeyspace))
	require.Contains(t, metrics, fmt.Sprintf(`keyspace_meta_tenant="%s"`, expectedTenant))
	require.Contains(t, metrics, fmt.Sprintf(`keyspace_meta_project="%s"`, expectedProject))
}

func TestExternalStarterAutoIDOwnerEndpoint(t *testing.T) {
	statusURL := requireStarterStatusURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := queryStarterAutoIDOwner(ctx, statusURL)
	require.NoError(t, err)
}

func TestExternalStarterExitRejectsInvalidOptions(t *testing.T) {
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	keyspaceName := requireStarterKeyspaceName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name  string
		query url.Values
		want  string
	}{
		{
			name: "invalid graceful",
			query: url.Values{
				"graceful": {"maybe"},
			},
			want: "invalid graceful\n",
		},
		{
			name: "wait duration above max",
			query: url.Values{
				"wait": {"24h1s"},
			},
			want: "invalid wait\n",
		},
		{
			name: "wait legacy seconds above max",
			query: url.Values{
				"wait": {"86401"},
			},
			want: "invalid wait\n",
		},
		{
			name: "negative wait",
			query: url.Values{
				"wait": {"-1"},
			},
			want: "invalid wait\n",
		},
		{
			name: "overflow wait",
			query: url.Values{
				"wait": {"9223372036854775807"},
			},
			want: "invalid wait\n",
		},
		{
			name: "invalid skip auto id owner",
			query: url.Values{
				"skip_auto_id_owner": {"maybe"},
			},
			want: "invalid skip_auto_id_owner\n",
		},
		{
			name: "invalid need manager free",
			query: url.Values{
				"need_mgr_free": {"maybe"},
			},
			want: "invalid need_mgr_free\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.query.Set("keyspace", keyspaceName)
			statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/exit?"+tt.query.Encode())
			require.Equal(t, http.StatusBadRequest, statusCode)
			require.Equal(t, tt.want, string(body))
		})
	}
}

func TestExternalStarterExitRejectsMismatchedKeyspace(t *testing.T) {
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	expectedKeyspace := requireStarterKeyspaceName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	remoteKeyspace := otherStarterKeyspaceName(expectedKeyspace)
	query := url.Values{
		"keyspace": {remoteKeyspace},
		"graceful": {"true"},
	}
	statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/exit?"+query.Encode())
	require.Equal(t, http.StatusPreconditionFailed, statusCode)
	var mismatch struct {
		Remote string `json:"remote"`
		Local  string `json:"local"`
	}
	require.NoError(t, json.Unmarshal(body, &mismatch))
	require.Equal(t, remoteKeyspace, mismatch.Remote)
	require.Equal(t, expectedKeyspace, mismatch.Local)
}

func TestExternalStarterExitWaitAndManagerNotifierContracts(t *testing.T) {
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	keyspaceName := requireStarterKeyspaceName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name string
		wait string
	}{
		{name: "without wait"},
		{name: "zero seconds", wait: "0"},
		{name: "zero duration", wait: "0s"},
		{name: "duration", wait: "1s"},
		{name: "compound duration", wait: "1h30m"},
		{name: "legacy seconds", wait: "60"},
		{name: "max duration", wait: "24h"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := url.Values{
				"keyspace":      {keyspaceName},
				"graceful":      {"true"},
				"need_mgr_free": {"true"},
			}
			if tt.wait != "" {
				query.Set("wait", tt.wait)
			}
			statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/exit?"+query.Encode())
			require.Equal(t, http.StatusServiceUnavailable, statusCode)
			require.Equal(t, "manager notifier is unavailable\n", string(body))
		})
	}

	if os.Getenv(envStarterRunExitWaitTest) != "1" {
		db := openStarterDB(t)
		require.NoError(t, db.PingContext(ctx))
	}

	t.Run("graceful_exit_waits_for_open_connection", func(t *testing.T) {
		requireStarterExitWaitTestEnabled(t)
		runExternalStarterGracefulExitWaitsForOpenConnection(t, statusURL, keyspaceName)
	})
}

func TestExternalStarterExitSkipsAutoIDOwner(t *testing.T) {
	requireStarterActivatedFromStandby(t)
	statusURL := requireStarterStatusURL(t)
	keyspaceName := requireStarterKeyspaceName(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !waitForStarterAutoIDOwner(ctx, statusURL) {
		t.Skip("external starter tidb-server did not become auto ID owner before timeout")
	}

	query := url.Values{
		"keyspace":           {keyspaceName},
		"skip_auto_id_owner": {"true"},
	}
	statusCode, _ := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/exit?"+query.Encode())
	require.Equal(t, http.StatusNotModified, statusCode)

	db := openStarterDB(t)
	require.NoError(t, db.PingContext(ctx))
}

func TestExternalStarterSysVarContracts(t *testing.T) {
	db := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	expectedMaxAllowedPacket := requireStarterMaxAllowedPacket(t)
	require.NoError(t, db.PingContext(ctx))
	require.NoError(t, execSQL(ctx, db, "create database if not exists starter_external"))
	require.NoError(t, execSQL(ctx, db, "drop table if exists starter_external.contract"))
	require.NoError(t, execSQL(ctx, db, "create table starter_external.contract (id int primary key, v varchar(32))"))
	require.NoError(t, execSQL(ctx, db, "insert into starter_external.contract values (1, 'starter')"))

	require.EqualValues(t, expectedMaxAllowedPacket, queryInt(ctx, t, db, "select @@global.max_allowed_packet"))
	require.EqualValues(t, expectedMaxAllowedPacket, queryInt(ctx, t, db, "select @@session.max_allowed_packet"))
	require.Equal(t, starterServiceScope, queryString(ctx, t, db, "select @@global.tidb_service_scope"))
	require.Equal(t, "starter", queryString(ctx, t, db, "select v from starter_external.contract where id = 1"))

	requireErrorContains(t, execSQL(ctx, db, "set @@global.max_allowed_packet = 16384"),
		"SET GLOBAL max_allowed_packet is not supported in starter deployment mode")

	require.Contains(t, []string{"1", "ON"}, strings.ToUpper(queryString(ctx, t, db, "select @@global.require_secure_transport")))
	requireErrorContains(t, execSQL(ctx, db, "set @@global.require_secure_transport = on"),
		"require_secure_transport can not be set in starter mode")
	requireErrorContains(t, execSQL(ctx, db, "set @@global.require_secure_transport = off"),
		"require_secure_transport can not be set in starter mode")
}

func TestExternalStarterMaxAllowedPacketIsEnforcedAtProtocolBoundary(t *testing.T) {
	db := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	maxAllowedPacket := requireStarterMaxAllowedPacket(t)
	oversizedSQL := fmt.Sprintf("select '%s'", strings.Repeat("a", maxAllowedPacket+1024))
	err := execSQL(ctx, db, oversizedSQL)
	require.Error(t, err)
	errText := strings.ToLower(err.Error())
	require.Truef(t,
		strings.Contains(errText, "max_allowed_packet") ||
			strings.Contains(errText, "packet bigger") ||
			strings.Contains(errText, "invalid connection"),
		"unexpected error: %v", err)
}

func TestExternalStarterSessionStatesRoundTrip(t *testing.T) {
	source := openStarterDB(t)
	target := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, execSQL(ctx, source, "set @starter_state = 'external-starter'"))
	require.NoError(t, execSQL(ctx, source, "set timestamp = 100"))

	var state string
	var token sql.NullString
	require.NoError(t, source.QueryRowContext(ctx, "show session_states").Scan(&state, &token))
	require.NotEmpty(t, state)

	require.NoError(t, execSQL(ctx, target, fmt.Sprintf("set session_states %q", state)))
	require.Equal(t, "external-starter", queryString(ctx, t, target, "select @starter_state"))
	require.Equal(t, "100", queryString(ctx, t, target, "select @@timestamp"))
}

func TestExternalStarterUsernamePrefixContracts(t *testing.T) {
	db := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))

	keyspaceName := requireStarterKeyspaceName(t)
	prefixError := fmt.Sprintf("User name must start with `%s.`", keyspaceName)
	userName := keyspaceName + ".ext_starter_user"
	dotUserName := keyspaceName + ".ext.starter_user"
	invalidRenamedUserName := "ext_starter_renamed"
	roleName := keyspaceName + ".ext_starter_role"
	wrongKeyspaceUserName := otherStarterKeyspaceName(keyspaceName) + ".ext_starter_user"

	cleanupStarterUsernamePrefixData(ctx, t, db, keyspaceName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupStarterUsernamePrefixData(cleanupCtx, t, db, keyspaceName)
	})

	requireErrorContains(t,
		execSQL(ctx, db, "create user `ext_starter_reject`@`%` identified by 'starter_pwd'"),
		prefixError)
	requireErrorContains(t,
		execSQL(ctx, db, "create role `ext_starter_role_reject`"),
		prefixError)

	require.NoError(t, execSQL(ctx, db, fmt.Sprintf("create user %s@%s identified by 'starter_pwd1'",
		quoteSQLIdentifier(userName), quoteSQLIdentifier("%"))))
	require.NoError(t, execSQL(ctx, db, fmt.Sprintf("create user %s@%s identified by 'starter_dot_pwd'",
		quoteSQLIdentifier(dotUserName), quoteSQLIdentifier("%"))))
	require.NoError(t, execSQL(ctx, db, fmt.Sprintf("create role %s", quoteSQLIdentifier(roleName))))

	requireErrorContains(t,
		execSQL(ctx, db, fmt.Sprintf("rename user %s@%s to %s@%s",
			quoteSQLIdentifier(userName), quoteSQLIdentifier("%"), quoteSQLIdentifier(invalidRenamedUserName), quoteSQLIdentifier("%"))),
		prefixError)

	require.NoError(t, execSQL(ctx, db, "grant ext_starter_role to ext_starter_user"))
	require.Equal(t, userName, queryString(ctx, t, db, fmt.Sprintf(
		"select TO_USER from mysql.role_edges where FROM_USER=%s and TO_USER=%s and TO_HOST=%s",
		quoteSQLString(roleName), quoteSQLString(userName), quoteSQLString("%"))))

	require.NoError(t, execSQL(ctx, db, "revoke ext_starter_role from ext_starter_user"))
	require.Equal(t, 0, queryInt(ctx, t, db, fmt.Sprintf(
		"select count(*) from mysql.role_edges where FROM_USER=%s and TO_USER=%s and TO_HOST=%s",
		quoteSQLString(roleName), quoteSQLString(userName), quoteSQLString("%"))))

	require.NoError(t, execSQL(ctx, db, "grant ext_starter_role to ext_starter_user"))
	require.NoError(t, execSQL(ctx, db, "set default role ext_starter_role to ext_starter_user"))
	require.Equal(t, roleName, queryString(ctx, t, db, fmt.Sprintf(
		"select DEFAULT_ROLE_USER from mysql.default_roles where USER=%s and DEFAULT_ROLE_USER=%s",
		quoteSQLString(userName), quoteSQLString(roleName))))

	require.NoError(t, execSQL(ctx, db, "alter user ext_starter_user identified by 'starter_pwd2'"))
	require.NoError(t, execSQL(ctx, db, "alter user `ext.starter_user`@`%` identified by 'starter_dot_pwd2'"))

	userDB := openStarterDBAs(t, "ext_starter_user", "starter_pwd2")
	require.NoError(t, userDB.PingContext(ctx))
	require.Equal(t, userName+"@%", queryString(ctx, t, userDB, "select current_user()"))

	dotUserDB := openStarterDBAs(t, "ext.starter_user", "starter_dot_pwd2")
	require.NoError(t, dotUserDB.PingContext(ctx))
	require.Equal(t, dotUserName+"@%", queryString(ctx, t, dotUserDB, "select current_user()"))

	wrongKeyspaceDB := openStarterDBAs(t, wrongKeyspaceUserName, "starter_pwd2")
	requireErrorContains(t, wrongKeyspaceDB.PingContext(ctx), "User name prefix does not match the assigned keyspace")
}

func TestExternalStarterAttributesUseKeyspaceScopedLabelRules(t *testing.T) {
	db := openStarterDB(t)
	pdStatusURL := requireStarterPDStatusURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))

	const schemaName = "starter_external_attrs"
	cleanupStarterAttributeData(ctx, t, db)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupStarterAttributeData(cleanupCtx, t, db)
	})

	keyspaceID := queryString(ctx, t, db, "select keyspace_id from information_schema.keyspace_meta")
	require.NotEmpty(t, keyspaceID)

	require.NoError(t, execSQL(ctx, db, "create database starter_external_attrs"))
	require.NoError(t, execSQL(ctx, db, `create table starter_external_attrs.attr_t (c int)
partition by range (c) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`))
	require.NoError(t, execSQL(ctx, db, `alter table starter_external_attrs.attr_t attributes="merge_option=allow,purpose=starter_table"`))
	require.NoError(t, execSQL(ctx, db, `alter table starter_external_attrs.attr_t partition p0 attributes="merge_option=deny,purpose=starter_partition"`))

	require.Equal(t, `"merge_option=allow","purpose=starter_table"`, queryString(ctx, t, db,
		"select attributes from information_schema.attributes where id='schema/starter_external_attrs/attr_t'"))
	require.Equal(t, `"merge_option=deny","purpose=starter_partition"`, queryString(ctx, t, db,
		"select attributes from information_schema.attributes where id='schema/starter_external_attrs/attr_t/p0'"))
	require.Equal(t, 0, queryInt(ctx, t, db,
		"select count(*) from information_schema.attributes where id like 'keyspace/%/schema/starter_external_attrs/%'"))

	rules := queryStarterLabelRules(ctx, t, pdStatusURL)
	requireStarterLabelRuleID(t, rules, fmt.Sprintf("keyspace/%s/schema/%s/attr_t", keyspaceID, schemaName))
	requireStarterLabelRuleID(t, rules, fmt.Sprintf("keyspace/%s/schema/%s/attr_t/p0", keyspaceID, schemaName))
	requireNoStarterLabelRuleID(t, rules, fmt.Sprintf("schema/%s/attr_t", schemaName))
	requireNoStarterLabelRuleID(t, rules, fmt.Sprintf("schema/%s/attr_t/p0", schemaName))
}

func openStarterDB(t *testing.T) *sql.DB {
	t.Helper()
	cfg := requireStarterDSNConfig(t)
	return openStarterDBWithDSN(t, cfg.FormatDSN())
}

func openStarterDBAs(t *testing.T, user, password string) *sql.DB {
	t.Helper()
	cfg := requireStarterDSNConfig(t)
	cfg.User = user
	cfg.Passwd = password
	return openStarterDBWithDSN(t, cfg.FormatDSN())
}

func requireStarterDSNConfig(t *testing.T) *mysql.Config {
	t.Helper()
	dsn := os.Getenv(envStarterDSN)
	if dsn == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterDSN)
	}
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	return cfg
}

func openStarterDBWithDSN(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Some tests intentionally trigger protocol errors that leave the connection bad.
		_ = db.Close()
	})
	return db
}

func requireStarterStatusURL(t *testing.T) string {
	t.Helper()
	statusURL := strings.TrimRight(os.Getenv(envStarterStatusURL), "/")
	if statusURL == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterStatusURL)
	}
	return statusURL
}

func requireStarterPDStatusURL(t *testing.T) string {
	t.Helper()
	statusURL := strings.TrimRight(os.Getenv(envStarterPDStatusURL), "/")
	if statusURL == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterPDStatusURL)
	}
	return statusURL
}

func requireStarterMaxAllowedPacket(t *testing.T) int {
	t.Helper()
	raw := os.Getenv(envStarterMaxAllowedPacket)
	if raw == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterMaxAllowedPacket)
	}
	v, err := strconv.Atoi(raw)
	require.NoError(t, err)
	return v
}

func requireStarterTiKVWorkerURL(t *testing.T) string {
	t.Helper()
	value := os.Getenv(envStarterTiKVWorkerURL)
	if value == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterTiKVWorkerURL)
	}
	requireHostPort(t, value)
	return value
}

func requireStarterKeyspaceName(t *testing.T) string {
	t.Helper()
	value := os.Getenv(envStarterKeyspaceName)
	if value == "" {
		return "SYSTEM"
	}
	return value
}

func requireStarterActivatedFromStandby(t *testing.T) {
	t.Helper()
	if os.Getenv(envStarterStandbyActivated) != "1" {
		t.Skipf("%s is not 1; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh with STARTER_STANDBY_MODE=1", envStarterStandbyActivated)
	}
}

func requireStarterActivateExportID(t *testing.T) string {
	t.Helper()
	value := os.Getenv(envStarterActivateExportID)
	if value == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterActivateExportID)
	}
	return value
}

func requireStarterExitWaitTestEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(envStarterRunExitWaitTest) != "1" {
		t.Skipf("%s is not 1; the destructive exit-wait case is run as the final script phase only", envStarterRunExitWaitTest)
	}
}

func requireStarterKeyspaceObservability(t *testing.T) {
	t.Helper()
	if os.Getenv(envStarterKeyspaceObs) != "1" {
		t.Skipf("%s is not 1; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh with starter keyspace observability enabled", envStarterKeyspaceObs)
	}
}

func requireStarterMetaTenant(t *testing.T) string {
	t.Helper()
	value := os.Getenv(envStarterMetaTenant)
	if value == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterMetaTenant)
	}
	return value
}

func requireStarterMetaProject(t *testing.T) string {
	t.Helper()
	value := os.Getenv(envStarterMetaProject)
	if value == "" {
		t.Skipf("%s is not set; run tests/realtikvtest/scripts/next-gen/run-starter-tests-with-server.sh", envStarterMetaProject)
	}
	return value
}

func requireHostPort(t *testing.T, value string) {
	t.Helper()
	host, port, err := net.SplitHostPort(value)
	require.NoError(t, err)
	require.NotEmpty(t, host)
	require.Regexp(t, `^[0-9]+$`, port)
}

func execSQL(ctx context.Context, db *sql.DB, query string) error {
	_, err := db.ExecContext(ctx, query)
	return err
}

func getStarterStatusPath(ctx context.Context, t *testing.T, statusURL, path string) (int, []byte) {
	t.Helper()
	statusCode, body, err := tryStarterStatusPath(ctx, statusURL, path)
	require.NoError(t, err)
	return statusCode, body
}

func tryStarterStatusPath(ctx context.Context, statusURL, path string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func runExternalStarterGracefulExitWaitsForOpenConnection(t *testing.T, statusURL, keyspaceName string) {
	t.Helper()
	db := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	require.NoError(t, conn.PingContext(ctx))

	const waitValue = "10s"
	query := url.Values{
		"keyspace": {keyspaceName},
		"graceful": {"true"},
		"wait":     {waitValue},
	}
	exitStart := time.Now()
	statusCode, body := getStarterStatusPath(ctx, t, statusURL, "/tidb-pool/exit?"+query.Encode())
	require.Equal(t, http.StatusOK, statusCode, string(body))

	require.NoError(t, waitForStarterStatusCode(ctx, statusURL, http.StatusInternalServerError))

	require.Eventually(t, func() bool {
		if time.Since(exitStart) < time.Second {
			return false
		}
		statusCode, _, err = tryStarterStatusPath(ctx, statusURL, "/status")
		return err == nil && statusCode == http.StatusInternalServerError
	}, 3*time.Second, 100*time.Millisecond, "tidb-server exited before the held connection was closed")
	require.Less(t, time.Since(exitStart), 10*time.Second)

	closeStart := time.Now()
	require.NoError(t, conn.Close())
	require.NoError(t, db.Close())
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	require.NoError(t, waitForStarterStatusUnavailable(shutdownCtx, statusURL))
	require.Less(t, time.Since(closeStart), 3*time.Second)
}

func waitForStarterStatusCode(ctx context.Context, statusURL string, want int) error {
	for {
		statusCode, _, err := tryStarterStatusPath(ctx, statusURL, "/status")
		if err == nil && statusCode == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for /status code %d: last status=%d, err=%v", want, statusCode, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForStarterStatusUnavailable(ctx context.Context, statusURL string) error {
	for {
		_, _, err := tryStarterStatusPath(ctx, statusURL, "/status")
		if err != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for /status to become unavailable")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func queryStarterAutoIDOwner(ctx context.Context, statusURL string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL+"/owner_manager/auto_id_service", nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("unexpected auto ID owner status code %d: %s", resp.StatusCode, string(body))
	}
	var status starterAutoIDOwnerStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return false, err
	}
	if status.IsOwner == nil {
		return false, fmt.Errorf("auto ID owner response missing is_owner")
	}
	return *status.IsOwner, nil
}

func waitForStarterAutoIDOwner(ctx context.Context, statusURL string) bool {
	for {
		isOwner, err := queryStarterAutoIDOwner(ctx, statusURL)
		if err == nil && isOwner {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cleanupStarterUsernamePrefixData(ctx context.Context, t *testing.T, db *sql.DB, keyspaceName string) {
	t.Helper()
	host := quoteSQLIdentifier("%")
	userNames := []string{
		keyspaceName + ".ext_starter_user",
		keyspaceName + ".ext.starter_user",
		keyspaceName + ".ext_starter_renamed",
		keyspaceName + ".ext_starter_reject",
		otherStarterKeyspaceName(keyspaceName) + ".ext_starter_user",
	}
	roleNames := []string{
		keyspaceName + ".ext_starter_role",
		keyspaceName + ".ext_starter_role_reject",
	}
	for _, userName := range userNames {
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf("drop user if exists %s@%s", quoteSQLIdentifier(userName), host)))
	}
	for _, roleName := range roleNames {
		require.NoError(t, execSQL(ctx, db, fmt.Sprintf("drop role if exists %s", quoteSQLIdentifier(roleName))))
	}
}

func otherStarterKeyspaceName(keyspaceName string) string {
	if strings.EqualFold(keyspaceName, "OTHER") {
		return "DIFFERENT"
	}
	return "OTHER"
}

func quoteSQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func cleanupStarterAttributeData(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	require.NoError(t, execSQL(ctx, db, "drop database if exists starter_external_attrs"))
}

func queryStarterLabelRules(ctx context.Context, t *testing.T, pdStatusURL string) []starterLabelRule {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pdStatusURL+"/pd/api/v1/config/region-label/rules", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var rules []starterLabelRule
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rules))
	return rules
}

func requireStarterLabelRuleID(t *testing.T, rules []starterLabelRule, id string) {
	t.Helper()
	for _, rule := range rules {
		if rule.ID == id {
			return
		}
	}
	require.Failf(t, "missing starter label rule", "rule ID %q not found in %v", id, rules)
}

func requireNoStarterLabelRuleID(t *testing.T, rules []starterLabelRule, id string) {
	t.Helper()
	for _, rule := range rules {
		require.NotEqual(t, id, rule.ID)
	}
}

func requireStarterObservabilityField(t *testing.T, fields []starterKeyspaceObservabilityField, want starterKeyspaceObservabilityField) {
	t.Helper()
	for _, field := range fields {
		if field == want {
			return
		}
	}
	require.Failf(t, "missing starter keyspace observability field", "field %+v not found in %+v", want, fields)
}

func queryString(ctx context.Context, t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
	return value
}

func queryInt(ctx context.Context, t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var value int
	require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
	return value
}

func requireErrorContains(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), contains)
}
