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

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type diagnosticTestEnvelope struct {
	Dataset          string            `json:"dataset"`
	SnapshotID       string            `json:"snapshot_id"`
	Records          []json.RawMessage `json:"records"`
	NextCursor       string            `json:"next_cursor"`
	Complete         bool              `json:"complete"`
	RecordCount      int               `json:"record_count"`
	RedactionProfile string            `json:"redaction_profile"`
	RedactionVersion int               `json:"redaction_version"`
	RedactionKeyID   string            `json:"redaction_key_id"`
}

func TestDiagnosticAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE diagnostic_api")
	tk.MustExec("USE diagnostic_api")
	tk.MustExec(`CREATE TABLE t1 (
		id BIGINT PRIMARY KEY CLUSTERED,
		a VARCHAR(32),
		b INT,
		INDEX idx_a(a)
	) PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("CREATE TABLE t2 (id INT, generated_col INT AS (id + 7) STORED)")
	tk.MustExec("INSERT INTO t1 VALUES (1, 'secret-value', 2), (11, 'other-value', 3)")
	tk.MustExec("ANALYZE TABLE t1")
	tk.MustExec(`CREATE GLOBAL BINDING FOR
		SELECT * FROM t1 WHERE a = 'secret-binding-literal'
		USING SELECT /*+ USE_INDEX(t1, idx_a) */ * FROM t1 WHERE a = 'secret-binding-literal'`)

	cfg := config.NewConfig().DiagnosticAPI
	cfg.Enabled = true
	cfg.RequireMTLS = false
	cfg.DefaultPageSize = 1
	redactionKeyFile := filepath.Join(t.TempDir(), "diagnostic-redaction-key")
	require.NoError(t, os.WriteFile(redactionKeyFile, []byte("0123456789abcdef0123456789abcdef"), 0o600))
	cfg.RedactionKeyFile = redactionKeyFile
	cfg.RedactionKeyID = "test-key-1"
	handler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), cfg)
	require.NoError(t, err)

	t.Run("capabilities", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/internal/diagnostics/v1/capabilities", nil)
		recorder := httptest.NewRecorder()
		handler.serveCapabilities(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		var capabilities diagnosticCapabilities
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &capabilities))
		require.Equal(t, config.DiagnosticRedactionProfileStrict, capabilities.Redaction.Profile)
		require.Equal(t, diagnosticRedactionVersion, capabilities.Redaction.Version)
		require.Equal(t, "test-key-1", capabilities.Redaction.KeyID)
		require.Equal(t, uint64(4194304), capabilities.Limits.MaxResponseBytes)
		var tables diagnosticDatasetCapability
		for _, dataset := range capabilities.Datasets {
			if dataset.Name == "schema.tables" {
				tables = dataset
				break
			}
		}
		require.Equal(t, config.DiagnosticRedactionProfileStrict, tables.RedactionProfile)
		require.Equal(t, diagnosticRedactionVersion, tables.RedactionVersion)
		require.Contains(t, tables.Fields, "schema_name")
		require.Contains(t, tables.FieldPolicies, diagnosticFieldPolicy{
			Name: "schema_name", Class: diagnosticFieldClassIdentifier, Transform: diagnosticTransformPseudonymize,
		})
		require.Contains(t, tables.FieldPolicies, diagnosticFieldPolicy{
			Name: "comment", Class: diagnosticFieldClassUserContent, Transform: diagnosticTransformOmit,
		})
		for _, dataset := range capabilities.Datasets {
			page := requestDiagnosticTestPage(t, handler, dataset.Name, 100, "")
			require.NotEmpty(t, page.Records, dataset.Name)
			var record map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(page.Records[0], &record))
			actualFields := make([]string, 0, len(record))
			for field := range record {
				actualFields = append(actualFields, field)
			}
			require.ElementsMatch(t, dataset.Fields, actualFields, dataset.Name)
		}
	})

	t.Run("mtls required", func(t *testing.T) {
		mtlsCfg := cfg
		mtlsCfg.RequireMTLS = true
		mtlsHandler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), mtlsCfg)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodGet, "/internal/diagnostics/v1/capabilities", nil)
		recorder := httptest.NewRecorder()
		mtlsHandler.serveCapabilities(recorder, req)
		require.Equal(t, http.StatusUnauthorized, recorder.Code)
		require.Contains(t, recorder.Body.String(), "client_certificate_required")
	})

	t.Run("schema datasets paginate without duplicates", func(t *testing.T) {
		expectations := map[string]int{
			"schema.tables":     2,
			"schema.columns":    5,
			"schema.partitions": 2,
		}
		for dataset, expected := range expectations {
			records, bodies := collectAllDiagnosticTestRecords(t, handler, dataset, 1)
			require.Len(t, records, expected, dataset)
			joined := strings.Join(bodies, "")
			require.NotContains(t, joined, "secret-value")
			require.NotContains(t, joined, "generated_expr_string")
			require.NotContains(t, joined, "less_than")
			require.NotContains(t, joined, `"comment"`)
			require.NotContains(t, joined, `"schema_name":"mysql"`)
			require.NotContains(t, joined, `"schema_name":"diagnostic_api"`)
			require.NotContains(t, joined, `"table_name":"t1"`)
			require.NotContains(t, joined, "‹")
		}

		indexRecords, _ := collectAllDiagnosticTestRecords(t, handler, "schema.indexes", 1)
		require.NotEmpty(t, indexRecords)
		var names []string
		for _, raw := range indexRecords {
			var record struct {
				IndexName string `json:"index_name"`
			}
			require.NoError(t, json.Unmarshal(raw, &record))
			names = append(names, record.IndexName)
		}
		for _, name := range names {
			require.True(t, strings.HasPrefix(name, "index_"), name)
		}
		require.NotContains(t, names, "idx_a")

		tableAliases := diagnosticTestTableAliases(t, handler)
		columnRecords, _ := collectAllDiagnosticTestRecords(t, handler, "schema.columns", 100)
		for _, raw := range columnRecords {
			var record diagnosticColumnRecord
			require.NoError(t, json.Unmarshal(raw, &record))
			require.Equal(t, tableAliases[record.TableID], record.TableName)
			require.True(t, strings.HasPrefix(record.SchemaName, "schema_"), record.SchemaName)
			require.True(t, strings.HasPrefix(record.ColumnName, "column_"), record.ColumnName)
		}
	})

	t.Run("metadata readable profile requires explicit configuration", func(t *testing.T) {
		readableCfg := cfg
		readableCfg.RedactionProfile = config.DiagnosticRedactionProfileMetadataReadable
		readableCfg.RedactionKeyFile = ""
		readableCfg.RedactionKeyID = ""
		readableHandler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), readableCfg)
		require.NoError(t, err)
		records, _ := collectAllDiagnosticTestRecords(t, readableHandler, "schema.indexes", 100)
		var names []string
		for _, raw := range records {
			var record diagnosticIndexRecord
			require.NoError(t, json.Unmarshal(raw, &record))
			names = append(names, record.IndexName)
		}
		require.Contains(t, names, "idx_a")
		page := requestDiagnosticTestPage(t, readableHandler, "schema.tables", 1, "")
		require.Equal(t, config.DiagnosticRedactionProfileMetadataReadable, page.RedactionProfile)
		require.Equal(t, diagnosticRedactionVersion, page.RedactionVersion)
		require.Empty(t, page.RedactionKeyID)

		req := httptest.NewRequest(http.MethodGet, "/internal/diagnostics/v1/capabilities", nil)
		recorder := httptest.NewRecorder()
		readableHandler.serveCapabilities(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		var capabilities diagnosticCapabilities
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &capabilities))
		for _, dataset := range capabilities.Datasets {
			if dataset.Name != "schema.partitions" {
				continue
			}
			require.Contains(t, dataset.FieldPolicies, diagnosticFieldPolicy{
				Name: "partition_name", Class: diagnosticFieldClassIdentifier, Transform: diagnosticTransformPass,
			})
			require.Contains(t, dataset.FieldPolicies, diagnosticFieldPolicy{
				Name: "placement_policy_ref", Class: diagnosticFieldClassIdentifier, Transform: diagnosticTransformOmit,
			})
		}
	})

	t.Run("strict aliases are stable per key and isolated across keys", func(t *testing.T) {
		aliases := diagnosticTestTableAliases(t, handler)
		sameKeyHandler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), cfg)
		require.NoError(t, err)
		require.Equal(t, aliases, diagnosticTestTableAliases(t, sameKeyHandler))

		otherKeyFile := filepath.Join(t.TempDir(), "other-diagnostic-redaction-key")
		require.NoError(t, os.WriteFile(otherKeyFile, []byte("abcdef0123456789abcdef0123456789"), 0o600))
		otherCfg := cfg
		otherCfg.RedactionKeyFile = otherKeyFile
		otherCfg.RedactionKeyID = "test-key-2"
		otherKeyHandler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), otherCfg)
		require.NoError(t, err)
		otherAliases := diagnosticTestTableAliases(t, otherKeyHandler)
		for tableID, alias := range aliases {
			require.NotEqual(t, alias, otherAliases[tableID])
		}
	})

	t.Run("strict profile rejects weak key material", func(t *testing.T) {
		weakKeyFile := filepath.Join(t.TempDir(), "weak-diagnostic-redaction-key")
		require.NoError(t, os.WriteFile(weakKeyFile, []byte("too-short"), 0o600))
		weakCfg := cfg
		weakCfg.RedactionKeyFile = weakKeyFile
		_, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), weakCfg)
		require.ErrorContains(t, err, "between 32 and 4096 bytes")
	})

	t.Run("binding summary excludes SQL text", func(t *testing.T) {
		records, bodies := collectAllDiagnosticTestRecords(t, handler, "binding.summary", 1)
		require.NotEmpty(t, records)
		joined := strings.Join(bodies, "")
		require.NotContains(t, joined, "secret-binding-literal")
		require.NotContains(t, joined, "original_sql")
		require.NotContains(t, joined, "bind_sql")
		require.NotContains(t, joined, "default_db")
		var record diagnosticBindingRecord
		require.NoError(t, json.Unmarshal(records[0], &record))
		require.NotEmpty(t, record.SQLDigest)
		createTime, err := time.Parse(time.RFC3339Nano, record.CreateTime)
		require.NoError(t, err)
		require.Equal(t, time.UTC, createTime.Location())
		updateTime, err := time.Parse(time.RFC3339Nano, record.UpdateTime)
		require.NoError(t, err)
		require.Equal(t, time.UTC, updateTime.Location())
	})

	t.Run("stats health excludes histogram payloads", func(t *testing.T) {
		records, bodies := collectAllDiagnosticTestRecords(t, handler, "stats.health", 1)
		require.NotEmpty(t, records)
		joined := strings.Join(bodies, "")
		require.NotContains(t, joined, "top_n")
		require.NotContains(t, joined, "buckets")
		var record diagnosticStatsHealthRecord
		require.NoError(t, json.Unmarshal(records[0], &record))
		require.NotZero(t, record.TableID)
	})

	t.Run("tampered and expired cursors restart snapshot", func(t *testing.T) {
		first := requestDiagnosticTestPage(t, handler, "schema.columns", 1, "")
		require.NotEmpty(t, first.NextCursor)
		tamperedBytes := []byte(first.NextCursor)
		tamperedBytes[len(tamperedBytes)/2] ^= 1
		tampered := string(tamperedBytes)
		recorder := requestDiagnosticTest(t, handler, "schema.columns", 1, tampered)
		require.Equal(t, http.StatusConflict, recorder.Code)
		require.Contains(t, recorder.Body.String(), `"restart_snapshot":true`)

		issuedAt := handler.now()
		handler.now = func() time.Time {
			return issuedAt.Add(handler.cursorTTL + time.Second)
		}
		t.Cleanup(func() {
			handler.now = time.Now
		})
		recorder = requestDiagnosticTest(t, handler, "schema.columns", 1, first.NextCursor)
		require.Equal(t, http.StatusConflict, recorder.Code)
		require.Contains(t, recorder.Body.String(), "has expired")
		handler.now = time.Now
	})

	t.Run("source limits", func(t *testing.T) {
		for range cap(handler.semaphore) {
			handler.semaphore <- struct{}{}
		}
		recorder := requestDiagnosticTest(t, handler, "schema.tables", 1, "")
		for range cap(handler.semaphore) {
			<-handler.semaphore
		}
		require.Equal(t, http.StatusTooManyRequests, recorder.Code)
		require.Equal(t, "1", recorder.Header().Get("Retry-After"))

		smallCfg := cfg
		smallCfg.MaxResponseBytes = 128
		smallHandler, err := newDiagnosticAPIHandler(domain.GetDomain(tk.Session()), smallCfg)
		require.NoError(t, err)
		recorder = requestDiagnosticTest(t, smallHandler, "schema.tables", 1, "")
		require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)
		require.Contains(t, recorder.Body.String(), "suggested_page_size")
	})
}

func collectAllDiagnosticTestRecords(
	t *testing.T,
	handler *diagnosticAPIHandler,
	dataset string,
	pageSize int,
) ([]json.RawMessage, []string) {
	t.Helper()
	var records []json.RawMessage
	var bodies []string
	cursor := ""
	snapshotID := ""
	redactionProfile := ""
	redactionVersion := 0
	redactionKeyID := ""
	seen := make(map[string]struct{})
	for page := 0; page < 100; page++ {
		envelope, body := requestDiagnosticTestPageWithBody(t, handler, dataset, pageSize, cursor)
		require.Equal(t, len(envelope.Records), envelope.RecordCount)
		if snapshotID == "" {
			snapshotID = envelope.SnapshotID
			redactionProfile = envelope.RedactionProfile
			redactionVersion = envelope.RedactionVersion
			redactionKeyID = envelope.RedactionKeyID
		} else {
			require.Equal(t, snapshotID, envelope.SnapshotID)
			require.Equal(t, redactionProfile, envelope.RedactionProfile)
			require.Equal(t, redactionVersion, envelope.RedactionVersion)
			require.Equal(t, redactionKeyID, envelope.RedactionKeyID)
		}
		require.NotEmpty(t, envelope.RedactionProfile)
		require.Equal(t, diagnosticRedactionVersion, envelope.RedactionVersion)
		for _, record := range envelope.Records {
			key := string(record)
			_, duplicate := seen[key]
			require.False(t, duplicate, "duplicate record in %s: %s", dataset, key)
			seen[key] = struct{}{}
			records = append(records, record)
		}
		bodies = append(bodies, body)
		if envelope.Complete {
			require.Empty(t, envelope.NextCursor)
			return records, bodies
		}
		require.NotEmpty(t, envelope.NextCursor)
		cursor = envelope.NextCursor
	}
	t.Fatalf("dataset %s did not complete", dataset)
	return nil, nil
}

func diagnosticTestTableAliases(t *testing.T, handler *diagnosticAPIHandler) map[int64]string {
	t.Helper()
	records, _ := collectAllDiagnosticTestRecords(t, handler, "schema.tables", 100)
	aliases := make(map[int64]string, len(records))
	for _, raw := range records {
		var record diagnosticTableRecord
		require.NoError(t, json.Unmarshal(raw, &record))
		require.True(t, strings.HasPrefix(record.SchemaName, "schema_"), record.SchemaName)
		require.True(t, strings.HasPrefix(record.TableName, "table_"), record.TableName)
		aliases[record.TableID] = record.TableName
	}
	return aliases
}

func requestDiagnosticTestPage(
	t *testing.T,
	handler *diagnosticAPIHandler,
	dataset string,
	pageSize int,
	cursor string,
) diagnosticTestEnvelope {
	t.Helper()
	envelope, _ := requestDiagnosticTestPageWithBody(t, handler, dataset, pageSize, cursor)
	return envelope
}

func requestDiagnosticTestPageWithBody(
	t *testing.T,
	handler *diagnosticAPIHandler,
	dataset string,
	pageSize int,
	cursor string,
) (diagnosticTestEnvelope, string) {
	t.Helper()
	recorder := requestDiagnosticTest(t, handler, dataset, pageSize, cursor)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var envelope diagnosticTestEnvelope
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, dataset, envelope.Dataset)
	return envelope, recorder.Body.String()
}

func requestDiagnosticTest(
	t *testing.T,
	handler *diagnosticAPIHandler,
	dataset string,
	pageSize int,
	cursor string,
) *httptest.ResponseRecorder {
	t.Helper()
	query := "page_size=" + url.QueryEscape(strconv.Itoa(pageSize))
	if cursor != "" {
		query += "&cursor=" + url.QueryEscape(cursor)
	}
	req := httptest.NewRequest(http.MethodGet, "/internal/diagnostics/v1/datasets/"+dataset+"?"+query, nil)
	req = mux.SetURLVars(req, map[string]string{"dataset": dataset})
	recorder := httptest.NewRecorder()
	handler.serveDataset(recorder, req)
	return recorder
}

func BenchmarkDiagnosticIdentifierRedaction(b *testing.B) {
	keyFile := filepath.Join(b.TempDir(), "diagnostic-redaction-key")
	require.NoError(b, os.WriteFile(keyFile, []byte("0123456789abcdef0123456789abcdef"), 0o600))
	cfg := config.NewConfig().DiagnosticAPI
	cfg.RedactionKeyFile = keyFile
	cfg.RedactionKeyID = "benchmark-key"
	redactor, err := newDiagnosticRedactor(cfg)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	var alias string
	for i := 0; i < b.N; i++ {
		alias = redactor.identifier("column", "customer_email", 101, 202)
	}
	if alias == "" {
		b.Fatal("empty alias")
	}
}
