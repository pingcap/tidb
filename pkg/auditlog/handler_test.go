// Copyright 2024 PingCAP, Inc.
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

package auditlog

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *AuditManager {
	t.Helper()
	InitAuditMetrics()
	writer := NewAuditWriter(WriterConfig{
		LogDir:     t.TempDir(),
		BufferSize: 100,
		BatchSize:  10,
	})
	return NewAuditManager(writer)
}

func TestAuditRuleHandlerListEmpty(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditRuleHandler(manager)

	req := httptest.NewRequest(http.MethodGet, "/audit/rules", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var rules []*AuditRule
	err := json.Unmarshal(w.Body.Bytes(), &rules)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestAuditRuleHandlerAddRule(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditRuleHandler(manager)

	rule := AuditRule{
		ID:        "test-add",
		QueryType: QueryTypeSelect,
	}
	body, _ := json.Marshal(rule)
	req := httptest.NewRequest(http.MethodPost, "/audit/rules", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	// Verify rule was added
	rules := manager.GetRules()
	assert.Len(t, rules, 1)
	assert.Equal(t, "test-add", rules[0].ID)
}

func TestAuditRuleHandlerAddDuplicate(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditRuleHandler(manager)

	rule := AuditRule{ID: "dup-rule", QueryType: QueryTypeAll}
	body, _ := json.Marshal(rule)

	// Add first
	req := httptest.NewRequest(http.MethodPost, "/audit/rules", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	// Add duplicate
	req = httptest.NewRequest(http.MethodPost, "/audit/rules", bytes.NewReader(body))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestAuditRuleDeleteHandler(t *testing.T) {
	manager := newTestManager(t)

	// Add a rule first
	manager.AddRule(&AuditRule{ID: "del-rule", QueryType: QueryTypeAll})

	handler := NewAuditRuleDeleteHandler(manager)
	router := mux.NewRouter()
	router.Handle("/audit/rules/{ruleID}", handler)

	req := httptest.NewRequest(http.MethodDelete, "/audit/rules/del-rule", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuditStatsHandler(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditStatsHandler(manager)

	req := httptest.NewRequest(http.MethodGet, "/audit/stats", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var stats map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &stats)
	require.NoError(t, err)
	assert.Contains(t, stats, "total_events_processed")
	assert.Contains(t, stats, "active_rules")
}

func TestAuditStatusHandler(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditStatusHandler(manager)

	req := httptest.NewRequest(http.MethodGet, "/audit/status", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var status map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &status)
	require.NoError(t, err)
	assert.Contains(t, status, "enabled")
	assert.Contains(t, status, "active_rules")
}

func TestAuditRuleHandlerMethodNotAllowed(t *testing.T) {
	manager := newTestManager(t)
	handler := NewAuditRuleHandler(manager)

	req := httptest.NewRequest(http.MethodPut, "/audit/rules", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
