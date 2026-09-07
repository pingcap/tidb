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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// AuditRuleHandler handles HTTP requests for audit rule CRUD operations.
type AuditRuleHandler struct {
	manager *AuditManager
}

// NewAuditRuleHandler creates a new handler for audit rule management.
func NewAuditRuleHandler(manager *AuditManager) *AuditRuleHandler {
	return &AuditRuleHandler{manager: manager}
}

// ServeHTTP handles GET (list rules), POST (add rule), and DELETE (remove rule).
func (h *AuditRuleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleListRules(w, r)
	case http.MethodPost:
		h.handleAddRule(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleListRules returns all configured audit rules as JSON.
func (h *AuditRuleHandler) handleListRules(w http.ResponseWriter, _ *http.Request) {
	rules := h.manager.GetRules()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rules); err != nil {
		log.Error("failed to encode audit rules", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// handleAddRule adds a new audit rule from the request body.
func (h *AuditRuleHandler) handleAddRule(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var rule AuditRule
	if err := json.Unmarshal(body, &rule); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.manager.AddRule(&rule); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "created",
		"rule_id": rule.ID,
	})
}

// AuditRuleDeleteHandler handles DELETE requests for removing audit rules.
type AuditRuleDeleteHandler struct {
	manager *AuditManager
}

// NewAuditRuleDeleteHandler creates a handler for deleting audit rules.
func NewAuditRuleDeleteHandler(manager *AuditManager) *AuditRuleDeleteHandler {
	return &AuditRuleDeleteHandler{manager: manager}
}

// ServeHTTP handles DELETE requests to remove a specific audit rule.
func (h *AuditRuleDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	ruleID := vars["ruleID"]
	if ruleID == "" {
		http.Error(w, "rule ID is required", http.StatusBadRequest)
		return
	}

	if err := h.manager.RemoveRule(ruleID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "deleted",
		"rule_id": ruleID,
	})
}

// AuditStatsHandler handles HTTP requests for audit statistics.
type AuditStatsHandler struct {
	manager *AuditManager
}

// NewAuditStatsHandler creates a handler for audit statistics.
func NewAuditStatsHandler(manager *AuditManager) *AuditStatsHandler {
	return &AuditStatsHandler{manager: manager}
}

// ServeHTTP returns the current audit statistics as JSON.
func (h *AuditStatsHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	stats := h.manager.GetStats()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Error("failed to encode audit stats", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// AuditStatusHandler handles HTTP requests for audit system status.
type AuditStatusHandler struct {
	manager *AuditManager
}

// NewAuditStatusHandler creates a handler for audit system status.
func NewAuditStatusHandler(manager *AuditManager) *AuditStatusHandler {
	return &AuditStatusHandler{manager: manager}
}

// ServeHTTP returns the audit system status.
func (h *AuditStatusHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	status := map[string]interface{}{
		"enabled":      h.manager.IsEnabled(),
		"active_rules": h.manager.countActiveRules(),
		"total_rules":  len(h.manager.rules),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
