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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	globalManager   *AuditManager
	globalManagerMu sync.Mutex
)

// SetGlobalAuditManager sets the global audit manager instance.
func SetGlobalAuditManager(m *AuditManager) {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	globalManager = m
}

// GlobalAuditManager returns the global audit manager instance.
func GlobalAuditManager() *AuditManager {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	return globalManager
}

// AuditManager is the central component that coordinates audit rule management
// and query event processing. It accepts query events from the server layer,
// matches them against configured rules, and forwards matching events to the
// async writer for batch persistence.
type AuditManager struct {
	rules   map[string]*AuditRule
	writer  *AuditWriter
	enabled atomic.Bool
	stats   *AuditStats
}

// AuditStats tracks aggregate statistics for the audit subsystem.
type AuditStats struct {
	TotalEventsProcessed atomic.Int64
	TotalEventsMatched   atomic.Int64
	TotalEventsDropped   atomic.Int64
	RuleMatchCounts      map[string]*atomic.Int64 // rule ID -> match count
}

// NewAuditManager creates a new AuditManager with the given writer.
func NewAuditManager(writer *AuditWriter) *AuditManager {
	return &AuditManager{
		rules:  make(map[string]*AuditRule),
		writer: writer,
		stats: &AuditStats{
			RuleMatchCounts: make(map[string]*atomic.Int64),
		},
	}
}

// Start initializes the audit manager and its writer.
func (m *AuditManager) Start() error {
	if err := m.writer.Start(); err != nil {
		return fmt.Errorf("failed to start audit writer: %w", err)
	}
	m.enabled.Store(true)
	log.Info("audit log manager started")
	return nil
}

// Stop gracefully shuts down the audit manager.
func (m *AuditManager) Stop() {
	m.enabled.Store(false)
	m.writer.Stop()
	log.Info("audit log manager stopped")
}

// AddRule adds a new audit rule. This method is called from HTTP handlers
// running in separate goroutines.
func (m *AuditManager) AddRule(rule *AuditRule) error {
	if rule.ID == "" {
		return fmt.Errorf("rule ID cannot be empty")
	}
	if _, exists := m.rules[rule.ID]; exists {
		return fmt.Errorf("rule with ID %q already exists", rule.ID)
	}
	rule.CreatedAt = time.Now()
	rule.Enabled = true
	m.rules[rule.ID] = rule
	m.stats.RuleMatchCounts[rule.ID] = &atomic.Int64{}
	AuditLogActiveRules.WithLabelValues().Set(float64(len(m.rules)))
	log.Info("audit rule added", zap.String("rule_id", rule.ID))
	return nil
}

// RemoveRule removes an audit rule by ID.
func (m *AuditManager) RemoveRule(ruleID string) error {
	rule, exists := m.rules[ruleID]
	if !exists {
		return fmt.Errorf("rule with ID %q not found", ruleID)
	}
	rule.Enabled = false
	AuditLogActiveRules.WithLabelValues().Set(float64(m.countActiveRules()))
	log.Info("audit rule removed", zap.String("rule_id", ruleID))
	return nil
}

// GetRules returns all audit rules.
func (m *AuditManager) GetRules() []*AuditRule {
	result := make([]*AuditRule, 0, len(m.rules))
	for _, rule := range m.rules {
		result = append(result, rule)
	}
	return result
}

// GetStats returns the current audit statistics.
func (m *AuditManager) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"total_events_processed": m.stats.TotalEventsProcessed.Load(),
		"total_events_matched":   m.stats.TotalEventsMatched.Load(),
		"total_events_dropped":   m.stats.TotalEventsDropped.Load(),
		"active_rules":           m.countActiveRules(),
		"total_rules":            len(m.rules),
	}
	ruleStats := make(map[string]int64)
	for id, count := range m.stats.RuleMatchCounts {
		ruleStats[id] = count.Load()
	}
	stats["rule_match_counts"] = ruleStats
	return stats
}

// LogQuery is the main entry point called from the server layer for each query.
// It evaluates the query against all rules and enqueues matching events.
func (m *AuditManager) LogQuery(user, db, sql string, connID uint64, duration time.Duration, success bool) {
	if !m.enabled.Load() {
		return
	}

	m.stats.TotalEventsProcessed.Add(1)
	qType := ClassifyQuery(sql)

	// Iterate over rules to find matches
	for _, rule := range m.rules {
		if rule.Matches(user, db, qType) {
			event := &AuditEvent{
				Timestamp:    time.Now(),
				User:         user,
				Database:     db,
				QueryType:    qType,
				Query:        truncateQuery(sql, 4096),
				ConnectionID: connID,
				Duration:     duration,
				RuleID:       rule.ID,
				Success:      success,
			}
			m.writer.Write(event)
			m.stats.TotalEventsMatched.Add(1)
			if counter, ok := m.stats.RuleMatchCounts[rule.ID]; ok {
				counter.Add(1)
			}
			AuditLogEventsTotal.WithLabelValues(string(qType), statusLabel(success)).Inc()
			return // first matching rule wins
		}
	}
}

// countActiveRules counts rules that are still enabled.
func (m *AuditManager) countActiveRules() int {
	count := 0
	for _, rule := range m.rules {
		if rule.Enabled {
			count++
		}
	}
	return count
}

// IsEnabled returns whether the audit manager is currently enabled.
func (m *AuditManager) IsEnabled() bool {
	return m.enabled.Load()
}

// truncateQuery truncates a SQL string to the given max length.
func truncateQuery(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "...(truncated)"
}

func statusLabel(success bool) string {
	if success {
		return "success"
	}
	return "failure"
}
