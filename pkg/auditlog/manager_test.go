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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyQuery(t *testing.T) {
	tests := []struct {
		sql      string
		expected QueryType
	}{
		{"SELECT * FROM t", QueryTypeSelect},
		{"select count(*) from users", QueryTypeSelect},
		{"INSERT INTO t VALUES (1, 2)", QueryTypeDML},
		{"UPDATE t SET a = 1", QueryTypeDML},
		{"DELETE FROM t WHERE id = 1", QueryTypeDML},
		{"REPLACE INTO t VALUES (1)", QueryTypeDML},
		{"CREATE TABLE t (id INT)", QueryTypeDDL},
		{"ALTER TABLE t ADD COLUMN name VARCHAR(255)", QueryTypeDDL},
		{"DROP TABLE t", QueryTypeDDL},
		{"TRUNCATE TABLE t", QueryTypeDDL},
		{"SHOW TABLES", QueryTypeAdmin},
		{"EXPLAIN SELECT * FROM t", QueryTypeAdmin},
	}
	for _, tt := range tests {
		got := ClassifyQuery(tt.sql)
		assert.Equal(t, tt.expected, got, "ClassifyQuery(%q)", tt.sql)
	}
}

func TestAuditRuleMatches(t *testing.T) {
	rule := &AuditRule{
		ID:        "test-rule",
		Enabled:   true,
		QueryType: QueryTypeSelect,
	}

	// Matches SELECT queries
	assert.True(t, rule.Matches("root", "test_db", QueryTypeSelect))
	// Does not match DML queries
	assert.False(t, rule.Matches("root", "test_db", QueryTypeDML))

	// Rule with user filter
	ruleWithUser := &AuditRule{
		ID:         "user-rule",
		UserFilter: "admin*",
		Enabled:    true,
		QueryType:  QueryTypeAll,
	}
	assert.True(t, ruleWithUser.Matches("admin_user", "db", QueryTypeSelect))
	assert.False(t, ruleWithUser.Matches("regular_user", "db", QueryTypeSelect))

	// Rule with db filter
	ruleWithDB := &AuditRule{
		ID:        "db-rule",
		DBFilter:  "prod_*",
		Enabled:   true,
		QueryType: QueryTypeAll,
	}
	assert.True(t, ruleWithDB.Matches("root", "prod_main", QueryTypeDML))
	assert.False(t, ruleWithDB.Matches("root", "test_db", QueryTypeDML))
}

func TestMatchesWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		expect  bool
	}{
		{"*", "anything", true},
		{"admin*", "admin_user", true},
		{"admin*", "regular_user", false},
		{"*_db", "test_db", true},
		{"*_db", "test_table", false},
		{"*test*", "my_test_db", true},
		{"*test*", "production", false},
		{"exact", "exact", true},
		{"exact", "notexact", false},
	}
	for _, tt := range tests {
		got := matchesWildcard(tt.pattern, tt.value)
		assert.Equal(t, tt.expect, got, "matchesWildcard(%q, %q)", tt.pattern, tt.value)
	}
}

func TestTruncateQuery(t *testing.T) {
	short := "SELECT 1"
	assert.Equal(t, short, truncateQuery(short, 100))

	long := "SELECT " + string(make([]byte, 200))
	result := truncateQuery(long, 50)
	require.Len(t, result, 50+len("...(truncated)"))
	assert.Contains(t, result, "...(truncated)")
}

func TestAuditManagerAddRemoveRules(t *testing.T) {
	InitAuditMetrics()
	writer := NewAuditWriter(WriterConfig{
		LogDir:     t.TempDir(),
		BufferSize: 100,
		BatchSize:  10,
	})
	manager := NewAuditManager(writer)

	// Add a rule
	rule := &AuditRule{
		ID:        "rule-1",
		QueryType: QueryTypeSelect,
	}
	err := manager.AddRule(rule)
	require.NoError(t, err)

	// Duplicate rule should fail
	err = manager.AddRule(&AuditRule{ID: "rule-1"})
	assert.Error(t, err)

	// Empty ID should fail
	err = manager.AddRule(&AuditRule{ID: ""})
	assert.Error(t, err)

	// Get rules
	rules := manager.GetRules()
	assert.Len(t, rules, 1)

	// Remove rule
	err = manager.RemoveRule("rule-1")
	require.NoError(t, err)

	// Remove non-existent rule should fail
	err = manager.RemoveRule("non-existent")
	assert.Error(t, err)
}

func TestAuditManagerStats(t *testing.T) {
	InitAuditMetrics()
	writer := NewAuditWriter(WriterConfig{
		LogDir:     t.TempDir(),
		BufferSize: 100,
		BatchSize:  10,
	})
	manager := NewAuditManager(writer)

	stats := manager.GetStats()
	assert.Equal(t, int64(0), stats["total_events_processed"])
	assert.Equal(t, int64(0), stats["total_events_matched"])
	assert.Equal(t, 0, stats["active_rules"])
}

func TestGlobalAuditManager(t *testing.T) {
	// Initially nil
	assert.Nil(t, GlobalAuditManager())

	InitAuditMetrics()
	writer := NewAuditWriter(WriterConfig{
		LogDir:     t.TempDir(),
		BufferSize: 100,
		BatchSize:  10,
	})
	manager := NewAuditManager(writer)

	SetGlobalAuditManager(manager)
	assert.NotNil(t, GlobalAuditManager())
	assert.Equal(t, manager, GlobalAuditManager())

	// Cleanup
	SetGlobalAuditManager(nil)
	assert.Nil(t, GlobalAuditManager())
}
