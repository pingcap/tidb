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
	"strings"
	"time"
)

// QueryType represents the type of SQL query.
type QueryType string

const (
	QueryTypeSelect QueryType = "SELECT"
	QueryTypeDML    QueryType = "DML"
	QueryTypeDDL    QueryType = "DDL"
	QueryTypeAdmin  QueryType = "ADMIN"
	QueryTypeAll    QueryType = "ALL"
)

// AuditRule defines a rule for filtering which queries should be audited.
type AuditRule struct {
	ID         string    `json:"id"`
	UserFilter string    `json:"user_filter"` // empty means all users
	DBFilter   string    `json:"db_filter"`   // empty means all databases
	QueryType  QueryType `json:"query_type"`  // empty or ALL means all types
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
}

// AuditEvent represents a single audited query event.
type AuditEvent struct {
	Timestamp    time.Time `json:"timestamp"`
	User         string    `json:"user"`
	Database     string    `json:"database"`
	QueryType    QueryType `json:"query_type"`
	Query        string    `json:"query"`
	ConnectionID uint64    `json:"connection_id"`
	Duration     time.Duration `json:"duration_ns"`
	RuleID       string    `json:"rule_id"`
	Success      bool      `json:"success"`
}

// Matches checks whether the given query event matches this audit rule.
func (r *AuditRule) Matches(user, db string, qType QueryType) bool {
	if r.UserFilter != "" && !matchesWildcard(r.UserFilter, user) {
		return false
	}
	if r.DBFilter != "" && !matchesWildcard(r.DBFilter, db) {
		return false
	}
	if r.QueryType != "" && r.QueryType != QueryTypeAll && r.QueryType != qType {
		return false
	}
	return true
}

// matchesWildcard performs simple wildcard matching where '*' matches any substring.
func matchesWildcard(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	pattern = strings.ToLower(pattern)
	value = strings.ToLower(value)
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		return strings.Contains(value, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(value, pattern[1:])
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	return pattern == value
}

// ClassifyQuery determines the QueryType from a SQL statement string.
func ClassifyQuery(sql string) QueryType {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	if strings.HasPrefix(trimmed, "SELECT") {
		return QueryTypeSelect
	}
	if strings.HasPrefix(trimmed, "INSERT") || strings.HasPrefix(trimmed, "UPDATE") ||
		strings.HasPrefix(trimmed, "DELETE") || strings.HasPrefix(trimmed, "REPLACE") {
		return QueryTypeDML
	}
	if strings.HasPrefix(trimmed, "CREATE") || strings.HasPrefix(trimmed, "ALTER") ||
		strings.HasPrefix(trimmed, "DROP") || strings.HasPrefix(trimmed, "TRUNCATE") {
		return QueryTypeDDL
	}
	return QueryTypeAdmin
}
