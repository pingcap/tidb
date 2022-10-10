// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/stretchr/testify/assert"
)

// test schema
var (
	schema = [][6]string{
		{"community", "comments", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "comments", "BASE TABLE", "owner", "varchar(255)", "NO"},
		{"community", "comments", "BASE TABLE", "repo", "varchar(255)", "NO"},
		{"community", "comments", "BASE TABLE", "comment_id", "int(11)", "NO"},
		{"community", "comments", "BASE TABLE", "comment_type", "varchar(128)", "NO"},
		{"community", "comments", "BASE TABLE", "pull_number", "int(11)", "NO"},
		{"community", "comments", "BASE TABLE", "body", "text", "NO"},
		{"community", "comments", "BASE TABLE", "user", "varchar(255)", "NO"},
		{"community", "comments", "BASE TABLE", "url", "varchar(1023)", "NO"},
		{"community", "comments", "BASE TABLE", "association", "varchar(255)", "NO"},
		{"community", "comments", "BASE TABLE", "relation", "varchar(255)", "NO"},
		{"community", "comments", "BASE TABLE", "created_at", "timestamp", "NO"},
		{"community", "comments", "BASE TABLE", "updated_at", "timestamp", "NO"},
		{"community", "picks", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "picks", "BASE TABLE", "season", "int(11)", "NO"},
		{"community", "picks", "BASE TABLE", "task_id", "int(11)", "NO"},
		{"community", "picks", "BASE TABLE", "teamID", "int(11)", "NO"},
		{"community", "picks", "BASE TABLE", "user", "varchar(255)", "NO"},
		{"community", "picks", "BASE TABLE", "pull_number", "int(11)", "NO"},
		{"community", "picks", "BASE TABLE", "status", "varchar(128)", "NO"},
		{"community", "picks", "BASE TABLE", "created_at", "timestamp", "NO"},
		{"community", "picks", "BASE TABLE", "updated_at", "timestamp", "NO"},
		{"community", "picks", "BASE TABLE", "closed_at", "datetime", "NO"},
		{"community", "pulls", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "pulls", "BASE TABLE", "owner", "varchar(255)", "NO"},
		{"community", "pulls", "BASE TABLE", "repo", "varchar(255)", "NO"},
		{"community", "pulls", "BASE TABLE", "pull_number", "int(11)", "NO"},
		{"community", "pulls", "BASE TABLE", "title", "text", "NO"},
		{"community", "pulls", "BASE TABLE", "body", "text", "NO"},
		{"community", "pulls", "BASE TABLE", "user", "varchar(255)", "NO"},
		{"community", "pulls", "BASE TABLE", "association", "varchar(255)", "NO"},
		{"community", "pulls", "BASE TABLE", "relation", "varchar(255)", "NO"},
		{"community", "pulls", "BASE TABLE", "label", "text", "NO"},
		{"community", "pulls", "BASE TABLE", "status", "varchar(128)", "NO"},
		{"community", "pulls", "BASE TABLE", "created_at", "timestamp", "NO"},
		{"community", "pulls", "BASE TABLE", "updated_at", "timestamp", "NO"},
		{"community", "pulls", "BASE TABLE", "closed_at", "datetime", "NO"},
		{"community", "pulls", "BASE TABLE", "merged_at", "datetime", "NO"},
		{"community", "tasks", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "season", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "complete_user", "varchar(255)", "NO"},
		{"community", "tasks", "BASE TABLE", "complete_team", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "owner", "varchar(255)", "NO"},
		{"community", "tasks", "BASE TABLE", "repo", "varchar(255)", "NO"},
		{"community", "tasks", "BASE TABLE", "title", "varchar(2047)", "NO"},
		{"community", "tasks", "BASE TABLE", "issue_number", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "pull_number", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "level", "varchar(255)", "NO"},
		{"community", "tasks", "BASE TABLE", "min_score", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "score", "int(11)", "NO"},
		{"community", "tasks", "BASE TABLE", "status", "varchar(255)", "NO"},
		{"community", "tasks", "BASE TABLE", "created_at", "timestamp", "NO"},
		{"community", "tasks", "BASE TABLE", "expired", "varchar(255)", "NO"},
		{"community", "teams", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "teams", "BASE TABLE", "season", "int(11)", "NO"},
		{"community", "teams", "BASE TABLE", "name", "varchar(255)", "NO"},
		{"community", "teams", "BASE TABLE", "issue_url", "varchar(1023)", "NO"},
		{"community", "users", "BASE TABLE", "id", "int(11)", "NO"},
		{"community", "users", "BASE TABLE", "season", "int(11)", "NO"},
		{"community", "users", "BASE TABLE", "user", "varchar(255)", "NO"},
		{"community", "users", "BASE TABLE", "team_id", "int(11)", "NO"},
	}
	dbname  = "community"
	indexes = make(map[string][]types.CIStr)
	tables  = []string{"comments", "picks", "pulls", "tasks", "teams", "users"}
)

// TestSQLSmith_LoadIndexes tests with load table indexes
func TestSQLSmith_LoadIndexes(t *testing.T) {
	e := Executor{
		conn:   nil,
		db:     dbname,
		tables: make(map[string]*types.Table),
	}
	indexes["users"] = []types.CIStr{"idx1", "idx2"}
	e.loadSchema(schema, indexes)

	assert.Equal(t, len(e.tables), 6)
	assert.Equal(t, len(e.tables["users"].Indexes), 2)
}
