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

package core

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var benchmarkPlanCacheKey string

func newPlanCacheKeyTestFixture(tb testing.TB, stmtText string, relatedTables, dirtyTables int) (*mock.Context, *PlanCacheStmt) {
	tb.Helper()
	p := parser.New()
	stmts, _, err := p.ParseSQL(stmtText)
	require.NoError(tb, err)
	require.Len(tb, stmts, 1)

	sctx := mock.NewContext()
	vars := sctx.GetSessionVars()
	vars.CurrentDB = "sbtest"
	vars.User = &auth.UserIdentity{
		AuthUsername: "root",
		AuthHostname: "localhost",
	}
	vars.TimeZone = time.Local
	vars.UsePlanBaselines = false
	vars.PlanCacheInvalidationOnFreshStats = false
	vars.IsolationReadEngines = map[kv.StoreType]struct{}{
		kv.TiDB: {},
		kv.TiKV: {},
	}

	relateVersion := make(map[int64]uint64, relatedTables)
	for i := range relatedTables {
		relateVersion[int64(i+1)] = uint64(100 + i)
	}
	for i := range dirtyTables {
		if vars.StmtCtx.TblInfo2UnionScan == nil {
			vars.StmtCtx.TblInfo2UnionScan = make(map[*model.TableInfo]bool, dirtyTables)
		}
		vars.StmtCtx.TblInfo2UnionScan[&model.TableInfo{ID: int64(1000 + i)}] = true
	}

	return sctx, &PlanCacheStmt{
		PreparedAst:   &ast.Prepared{Stmt: stmts[0]},
		SchemaVersion: 123456,
		RelateVersion: relateVersion,
		StmtText:      stmtText,
	}
}

func TestNewPlanCacheKeyStable(t *testing.T) {
	testCases := []struct {
		name          string
		stmtText      string
		relatedTables int
		dirtyTables   int
	}{
		{
			name:          "point-select",
			stmtText:      "SELECT c FROM sbtest1 WHERE id=?",
			relatedTables: 1,
		},
		{
			name:          "rich-metadata",
			stmtText:      fmt.Sprintf("SELECT c FROM sbtest1 WHERE id=? AND c IN (%s)", strings.Repeat("?,", 63)+"?"),
			relatedTables: 16,
			dirtyTables:   16,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sctx, stmt := newPlanCacheKeyTestFixture(t, testCase.stmtText, testCase.relatedTables, testCase.dirtyTables)
			first, _, cacheable, reason, err := NewPlanCacheKey(sctx, stmt)
			require.NoError(t, err)
			require.True(t, cacheable, reason)
			second, _, cacheable, reason, err := NewPlanCacheKey(sctx, stmt)
			require.NoError(t, err)
			require.True(t, cacheable, reason)
			require.Equal(t, first, second)
		})
	}
}

func BenchmarkNewPlanCacheKey(b *testing.B) {
	testCases := []struct {
		name          string
		stmtText      string
		relatedTables int
		dirtyTables   int
	}{
		{
			name:          "point-select/one-related-table",
			stmtText:      "SELECT c FROM sbtest1 WHERE id=?",
			relatedTables: 1,
		},
		{
			name:     "point-select/no-related-table",
			stmtText: "SELECT c FROM sbtest1 WHERE id=?",
		},
		{
			name:          "rich-metadata",
			stmtText:      fmt.Sprintf("SELECT c FROM sbtest1 WHERE id=? AND c IN (%s)", strings.Repeat("?,", 63)+"?"),
			relatedTables: 16,
			dirtyTables:   16,
		},
	}

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			sctx, stmt := newPlanCacheKeyTestFixture(b, testCase.stmtText, testCase.relatedTables, testCase.dirtyTables)
			var key string
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var cacheable bool
				var reason string
				var err error
				key, _, cacheable, reason, err = NewPlanCacheKey(sctx, stmt)
				if err != nil {
					b.Fatal(err)
				}
				if !cacheable {
					b.Fatal(reason)
				}
			}
			b.StopTimer()
			benchmarkPlanCacheKey = key
		})
	}
}
