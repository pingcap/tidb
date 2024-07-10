// Copyright 2023 PingCAP, Inc.
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

package bindinfo

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/bindinfo/internal/logutil"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	tablefilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

type captureFilter struct {
	frequency int64
	tables    []tablefilter.Filter // `schema.table`
	users     map[string]struct{}

	fail      bool
	currentDB string
}

func (cf *captureFilter) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if x, ok := in.(*ast.TableName); ok {
		tblEntry := stmtctx.TableEntry{
			DB:    x.Schema.L,
			Table: x.Name.L,
		}
		if x.Schema.L == "" {
			tblEntry.DB = cf.currentDB
		}
		for _, tableFilter := range cf.tables {
			if tableFilter.MatchTable(tblEntry.DB, tblEntry.Table) {
				cf.fail = true // some filter is matched
			}
		}
	}
	return in, cf.fail
}

func (*captureFilter) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (cf *captureFilter) isEmpty() bool {
	return len(cf.tables) == 0 && len(cf.users) == 0
}

// ParseCaptureTableFilter checks whether this filter is valid and parses it.
func ParseCaptureTableFilter(tableFilter string) (f tablefilter.Filter, valid bool) {
	// forbid wildcards '!' and '@' for safety,
	// please see https://github.com/pingcap/tidb-tools/tree/master/pkg/table-filter for more details.
	tableFilter = strings.TrimLeft(tableFilter, " \t")
	if tableFilter == "" {
		return nil, false
	}
	if tableFilter[0] == '!' || tableFilter[0] == '@' {
		return nil, false
	}
	var err error
	f, err = tablefilter.Parse([]string{tableFilter})
	if err != nil {
		return nil, false
	}
	return f, true
}

func (h *globalBindingHandle) extractCaptureFilterFromStorage() (filter *captureFilter) {
	filter = &captureFilter{
		frequency: 1,
		users:     make(map[string]struct{}),
	}

	_ = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		// No need to acquire the session context lock for ExecRestrictedSQL, it
		// uses another background session.
		rows, _, err := execRows(sctx, `SELECT filter_type, filter_value FROM mysql.capture_plan_baselines_blacklist order by filter_type`)
		if err != nil {
			logutil.BindLogger().Warn("failed to load mysql.capture_plan_baselines_blacklist", zap.Error(err))
			return err
		}
		for _, row := range rows {
			filterTp := strings.ToLower(row.GetString(0))
			valStr := strings.ToLower(row.GetString(1))
			switch filterTp {
			case "table":
				tfilter, valid := ParseCaptureTableFilter(valStr)
				if !valid {
					logutil.BindLogger().Warn("capture table filter is invalid, ignore it", zap.String("filter_value", valStr))
					continue
				}
				filter.tables = append(filter.tables, tfilter)
			case "user":
				filter.users[valStr] = struct{}{}
			case "frequency":
				f, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					logutil.BindLogger().Warn("failed to parse frequency type value, ignore it", zap.String("filter_value", valStr), zap.Error(err))
					continue
				}
				if f < 1 {
					logutil.BindLogger().Warn("frequency threshold is less than 1, ignore it", zap.Int64("frequency", f))
					continue
				}
				if f > filter.frequency {
					filter.frequency = f
				}
			default:
				logutil.BindLogger().Warn("unknown capture filter type, ignore it", zap.String("filter_type", filterTp))
			}
		}
		return nil
	})
	return
}

// CaptureBaselines is used to automatically capture plan baselines.
func (h *globalBindingHandle) CaptureBaselines() {
	parser4Capture := parser.New()
	captureFilter := h.extractCaptureFilterFromStorage()
	emptyCaptureFilter := captureFilter.isEmpty()
	bindableStmts := stmtsummaryv2.GetMoreThanCntBindableStmt(captureFilter.frequency)
	for _, bindableStmt := range bindableStmts {
		stmt, err := parser4Capture.ParseOneStmt(bindableStmt.Query, bindableStmt.Charset, bindableStmt.Collation)
		if err != nil {
			logutil.BindLogger().Debug("parse SQL failed in baseline capture", zap.String("SQL", bindableStmt.Query), zap.Error(err))
			continue
		}
		if insertStmt, ok := stmt.(*ast.InsertStmt); ok && insertStmt.Select == nil {
			continue
		}
		if !emptyCaptureFilter {
			captureFilter.fail = false
			captureFilter.currentDB = bindableStmt.Schema
			stmt.Accept(captureFilter)
			if captureFilter.fail {
				continue
			}

			if len(captureFilter.users) > 0 {
				filteredByUser := true
				for user := range bindableStmt.Users {
					if _, ok := captureFilter.users[user]; !ok {
						filteredByUser = false // some user not in the black-list has processed this stmt
						break
					}
				}
				if filteredByUser {
					continue
				}
			}
		}
		dbName := utilparser.GetDefaultDB(stmt, bindableStmt.Schema)
		normalizedSQL, digest := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(stmt, dbName, bindableStmt.Query))
		if r := h.getCache().GetBinding(digest.String()); HasAvailableBinding(r) {
			continue
		}
		bindSQL := GenerateBindingSQL(stmt, bindableStmt.PlanHint, true, dbName)
		if bindSQL == "" {
			continue
		}

		var charset, collation string
		_ = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
			charset, collation = sctx.GetSessionVars().GetCharsetInfo()
			return nil
		})
		binding := Binding{
			OriginalSQL: normalizedSQL,
			Db:          dbName,
			BindSQL:     bindSQL,
			Status:      Enabled,
			Charset:     charset,
			Collation:   collation,
			Source:      Capture,
			SQLDigest:   digest.String(),
		}
		// We don't need to pass the `sctx` because the BindSQL has been validated already.
		err = h.CreateGlobalBinding(nil, binding)
		if err != nil {
			logutil.BindLogger().Debug("create bind record failed in baseline capture", zap.String("SQL", bindableStmt.Query), zap.Error(err))
		}
	}
}
