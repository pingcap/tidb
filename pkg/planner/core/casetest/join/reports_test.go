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

package join

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type reportSummary struct {
	Error string `json:"error"`
}

func TestReportsCantFindColumn(t *testing.T) {
	// issue: 65454
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		reportsDir := filepath.Join("testdata", "reports")
		summaries, err := filepath.Glob(filepath.Join(reportsDir, "case_*", "summary.json"))
		require.NoError(t, err)
		require.NotEmpty(t, summaries, "no deterministic report cases found in %s", reportsDir)

		for _, summaryPath := range summaries {
			summary, err := readReportSummary(summaryPath)
			require.NoError(t, err)
			if !strings.Contains(summary.Error, "Can't find column") {
				continue
			}
			caseDir := filepath.Dir(summaryPath)
			caseName := filepath.Base(caseDir)
			t.Run(caseName, func(t *testing.T) {
				schemaSQL := mustReadFile(t, filepath.Join(caseDir, "schema.sql"))
				insertSQL := mustReadFile(t, filepath.Join(caseDir, "inserts.sql"))
				caseSQL := mustReadFile(t, filepath.Join(caseDir, "case.sql"))

				dbNames := collectDBNames(schemaSQL, insertSQL, caseSQL)
				if len(dbNames) == 0 {
					dbNames = []string{"report_" + strings.ReplaceAll(caseName, "-", "_")}
				}
				for _, dbName := range dbNames {
					tk.MustExec("drop database if exists " + dbName)
					tk.MustExec("create database " + dbName)
				}
				tk.MustExec("use " + dbNames[0])

				if err := runSQLText(t, tk, schemaSQL, false); err != nil {
					t.Skipf("skip case due to setup error: %v", err)
				}
				if err := runSQLText(t, tk, insertSQL, false); err != nil {
					t.Skipf("skip case due to setup error: %v", err)
				}
				_ = runSQLText(t, tk, caseSQL, true)

				for _, dbName := range dbNames {
					tk.MustExec("drop database if exists " + dbName)
				}
			})
		}
	})
}

func readReportSummary(path string) (*reportSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var summary reportSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func mustReadFile(t *testing.T, path string) string {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

func runSQLText(t *testing.T, tk *testkit.TestKit, sqlText string, allowNonCantFind bool) error {
	for _, stmt := range splitSQL(sqlText) {
		if err := execSQL(t, tk, stmt, allowNonCantFind); err != nil {
			return err
		}
	}
	return nil
}

func execSQL(t *testing.T, tk *testkit.TestKit, stmt string, allowNonCantFind bool) error {
	sqlText := strings.TrimSpace(stmt)
	if sqlText == "" {
		return nil
	}
	rs, err := tk.Exec(sqlText)
	if rs != nil {
		closeErr := rs.Close()
		if err == nil && closeErr != nil {
			return closeErr
		}
	}
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "Can't find column") {
		require.NoError(t, err, "sql:%s", sqlText)
		return err
	}
	if !allowNonCantFind {
		return err
	}
	return nil
}

func collectDBNames(sqlTexts ...string) []string {
	dbs := make(map[string]struct{})
	nameRe := regexp.MustCompile(`(?i)\bshiro_fuzz[0-9a-zA-Z_]*\b`)
	for _, sqlText := range sqlTexts {
		for _, name := range nameRe.FindAllString(sqlText, -1) {
			dbs[strings.ToLower(name)] = struct{}{}
		}
		for _, stmt := range splitSQL(sqlText) {
			fields := strings.Fields(strings.TrimSpace(stmt))
			if len(fields) == 0 {
				continue
			}
			switch strings.ToLower(fields[0]) {
			case "use":
				if len(fields) >= 2 {
					name := trimDBName(fields[1])
					if name != "" {
						dbs[strings.ToLower(name)] = struct{}{}
					}
				}
			case "create":
				if len(fields) >= 3 && strings.ToLower(fields[1]) == "database" {
					nameIdx := 2
					if len(fields) >= 6 && strings.ToLower(fields[2]) == "if" && strings.ToLower(fields[3]) == "not" && strings.ToLower(fields[4]) == "exists" {
						nameIdx = 5
					}
					if len(fields) > nameIdx {
						name := trimDBName(fields[nameIdx])
						if name != "" {
							dbs[strings.ToLower(name)] = struct{}{}
						}
					}
				}
			}
		}
	}
	if len(dbs) == 0 {
		return nil
	}
	names := make([]string, 0, len(dbs))
	for name := range dbs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func trimDBName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimSuffix(name, ";")
	name = strings.Trim(name, "`")
	return name
}

func splitSQL(sql string) []string {
	var res []string
	var b strings.Builder
	inSingle, inDouble, inBack := false, false, false
	escaped := false
	for _, r := range sql {
		if escaped {
			escaped = false
			b.WriteRune(r)
			continue
		}
		if r == '\\' && (inSingle || inDouble) {
			escaped = true
			b.WriteRune(r)
			continue
		}
		switch r {
		case '\'':
			if !inDouble && !inBack {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBack {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBack = !inBack
			}
		case ';':
			if !inSingle && !inDouble && !inBack {
				res = append(res, b.String())
				b.Reset()
				continue
			}
		}
		b.WriteRune(r)
	}
	if strings.TrimSpace(b.String()) != "" {
		res = append(res, b.String())
	}
	return res
}
