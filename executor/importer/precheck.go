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

package importer

import (
	"context"
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/precheck"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/sqlexec"
)

// CheckRequirements checks the requirements for load data.
func (e *LoadDataController) CheckRequirements(ctx context.Context, conn sqlexec.SQLExecutor) error {
	collector := newPreCheckCollector()
	if e.ImportMode == PhysicalImportMode {
		// todo: maybe we can reuse checker in lightning
		sql := fmt.Sprintf("SELECT 1 FROM %s USE INDEX() LIMIT 1", common.UniqueTable(e.DBName, e.Table.Meta().Name.L))
		rs, err := conn.ExecuteInternal(ctx, sql)
		if err != nil {
			return err
		}
		defer terror.Call(rs.Close)
		rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			collector.fail(precheck.CheckTargetTableEmpty, "target table is not empty")
		} else {
			collector.pass(precheck.CheckTargetTableEmpty)
		}
	}
	if !collector.success() {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("\n" + collector.output())
	}
	return nil
}

const (
	failed = "failed"
	passed = "passed"
)

type preCheckCollector struct {
	t         table.Writer
	failCount int
}

func newPreCheckCollector() *preCheckCollector {
	t := table.NewWriter()
	t.AppendHeader(table.Row{"Check Item", "Result", "Detailed Message"})
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "Check Item", WidthMax: 20},
		{Name: "Result", WidthMax: 6},
		{Name: "Detailed Message", WidthMax: 130},
	})
	style := table.StyleDefault
	style.Format.Header = text.FormatDefault
	t.SetStyle(style)
	return &preCheckCollector{
		t: t,
	}
}

func (c *preCheckCollector) fail(item precheck.CheckItemID, msg string) {
	c.failCount++
	c.t.AppendRow(table.Row{item.DisplayName(), failed, msg})
	c.t.AppendSeparator()
}

func (c *preCheckCollector) success() bool {
	return c.failCount == 0
}

func (c *preCheckCollector) output() string {
	c.t.SetAllowedRowLength(170)
	c.t.SetRowPainter(func(row table.Row) text.Colors {
		if result, ok := row[1].(string); ok {
			if result == failed {
				return text.Colors{text.FgRed}
			}
		}
		return nil
	})
	return c.t.Render() + "\n"
}

func (c *preCheckCollector) pass(item precheck.CheckItemID) {
	c.t.AppendRow(table.Row{item.DisplayName(), passed, ""})
	c.t.AppendSeparator()
}
