// Copyright 2020 PingCAP, Inc.
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

package restore

import (
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

type CheckType string

const (
	Critical CheckType = "critical"
	Warn     CheckType = "performance"
)

type Template interface {
	// Collect mainly collect performance related checks' results and critical level checks' results.
	// If the performance is not as expect or one of critical check not passed. it will stop import task.
	Collect(t CheckType, passed bool, msg string)

	// Success represents the whole check has passed or not.
	Success() bool

	// FailedCount represents (the warn check failed count, the critical check failed count)
	FailedCount(t CheckType) int

	// Output print all checks results.
	Output() string

	// FailedMsg represents the error msg for the failed check.
	FailedMsg() string
}

type SimpleTemplate struct {
	count               int
	warnFailedCount     int
	criticalFailedCount int
	normalMsgs          []string // only used in unit test now
	criticalMsgs        []string
	t                   table.Writer
}

func NewSimpleTemplate() Template {
	t := table.NewWriter()
	t.AppendHeader(table.Row{"#", "Check Item", "Type", "Passed"})
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "#", WidthMax: 6},
		{Name: "Check Item", WidthMax: 130},
		{Name: "Type", WidthMax: 20},
		{Name: "Passed", WidthMax: 6},
	})
	return &SimpleTemplate{
		t: t,
	}
}

func (c *SimpleTemplate) FailedMsg() string {
	return strings.Join(c.criticalMsgs, ";\n")
}

func (c *SimpleTemplate) Collect(t CheckType, passed bool, msg string) {
	c.count++
	if !passed {
		switch t {
		case Critical:
			c.criticalFailedCount++
		case Warn:
			c.warnFailedCount++
		}
	}
	if !passed && t == Critical {
		c.criticalMsgs = append(c.criticalMsgs, msg)
	} else {
		c.normalMsgs = append(c.normalMsgs, msg)
	}
	c.t.AppendRow(table.Row{c.count, msg, t, passed})
	c.t.AppendSeparator()
}

func (c *SimpleTemplate) Success() bool {
	return c.criticalFailedCount == 0
}

func (c *SimpleTemplate) FailedCount(t CheckType) int {
	if t == Warn {
		return c.warnFailedCount
	}
	if t == Critical {
		return c.criticalFailedCount
	}
	return 0
}

func (c *SimpleTemplate) Output() string {
	c.t.SetAllowedRowLength(170)
	c.t.SetRowPainter(func(row table.Row) text.Colors {
		if passed, ok := row[3].(bool); ok {
			if !passed {
				if typ, ok := row[2].(CheckType); ok {
					if typ == Warn {
						return text.Colors{text.FgYellow}
					}
					if typ == Critical {
						return text.Colors{text.FgRed}
					}
				}
			}
		}
		return nil
	})
	return c.t.Render() + "\n"
}
