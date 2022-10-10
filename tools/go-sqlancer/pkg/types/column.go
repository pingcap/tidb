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

package types

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"

	"github.com/pingcap/tidb/parser/ast"
)

var (
	typePattern = regexp.MustCompile(`([a-z]*)\(?([0-9]*)\)?`)
)

// Column defines database column
type Column struct {
	Table      CIStr
	AliasTable CIStr
	Name       CIStr
	AliasName  CIStr
	Type       string
	Length     int
	Null       bool
}

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.Table, c.Name)
}

// Columns is a list of column
type Columns []Column

func (c Columns) Len() int {
	return len(c)
}

func (c Columns) Less(i, j int) bool {
	return c[i].String() < c[j].String()
}

func (c Columns) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Clone makes a replica of column
func (c *Column) Clone() Column {
	return Column{
		Table:      c.Table,
		AliasTable: c.AliasTable,
		Name:       c.Name,
		AliasName:  c.AliasName,
		Type:       c.Type,
		Length:     c.Length,
		Null:       c.Null,
	}
}

// ParseType parse types and data length
func (c *Column) ParseType(t string) {
	matches := typePattern.FindStringSubmatch(t)
	if len(matches) != 3 {
		return
	}
	c.Type = matches[1]
	if matches[2] != "" {
		l, err := strconv.Atoi(matches[2])
		if err == nil {
			c.Length = l
		}
	} else {
		c.Length = 0
	}
}

// ToModel converts to ast model
func (c Column) ToModel() *ast.ColumnNameExpr {
	table := c.Table
	if c.AliasTable != "" {
		table = c.AliasTable
	}
	name := c.Name
	if c.AliasName != "" {
		name = c.AliasName
	}
	return &ast.ColumnNameExpr{
		Name: &ast.ColumnName{
			Table: table.ToModel(),
			Name:  name.ToModel(),
		},
	}
}

// GetAliasTableName get tmp table name if exist, otherwise origin name
func (c Column) GetAliasTableName() CIStr {
	if c.AliasTable != "" {
		return c.AliasTable
	}
	return c.Table
}

// GetAliasName get alias column name
func (c Column) GetAliasName() CIStr {
	if c.AliasName != "" {
		return c.AliasName
	}
	return c.Name
}

// RandColumn is to randomly get column
func (c Columns) RandColumn() Column {
	if len(c) == 0 {
		panic("no columns in table")
	}
	return c[rand.Intn(len(c))]
}

// AddOption add option for column
// func (c *Column) AddOption(opt ast.ColumnOptionType) {
// 	for _, option := range c.Options {
// 		if option == opt {
// 			return
// 		}
// 	}
// 	c.Options = append(c.Options, opt)
// }

// // HasOption return is has the given option
// func (c *Column) HasOption(opt ast.ColumnOptionType) bool {
// 	for _, option := range c.Options {
// 		if option == opt {
// 			return true
// 		}
// 	}
// 	return false
// }
