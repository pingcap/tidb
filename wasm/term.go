// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
// +build js wasm

package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/parser/ast"
)

// Terminal translate execution result to human readable string
type Terminal interface {
	// WriteRows format the table head and rows of the table to string
	WriteRows([]*ast.ResultField, [][]string, time.Duration) string

	// WriteEmpty format the empty result to string
	WriteEmpty(time.Duration) string

	// WriteSuccess returns a success message
	WriteSuccess(time.Duration) string

	// Error format error to string
	Error(error) string
}

// NewTerm returns a Terminal
func NewTerm() Terminal {
	return &Term{bufio.NewReader(os.Stdin), []string{}}
}

// A implemention of Terminal
type Term struct {
	reader   *bufio.Reader
	commands []string
}

// Column is the representation of a column of a table.
// It consists of the column name, the values the column contains,
// And Len is the max length of all values and the name, it's used
// to format the output.
type Column struct {
	Name   string
	Values []string
	Len    int
}

// WriteRows format the table head and rows of the table to string
func (t *Term) WriteRows(fields []*ast.ResultField, rows [][]string, d time.Duration) string {
	columns := make([]*Column, len(fields))
	for i := range columns {
		columns[i] = &Column{
			Name: fields[i].Column.Name.O,
			Len:  len(fields[i].Column.Name.O),
		}
	}

	for i := range rows {
		for j, c := range columns {
			value := rows[i][j]
			c.Values = append(c.Values, value)
			if len(value) > c.Len {
				c.Len = len(value)
			}
		}
	}

	ret := t.divider(columns)
	ret += t.print(columns, -1)
	ret += t.divider(columns)
	for idx := range rows {
		ret += t.print(columns, idx)
	}
	if len(rows) != 0 {
		ret += t.divider(columns)
		ret += fmt.Sprintf("%d row in set (%.2f sec)\n", len(rows), d.Seconds())
	} else {
		ret += fmt.Sprintf("Empty set (%.2f sec)\n", d.Seconds())
	}

	return ret
}

// WriteEmpty format the empty result to string
func (t *Term) WriteSuccess(d time.Duration) string {
	return fmt.Sprintf("Execute success (%.2f sec)\n", d.Seconds())
}

// WriteEmpty format the empty result to string
func (t *Term) WriteEmpty(d time.Duration) string {
	return fmt.Sprintf("Empty set (%.2f sec)\n", d.Seconds())
}

// divider add a line ('--------') to split two rows
func (*Term) divider(cs []*Column) string {
	ret := fmt.Sprint("+")
	for _, c := range cs {
		for i := 0; i < c.Len+2; i++ {
			ret += fmt.Sprint("-")
		}
		ret += fmt.Sprint("+")
	}
	ret += fmt.Sprintln("")
	return ret
}

// print the value in the idx of cs.
func (*Term) print(cs []*Column, idx int) string {
	ret := fmt.Sprint("| ")
	for _, c := range cs {
		format := fmt.Sprintf("%%-%dv", c.Len)
		if idx < 0 {
			ret += fmt.Sprintf(format, c.Name)
		} else {
			ret += fmt.Sprintf(format, c.Values[idx])
		}
		ret += fmt.Sprint(" | ")
	}
	ret += fmt.Sprintln("")
	return ret
}

// Error format error to string
func (*Term) Error(err error) string {
	return fmt.Sprintln(err)
}
