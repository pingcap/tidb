// Copyright 2021 PingCAP, Inc.
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

//go:build !codes

package testkit

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment string
	require *require.Assertions
	assert  *assert.Assertions
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	resBuff := bytes.NewBufferString("")
	for _, row := range res.rows {
		_, _ = fmt.Fprintf(resBuff, "%s\n", row)
	}

	needBuff := bytes.NewBufferString("")
	for _, row := range expected {
		_, _ = fmt.Fprintf(needBuff, "%s\n", row)
	}

	res.require.Equal(needBuff.String(), resBuff.String(), res.comment)
}

// AddComment adds the extra comment for the Result's output.
func (res *Result) AddComment(c string) {
	res.comment += "\n" + c
}

// CheckWithFunc asserts the result match the expected results in the way `f` specifies.
func (res *Result) CheckWithFunc(expected [][]interface{}, f func([]string, []interface{}) bool) {
	res.require.Equal(len(res.rows), len(expected), res.comment+"\nResult length mismatch")

	for i, resRow := range res.rows {
		expectedRow := expected[i]
		res.require.Truef(f(resRow, expectedRow), res.comment+"\nCheck with function failed\nactual: %s\nexpected: %s", resRow, expectedRow)
	}
}

// Rows is similar to RowsWithSep, use white space as separator string.
func Rows(args ...string) [][]interface{} {
	return RowsWithSep(" ", args...)
}

// Sort sorts and return the result.
func (res *Result) Sort() *Result {
	slices.SortFunc(res.rows, func(a, b []string) bool {
		for i := range a {
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
		return false
	})
	return res
}

// RowsWithSep is a convenient function to wrap args to a slice of []interface.
// The arg represents a row, split by sep.
func RowsWithSep(sep string, args ...string) [][]interface{} {
	rows := make([][]interface{}, len(args))
	for i, v := range args {
		parts := strings.Split(v, sep)
		row := make([]interface{}, len(parts))
		for j, s := range parts {
			row[j] = s
		}
		rows[i] = row
	}
	return rows
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	ifacesSlice := make([][]interface{}, len(res.rows))
	for i := range res.rows {
		ifaces := make([]interface{}, len(res.rows[i]))
		for j := range res.rows[i] {
			ifaces[j] = res.rows[i][j]
		}
		ifacesSlice[i] = ifaces
	}
	return ifacesSlice
}

// CheckAt asserts the result of selected columns equals the expected results.
func (res *Result) CheckAt(cols []int, expected [][]interface{}) {
	for _, e := range expected {
		res.require.Equal(len(e), len(cols))
	}

	rows := make([][]string, 0, len(expected))
	for i := range res.rows {
		row := make([]string, 0, len(cols))
		for _, r := range cols {
			row = append(row, res.rows[i][r])
		}
		rows = append(rows, row)
	}
	got := fmt.Sprintf("%s", rows)
	need := fmt.Sprintf("%s", expected)
	res.require.Equal(need, got, res.comment)
}
