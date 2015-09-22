// Copyright 2015 PingCAP, Inc.
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

package expression

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
)

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	// TableName is the table name for the column to assign.
	TableName string
	// ColName is the variable name we want to set.
	ColName string
	// Expr is the expression assigning to ColName.
	Expr Expression
}

// String returns Assignment representation.
func (a *Assignment) String() string {
	if len(a.TableName) == 0 {
		return fmt.Sprintf("%s=%s", a.ColName, a.Expr)
	}
	return fmt.Sprintf("%s.%s=%s", a.TableName, a.ColName, a.Expr)
}

// NewAssignment builds a new Assignment expression.
func NewAssignment(key string, value Expression) (*Assignment, error) {
	strs := strings.Split(key, ".")
	var (
		tblName string
		colName string
	)
	if len(strs) == 1 {
		colName = strs[0]
	} else if len(strs) == 2 {
		tblName = strs[0]
		colName = strs[1]
	} else {
		// TODO: we should support db.tbl.col later.
		return nil, errors.Errorf("Invalid format of Assignment key: %s", key)
	}
	return &Assignment{
		TableName: tblName,
		ColName:   colName,
		Expr:      value,
	}, nil
}
