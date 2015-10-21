// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package field

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/mysql"
)

// ResultField provides meta data of table column.
type ResultField struct {
	column.Col   // Col.Name is OrgName.
	Name         string
	TableName    string
	OrgTableName string
	DBName       string
}

// String implements fmt.Stringer interface.
func (rf *ResultField) String() string {
	return JoinQualifiedName(rf.DBName, rf.TableName, rf.Name)
}

// Clone clones a new ResultField from old ResultField.
func (rf *ResultField) Clone() *ResultField {
	r := *rf
	return &r
}

// RFQNames gets all ResultField names.
func RFQNames(l []*ResultField) []string {
	r := make([]string, len(l))
	for i, v := range l {
		r[i] = fmt.Sprintf("%q", v.Name)
	}
	return r
}

// ColsToResultFields converts Cols to ResultFields.
func ColsToResultFields(cols []*column.Col, tableName string) []*ResultField {
	// TODO: add DBName
	r := make([]*ResultField, len(cols))
	for i, v := range cols {
		r[i] = ColToResultField(v, tableName)
	}
	return r
}

// ColToResultField converts Col to ResultField.
func ColToResultField(col *column.Col, tableName string) *ResultField {
	// TODO: add DBName.
	rf := &ResultField{
		Col:          *col,
		Name:         col.Name.O,
		TableName:    tableName,
		OrgTableName: tableName,
	}
	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if rf.Tp == mysql.TypeVarchar {
		rf.Tp = mysql.TypeVarString
	}
	return rf
}

// ContainFieldName checks whether name is in ResultFields.
func ContainFieldName(name string, fields []*ResultField) bool {
	indices := GetResultFieldIndex(name, fields)
	return len(indices) > 0
}

// ContainAllFieldNames checks whether names are all in ResultFields.
// TODO: add alias table name support
func ContainAllFieldNames(names []string, fields []*ResultField) bool {
	for _, name := range names {
		if !ContainFieldName(name, fields) {
			return false
		}
	}

	return true
}

// SplitQualifiedName splits an identifier name to db, table and field name.
func SplitQualifiedName(name string) (db string, table string, field string) {
	seps := strings.Split(name, ".")

	l := len(seps)
	switch l {
	case 1:
		// `name` is field.
		field = seps[0]
	case 2:
		// `name` is `table.field`.
		table, field = seps[0], seps[1]
	case 3:
		// `name` is `db.table.field`.
		db, table, field = seps[0], seps[1], seps[2]
	default:
		// `name` is `db.table.field`.
		db, table, field = seps[l-3], seps[l-2], seps[l-1]
	}

	return
}

// JoinQualifiedName converts db, table, field to a qualified name.
func JoinQualifiedName(db string, table string, field string) string {
	if len(db) > 0 {
		return fmt.Sprintf("%s.%s.%s", db, table, field)
	} else if len(table) > 0 {
		return fmt.Sprintf("%s.%s", table, field)
	} else {
		return field
	}
}

// GetResultFieldIndex gets name index in ResultFields.
func GetResultFieldIndex(name string, fields []*ResultField) []int {
	var indices []int

	db, table, field := SplitQualifiedName(name)
	for i, f := range fields {
		if checkFieldsEqual(db, table, field, f.DBName, f.TableName, f.Name) {
			indices = append(indices, i)
			continue
		}
	}

	return indices
}

// CheckFieldsEqual checks if xname and yname is equal.
// xname/yname in the pattern dbname.tablename.fieldname
// If any part of any argument is missing, it will ignore it and
// continue check other parts. So a.b.c equals b.c
func CheckFieldsEqual(xname, yname string) bool {
	xdb, xtable, xfield := SplitQualifiedName(xname)
	ydb, ytable, yfield := SplitQualifiedName(yname)
	return checkFieldsEqual(xdb, xtable, xfield, ydb, ytable, yfield)
}

// See: https://dev.mysql.com/doc/refman/5.0/en/identifier-case-sensitivity.html
// TODO: check `lower_case_table_names` system variable.
func checkFieldsEqual(xdb, xtable, xfield, ydb, ytable, yfield string) bool {
	if !strings.EqualFold(xfield, yfield) {
		return false
	}

	if len(xtable) > 0 && len(ytable) > 0 {
		if !strings.EqualFold(xtable, ytable) {
			return false
		}
	}

	if len(xdb) > 0 && len(ydb) > 0 {
		if !strings.EqualFold(xdb, ydb) {
			return false
		}
	}

	return true
}

// CloneFieldByName clones a ResultField in ResultFields according to name.
func CloneFieldByName(name string, fields []*ResultField) (*ResultField, error) {
	indices := GetResultFieldIndex(name, fields)
	if len(indices) == 0 {
		return nil, errors.Errorf("unknown field %s", name)
	}

	return fields[indices[0]].Clone(), nil
}

// CheckWildcardField checks wildcard field, like `*` or `table.*` or `db.table.*`.
func CheckWildcardField(name string) (string, bool, error) {
	_, table, field := SplitQualifiedName(name)
	return table, field == "*", nil
}

// IsQualifiedName returns whether name contains "." or not.
func IsQualifiedName(name string) bool {
	return strings.Contains(name, ".")
}
