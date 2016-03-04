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

package coldef

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See: http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	TableIdent    table.Ident
	IndexColNames []*IndexColName
}

// String implements fmt.Stringer interface.
func (rd *ReferenceDef) String() string {
	cns := make([]string, 0, len(rd.IndexColNames))
	for _, icn := range rd.IndexColNames {
		cns = append(cns, icn.String())
	}
	return fmt.Sprintf("REFERENCES %s (%s)", rd.TableIdent, strings.Join(cns, ", "))
}

// Clone clones a new ReferenceDef from old ReferenceDef.
func (rd *ReferenceDef) Clone() *ReferenceDef {
	cnames := make([]*IndexColName, 0, len(rd.IndexColNames))
	for _, idxColName := range rd.IndexColNames {
		t := *idxColName
		cnames = append(cnames, &t)
	}
	return &ReferenceDef{TableIdent: rd.TableIdent, IndexColNames: cnames}
}

// IndexColName is used for parsing index column name from SQL.
type IndexColName struct {
	ColumnName string
	Length     int
}

// String implements fmt.Stringer interface.
func (icn *IndexColName) String() string {
	if icn.Length >= 0 {
		return fmt.Sprintf("%s(%d)", icn.ColumnName, icn.Length)
	}
	return icn.ColumnName
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	Name        string
	Tp          *types.FieldType
	Constraints []*ConstraintOpt
}

// String implements fmt.Stringer interface.
func (c *ColumnDef) String() string {
	ans := []string{c.Name}

	for _, x := range c.Constraints {
		ans = append(ans, x.String())
	}
	return strings.Join(ans, " ")
}
