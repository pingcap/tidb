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

package ddl

import (
	"fmt"

	"github.com/pingcap/tidb/parser/coldef"
)

// AlterTableSpecification.Action types.
// TODO: Add more actions.
const (
	AlterTableOpt int = iota + 1
	AlterAddColumn
	AlterAddConstr
	AlterDropColumn
	AlterDropPrimaryKey
	AlterDropIndex
	AlterDropForeignKey
)

// ColumnPosition Types.
const (
	ColumnPositionNone int = iota
	ColumnPositionFirst
	ColumnPositionAfter
)

// ColumnPosition represents the position of the newly added column.
type ColumnPosition struct {
	// ColumnPositionNone | ColumnPositionFirst | ColumnPositionAfter
	Type int
	// RelativeColumn is the column which is before the newly added column if type is ColumnPositionAfter.
	RelativeColumn string
}

// String implements fmt.Stringer.
func (cp *ColumnPosition) String() string {
	switch cp.Type {
	case ColumnPositionFirst:
		return "FIRST"
	case ColumnPositionAfter:
		return fmt.Sprintf("AFTER %s", cp.RelativeColumn)
	default:
		return ""
	}
}

// AlterSpecification alter table specification.
type AlterSpecification struct {
	Action     int
	Name       string
	Constraint *coldef.TableConstraint
	TableOpts  []*coldef.TableOpt
	Column     *coldef.ColumnDef
	Position   *ColumnPosition
}

// String implements fmt.Stringer.
func (as *AlterSpecification) String() string {
	switch as.Action {
	case AlterTableOpt:
		// TODO: Finish this
		return ""
	case AlterAddConstr:
		if as.Constraint != nil {
			return fmt.Sprintf("ADD %s", as.Constraint.String())
		}
		return ""
	case AlterDropColumn:
		return fmt.Sprintf("DROP COLUMN %s", as.Name)
	case AlterDropPrimaryKey:
		return fmt.Sprintf("DROP PRIMARY KEY")
	case AlterDropIndex:
		return fmt.Sprintf("DROP INDEX %s", as.Name)
	case AlterDropForeignKey:
		return fmt.Sprintf("DROP FOREIGN KEY %s", as.Name)
	case AlterAddColumn:
		ps := as.Position.String()
		if len(ps) > 0 {
			return fmt.Sprintf("ADD Column %s %s", as.Column.String(), ps)
		}
		return fmt.Sprintf("ADD Column %s", as.Column.String())
	default:
		return ""
	}
}
