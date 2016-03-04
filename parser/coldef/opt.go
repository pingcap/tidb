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

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
)

// FloatOpt is used for parsing floating-point type option from SQL.
// TODO: add reference doc.
type FloatOpt struct {
	Flen    int
	Decimal int
}

// CharsetOpt is used for parsing charset option from SQL.
type CharsetOpt struct {
	Chs string
	Col string
}

// String implements fmt.Stringer interface.
func (o *CharsetOpt) String() string {
	var ss []string
	if o.Chs != "" {
		ss = append(ss, "CHARACTER SET = "+o.Chs)
	}
	if o.Col != "" {
		ss = append(ss, "COLLATE = "+o.Col)
	}
	return strings.Join(ss, " ")
}

// ConstraintOpt is used for parsing column constraint info from SQL.
type ConstraintOpt struct {
	Tp     int
	Bvalue bool
	Evalue ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *ConstraintOpt) String() string {
	switch c.Tp {
	case ConstrNotNull:
		return "NOT NULL"
	case ConstrNull:
		return "NULL"
	case ConstrAutoIncrement:
		return "AUTO_INCREMENT"
	case ConstrPrimaryKey:
		return "PRIMARY KEY"
	case ConstrUniq:
		return "UNIQUE"
	case ConstrUniqKey:
		return "UNIQUE KEY"
	case ConstrDefaultValue:
		return "DEFAULT " + ast.ToString(c.Evalue)
	case ConstrOnUpdate:
		return "ON UPDATE " + ast.ToString(c.Evalue)
	default:
		return ""
	}
}

// DB Options.
const (
	DBOptNone = iota
	DBOptCharset
	DBOptCollate
)

// DatabaseOpt is used for parsing database option from SQL.
type DatabaseOpt struct {
	Tp    int
	Value string
}

// Constraints.
const (
	ConstrNoConstr = iota
	ConstrPrimaryKey
	ConstrForeignKey
	ConstrNotNull
	ConstrAutoIncrement
	ConstrDefaultValue
	ConstrUniq
	ConstrIndex
	ConstrUniqIndex
	ConstrKey
	ConstrUniqKey
	ConstrNull
	ConstrOnUpdate
	ConstrFulltext
	ConstrComment
)

// LockType is select lock type.
type LockType int

// Select Lock Type.
const (
	SelectLockNone LockType = iota
	SelectLockForUpdate
	SelectLockInShareMode
)

// Table Options.
const (
	TblOptNone = iota
	TblOptEngine
	TblOptCharset
	TblOptCollate
	TblOptAutoIncrement
	TblOptComment
	TblOptAvgRowLength
	TblOptCheckSum
	TblOptCompression
	TblOptConnection
	TblOptPassword
	TblOptKeyBlockSize
	TblOptMaxRows
	TblOptMinRows
	TblOptDelayKeyWrite
)

// TableOpt is used for parsing table option from SQL.
type TableOpt struct {
	Tp        int
	StrValue  string
	UintValue uint64
}

// TableOption is the collection of table options.
// TODO: rename TableOpt or TableOption.
type TableOption struct {
	Engine        string
	Charset       string
	Collate       string
	AutoIncrement uint64 // TODO: apply this value to autoid.Allocator.
}

// String implements fmt.Stringer interface.
func (o *TableOption) String() string {
	strs := []string{}
	if o.Engine != "" {
		x := fmt.Sprintf("ENGINE=%s", o.Engine)
		strs = append(strs, x)
	}
	if o.Charset != "" {
		x := fmt.Sprintf("CHARSET=%s", o.Charset)
		strs = append(strs, x)
	}
	if o.Collate != "" {
		x := fmt.Sprintf("COLLATE=%s", o.Collate)
		strs = append(strs, x)
	}

	return strings.Join(strs, " ")
}

// TableConstraint is constraint for table definition.
type TableConstraint struct {
	Tp         int
	ConstrName string

	// Used for PRIMARY KEY, UNIQUE, ......
	Keys []*IndexColName

	// Used for foreign key.
	Refer *ReferenceDef
}

// Clone clones a new TableConstraint from old TableConstraint.
func (tc *TableConstraint) Clone() *TableConstraint {
	keys := make([]*IndexColName, 0, len(tc.Keys))
	for _, k := range tc.Keys {
		keys = append(keys, k)
	}
	ntc := &TableConstraint{
		Tp:         tc.Tp,
		ConstrName: tc.ConstrName,
		Keys:       keys,
	}
	if tc.Refer != nil {
		ntc.Refer = tc.Refer.Clone()
	}
	return ntc
}

// String implements fmt.Stringer interface.
func (tc *TableConstraint) String() string {
	tokens := []string{}
	if tc.Tp == ConstrPrimaryKey {
		tokens = append(tokens, "PRIMARY KEY")
	} else {
		if tc.Tp == ConstrKey {
			tokens = append(tokens, "KEY")
		} else if tc.Tp == ConstrIndex {
			tokens = append(tokens, "INDEX")
		} else if tc.Tp == ConstrUniq {
			tokens = append(tokens, "UNIQUE")
		} else if tc.Tp == ConstrUniqKey {
			tokens = append(tokens, "UNIQUE KEY")
		} else if tc.Tp == ConstrUniqIndex {
			tokens = append(tokens, "UNIQUE INDEX")
		} else if tc.Tp == ConstrForeignKey {
			tokens = append(tokens, "FOREIGN KEY")
		}
		tokens = append(tokens, tc.ConstrName)
	}
	keysStr := make([]string, 0, len(tc.Keys))
	for _, v := range tc.Keys {
		keysStr = append(keysStr, v.String())
	}
	tokens = append(tokens, fmt.Sprintf("(%s)", strings.Join(keysStr, ", ")))
	if tc.Refer != nil {
		tokens = append(tokens, tc.Refer.String())
	}
	return strings.Join(tokens, " ")
}

// AuthOption is used for parsing create user statement.
// TODO: support auth_plugin
type AuthOption struct {
	// AuthString/HashString can be empty, so we need to decide which one to use.
	ByAuthString bool
	AuthString   string
	HashString   string
}

// UserSpecification is used for parsing create user statement.
type UserSpecification struct {
	User    string
	AuthOpt *AuthOption
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	Priv mysql.PrivilegeType
	Cols []string
}

const (
	// ObjectTypeNone is for empty object type.
	ObjectTypeNone = iota
	// ObjectTypeTable means the following object is a table.
	ObjectTypeTable
)

const (
	// GrantLevelNone is the dummy const for default value.
	GrantLevelNone = iota
	// GrantLevelGlobal means the privileges are administrative or apply to all databases on a given server.
	GrantLevelGlobal
	// GrantLevelDB means the privileges apply to all objects in a given database.
	GrantLevelDB
	// GrantLevelTable means the privileges apply to all columns in a given table.
	GrantLevelTable
)

// GrantLevel is used for store the privilege scope.
type GrantLevel struct {
	Level     int
	DBName    string
	TableName string
}
