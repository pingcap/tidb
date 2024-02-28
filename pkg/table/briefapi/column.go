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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2016 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package briefapi

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Column provides meta data describing a table column.
type Column struct {
	*model.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr *ClonableExprNode
	// If this column has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.GetType(), c.GetCharset())}
	if mysql.HasAutoIncrementFlag(c.GetFlag()) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.GetFlag()) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// ToInfo casts Column to model.ColumnInfo
// NOTE: DONT modify return value.
func (c *Column) ToInfo() *model.ColumnInfo {
	return c.ColumnInfo
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.GetFlag()) && tbInfo.PKIsHandle
}

// IsCommonHandleColumn checks if the column is common handle column.
func (c *Column) IsCommonHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.GetFlag()) && tbInfo.IsCommonHandle
}

// ClonableExprNode is a wrapper for ast.ExprNode.
type ClonableExprNode struct {
	ctor     func() ast.ExprNode
	internal ast.ExprNode
}

// NewClonableExprNode creates a ClonableExprNode.
func NewClonableExprNode(ctor func() ast.ExprNode, internal ast.ExprNode) *ClonableExprNode {
	return &ClonableExprNode{
		ctor:     ctor,
		internal: internal,
	}
}

// Clone makes a "copy" of internal ast.ExprNode by reconstructing it.
func (n *ClonableExprNode) Clone() ast.ExprNode {
	intest.AssertNotNil(n.ctor)
	if n.ctor == nil {
		return n.internal
	}
	return n.ctor()
}

// Internal returns the reference of the internal ast.ExprNode.
// Note: only use this method when you are sure that the internal ast.ExprNode is not modified concurrently.
func (n *ClonableExprNode) Internal() ast.ExprNode {
	return n.internal
}

type columnAPI interface {
	// Cols returns the columns of the table which is used in select, including hidden columns.
	Cols() []*Column

	// VisibleCols returns the columns of the table which is used in select, excluding hidden columns.
	VisibleCols() []*Column

	// HiddenCols returns the hidden columns of the table.
	HiddenCols() []*Column

	// WritableCols returns columns of the table in writable states.
	// Writable states includes Public, WriteOnly, WriteOnlyReorganization.
	WritableCols() []*Column

	// DeletableCols returns columns of the table in deletable states.
	// Deletable states includes Public, WriteOnly, WriteOnlyReorganization, DeleteOnly, DeleteReorganization.
	DeletableCols() []*Column

	// FullHiddenColsAndVisibleCols returns hidden columns in all states and unhidden columns in public states.
	FullHiddenColsAndVisibleCols() []*Column
}
