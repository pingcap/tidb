// Copyright 2026 PingCAP, Inc.
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

package expression

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

type tidbShardInternalVersionBuildContext struct {
	BuildContext
}

func (ctx *tidbShardInternalVersionBuildContext) allowTiDBShardInternalVersion() bool {
	return true
}

type tidbShardInternalVersionBuildContextProvider interface {
	allowTiDBShardInternalVersion() bool
}

func allowTiDBShardInternalVersion(ctx BuildContext) bool {
	provider, ok := ctx.(tidbShardInternalVersionBuildContextProvider)
	return ok && provider.allowTiDBShardInternalVersion()
}

// WithTiDBShardInternalVersion wraps a build context so it can build the internal-only
// versioned tidb_shard() form, e.g. tidb_shard(col, 1).
func WithTiDBShardInternalVersion(ctx BuildContext) BuildContext {
	if ctx == nil || allowTiDBShardInternalVersion(ctx) {
		return ctx
	}
	return &tidbShardInternalVersionBuildContext{BuildContext: ctx}
}

// WithTiDBShardInternalVersionForGeneratedColumn wraps the build context when the
// generated column stores an internal-only versioned tidb_shard() expression.
func WithTiDBShardInternalVersionForGeneratedColumn(ctx BuildContext, expr ast.ExprNode, col *model.ColumnInfo) BuildContext {
	if col == nil || !col.Hidden {
		return ctx
	}
	if _, ok := model.IsVersionedTiDBShardExprNode(expr); !ok {
		return ctx
	}
	return WithTiDBShardInternalVersion(ctx)
}

// WithTiDBShardInternalVersionForShardIndexVersion wraps the build context when
// building expressions for a shard-index version that uses the internal-only
// tidb_shard() representation.
func WithTiDBShardInternalVersionForShardIndexVersion(ctx BuildContext, version uint8) BuildContext {
	if version != model.ShardIndexVersionStringV1 {
		return ctx
	}
	return WithTiDBShardInternalVersion(ctx)
}

// BuildSimpleExprForGeneratedColumn builds a generated-column expression and
// enables the internal tidb_shard() form only when the column metadata stores
// that versioned internal representation.
func BuildSimpleExprForGeneratedColumn(ctx BuildContext, expr ast.ExprNode, schema *Schema, names types.NameSlice, tblInfo *model.TableInfo, col *model.ColumnInfo) (Expression, error) {
	buildCtx := WithTiDBShardInternalVersionForGeneratedColumn(ctx, expr, col)
	return BuildSimpleExpr(buildCtx, expr,
		WithInputSchemaAndNames(schema, names, tblInfo),
		WithAllowCastArray(true),
	)
}

// BuildSimpleExprForShardIndexHiddenColumn builds the hidden-column expression
// for a shard index, enabling the internal tidb_shard() form only for shard
// index versions that require it.
func BuildSimpleExprForShardIndexHiddenColumn(ctx BuildContext, expr ast.ExprNode, db string, tblInfo *model.TableInfo, version uint8) (Expression, error) {
	buildCtx := WithTiDBShardInternalVersionForShardIndexVersion(ctx, version)
	return BuildSimpleExpr(buildCtx, expr,
		WithTableInfo(db, tblInfo),
		WithAllowCastArray(true),
	)
}
