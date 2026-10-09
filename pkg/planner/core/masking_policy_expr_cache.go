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

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

type maskingPolicyExprCacheEntry struct {
	expr   expression.Expression
	column *expression.Column
}

type maskingPolicyExprCache struct {
	schemaVersion int64
	entries       map[int64]*maskingPolicyExprCacheEntry
	building      map[int64]struct{}
}

func getMaskingPolicyExprCache(sv *variable.SessionVars, schemaVersion int64) *maskingPolicyExprCache {
	cacheObj, cacheVer := sv.GetMaskingPolicyExprCache()
	if cacheObj != nil {
		if cache, ok := cacheObj.(*maskingPolicyExprCache); ok && cacheVer == schemaVersion {
			return cache
		}
	}
	cache := &maskingPolicyExprCache{
		schemaVersion: schemaVersion,
		entries:       make(map[int64]*maskingPolicyExprCacheEntry),
		building:      make(map[int64]struct{}),
	}
	sv.SetMaskingPolicyExprCache(cache, schemaVersion)
	return cache
}

func getMaskingPolicyExpr(
	ctx expression.BuildContext,
	sv *variable.SessionVars,
	schemaVersion int64,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (expression.Expression, *expression.Column, error) {
	cache := getMaskingPolicyExprCache(sv, schemaVersion)
	if entry, ok := cache.entries[policy.ID]; ok {
		return entry.expr.Clone(), entry.column, nil
	}
	if _, ok := cache.building[policy.ID]; ok {
		return nil, nil, errors.New("masking policy expression recursion detected")
	}
	cache.building[policy.ID] = struct{}{}
	expr, placeholder, err := buildMaskingPolicyExpr(ctx, policy, tblInfo, colInfo)
	delete(cache.building, policy.ID)
	if err != nil {
		return nil, nil, err
	}
	cache.entries[policy.ID] = &maskingPolicyExprCacheEntry{
		expr:   expr,
		column: placeholder,
	}
	return expr.Clone(), placeholder, nil
}

func buildMaskingPolicyExpr(
	ctx expression.BuildContext,
	policy *model.MaskingPolicyInfo,
	tblInfo *model.TableInfo,
	colInfo *model.ColumnInfo,
) (expression.Expression, *expression.Column, error) {
	if policy == nil || tblInfo == nil || colInfo == nil {
		return nil, nil, errors.New("masking policy expression requires policy/table/column info")
	}
	cols, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, policy.DBName, tblInfo.Name, []*model.ColumnInfo{colInfo}, tblInfo)
	if err != nil {
		return nil, nil, err
	}
	schema := expression.NewSchema(cols...)
	expr, err := expression.ParseSimpleExpr(ctx, policy.Expression, expression.WithInputSchemaAndNames(schema, names, tblInfo))
	if err != nil {
		return nil, nil, err
	}
	if len(cols) != 1 {
		return nil, nil, errors.New("masking policy expression expects one input column")
	}
	return expr, cols[0], nil
}
