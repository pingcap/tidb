// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

func parseRawArgs(sctx sessionctx.Context, rawArgs []string) ([]types.Datum, []*types.FieldType, error) {
	// TODO:
	return nil, nil, nil
}

func getPlanFromCache(sctx sessionctx.Context, rawSQL string, rawArgs []string) (plan PhysicalPlan, existed bool, err error) {
	if !sctx.GetSessionVars().EnableGeneralPlanCache {
		return nil, false, nil
	}

	// TODO: get db, schemaVer, latestSchemaVer
	db := ""
	schemaVer, latestSchemaVer := int64(0), int64(0)
	cacheKey, err := NewPlanCacheKey(sctx.GetSessionVars(), rawSQL, db, schemaVer, latestSchemaVer)
	if err != nil {
		return nil, false, err
	}

	rawCachedVals, existed := sctx.PreparedPlanCache().Get(cacheKey)
	if !existed {
		return nil, false, nil
	}

	// TODO: check privileges

	// TODO: parse rawArgs
	params, varTypes, err := parseRawArgs(sctx, rawArgs)
	if err != nil {
		return nil, false, err
	}
	sctx.GetSessionVars().PreparedParams = params

	// TODO: get bindSQL
	bindSQL := ""
	cachedVals := rawCachedVals.([]*PlanCacheValue)
	for _, cachedVal := range cachedVals {
		if cachedVal.BindSQL != bindSQL {
			continue
		}
		if !cachedVal.varTypesUnchanged(nil, varTypes) {
			continue
		}
		planValid := true
		for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
			if !unionScan && tableHasDirtyContent(sctx, tblInfo) {
				planValid = false
				// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
				// rebuilding the filters in UnionScan is pretty trivial.
				sctx.PreparedPlanCache().Delete(cacheKey)
				break
			}
		}
		if planValid {
			// TODO: rebuild the plan
			// TODO: metrics

			return cachedVal.Plan.(PhysicalPlan), true, nil
		}
	}
	return nil, false, nil
}
