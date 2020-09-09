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

package domain

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
)

// domainKeyType is a dummy type to avoid naming collision in context.
type domainKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k domainKeyType) String() string {
	return "domain"
}

const domainKey domainKeyType = 0

// BindDomain binds domain to context.
func BindDomain(ctx sessionctx.Context, domain *Domain) {
	ctx.SetValue(domainKey, domain)
}

// GetDomain gets domain from context.
func GetDomain(ctx sessionctx.Context) *Domain {
	v, ok := ctx.Value(domainKey).(*Domain)
	if !ok {
		return nil
	}
	return v
}

// CanRuntimePruneTbl indicates whether tbl support runtime prune.
func CanRuntimePruneTbl(ctx sessionctx.Context, tbl *model.TableInfo) bool {
	_, ok := ctx.GetSessionVars().Users["try_old_partition_implementation"]
	if ok {
		return false
	}
	if tbl.Partition == nil {
		return false
	}
	return GetDomain(ctx).StatsHandle().CanRuntimePrune(tbl.ID, tbl.Partition.Definitions[0].ID)
}

// CanRuntimePrune indicates whether tbl support runtime prune for table and first partition id.
func CanRuntimePrune(ctx sessionctx.Context, tid, p0Id int64) bool {
	_, ok := ctx.GetSessionVars().Users["try_old_partition_implementation"]
	if ok {
		return false
	}
	return GetDomain(ctx).StatsHandle().CanRuntimePrune(tid, p0Id)
}
