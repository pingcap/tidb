// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type policyGetter struct {
	is *infoSchema
}

func (p *policyGetter) GetPolicy(policyID int64) (*model.PolicyInfo, error) {
	if policy, ok := p.is.PolicyByID(policyID); ok {
		return policy, nil
	}
	return nil, errors.Errorf("Cannot find placement policy with ID: %d", policyID)
}

type bundleInfoBuilder struct {
	deltaUpdate bool
	// tables or partitions that need to update placement bundle
	updateTables map[int64]any
	// all tables or partitions referring these policies should update placement bundle
	updatePolicies map[int64]any
	// partitions that need to update placement bundle
	updatePartitions map[int64]any
}

func (b *bundleInfoBuilder) initBundleInfoBuilder() {
	b.updateTables = make(map[int64]any)
	b.updatePartitions = make(map[int64]any)
	b.updatePolicies = make(map[int64]any)
}

func (b *bundleInfoBuilder) SetDeltaUpdateBundles() {
	b.deltaUpdate = true
}

func (b *bundleInfoBuilder) deleteBundle(is *infoSchema, tblID int64) {
	delete(is.ruleBundleMap, tblID)
}

func (b *bundleInfoBuilder) markTableBundleShouldUpdate(tblID int64) {
	b.updateTables[tblID] = struct{}{}
}

func (b *bundleInfoBuilder) markPartitionBundleShouldUpdate(partID int64) {
	b.updatePartitions[partID] = struct{}{}
}

func (b *bundleInfoBuilder) markBundlesReferPolicyShouldUpdate(policyID int64) {
	b.updatePolicies[policyID] = struct{}{}
}

func (b *bundleInfoBuilder) updateInfoSchemaBundles(is *infoSchema) {
	if b.deltaUpdate {
		b.completeUpdateTables(is)
		for tblID := range b.updateTables {
			b.updateTableBundles(is, tblID)
		}
		return
	}

	// do full update bundles
	is.ruleBundleMap = make(map[int64]*placement.Bundle)
	for _, tbls := range is.schemaMap {
		for _, tbl := range tbls.tables {
			b.updateTableBundles(is, tbl.Meta().ID)
		}
	}
}

func (b *bundleInfoBuilder) completeUpdateTables(is *infoSchema) {
	if len(b.updatePolicies) == 0 && len(b.updatePartitions) == 0 {
		return
	}

	for _, tbls := range is.schemaMap {
		for _, tbl := range tbls.tables {
			tblInfo := tbl.Meta()
			if tblInfo.PlacementPolicyRef != nil {
				if _, ok := b.updatePolicies[tblInfo.PlacementPolicyRef.ID]; ok {
					b.markTableBundleShouldUpdate(tblInfo.ID)
				}
			}

			if tblInfo.Partition != nil {
				for _, par := range tblInfo.Partition.Definitions {
					if _, ok := b.updatePartitions[par.ID]; ok {
						b.markTableBundleShouldUpdate(tblInfo.ID)
					}
				}
			}
		}
	}
}

func (b *bundleInfoBuilder) updateTableBundles(infoSchemaInterface InfoSchema, tableID int64) {
	is := infoSchemaInterface.base()
	tbl, ok := infoSchemaInterface.TableByID(tableID)
	if !ok {
		b.deleteBundle(is, tableID)
		return
	}

	getter := &policyGetter{is: is}
	bundle, err := placement.NewTableBundle(getter, tbl.Meta())
	if err != nil {
		logutil.BgLogger().Error("create table bundle failed", zap.Error(err))
	} else if bundle != nil {
		is.ruleBundleMap[tableID] = bundle
	} else {
		b.deleteBundle(is, tableID)
	}

	if tbl.Meta().Partition == nil {
		return
	}

	for _, par := range tbl.Meta().Partition.Definitions {
		bundle, err = placement.NewPartitionBundle(getter, par)
		if err != nil {
			logutil.BgLogger().Error("create partition bundle failed",
				zap.Error(err),
				zap.Int64("partition id", par.ID),
			)
		} else if bundle != nil {
			is.ruleBundleMap[par.ID] = bundle
		} else {
			b.deleteBundle(is, par.ID)
		}
	}
}
