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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
)

func applyCreatePolicy(b *Builder, m *meta.Meta, diff *model.SchemaDiff) error {
	po, err := m.GetPolicy(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if po == nil {
		return ErrPlacementPolicyNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Policy ID %d)", diff.SchemaID),
		)
	}

	if _, ok := b.infoSchema.PolicyByID(po.ID); ok {
		// if old policy with the same id exists, it means replace,
		// so the tables referring this policy's bundle should be updated
		b.markBundlesReferPolicyShouldUpdate(po.ID)
	}

	b.infoSchema.setPolicy(po)
	return nil
}

func applyAlterPolicy(b *Builder, m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	po, err := m.GetPolicy(diff.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if po == nil {
		return nil, ErrPlacementPolicyNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Policy ID %d)", diff.SchemaID),
		)
	}

	b.infoSchema.setPolicy(po)
	b.markBundlesReferPolicyShouldUpdate(po.ID)
	// TODO: return the policy related table ids
	return []int64{}, nil
}

func applyDropPolicy(b *Builder, PolicyID int64) []int64 {
	po, ok := b.infoSchema.PolicyByID(PolicyID)
	if !ok {
		return nil
	}
	b.infoSchema.deletePolicy(po.Name.L)
	// TODO: return the policy related table ids
	return []int64{}
}

func applyCreateOrAlterResourceGroup(b *Builder, m *meta.Meta, diff *model.SchemaDiff) error {
	group, err := m.GetResourceGroup(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if group == nil {
		return ErrResourceGroupNotExists.GenWithStackByArgs(fmt.Sprintf("(Group ID %d)", diff.SchemaID))
	}
	// TODO: need mark updated?
	b.infoSchema.setResourceGroup(group)
	return nil
}

func applyDropResourceGroup(b *Builder, m *meta.Meta, diff *model.SchemaDiff) []int64 {
	group, ok := b.infoSchema.ResourceGroupByID(diff.SchemaID)
	if !ok {
		return nil
	}
	b.infoSchema.deleteResourceGroup(group.Name.L)
	// TODO: return the related information.
	return []int64{}
}

func (b *Builder) addTemporaryTable(tblID int64) {
	if b.infoSchema.temporaryTableIDs == nil {
		b.infoSchema.temporaryTableIDs = make(map[int64]struct{})
	}
	b.infoSchema.temporaryTableIDs[tblID] = struct{}{}
}

func (b *Builder) copyBundlesMap(oldIS *infoSchema) {
	b.infoSchema.ruleBundleMap = make(map[int64]*placement.Bundle)
	for id, v := range oldIS.ruleBundleMap {
		b.infoSchema.ruleBundleMap[id] = v
	}
}

func (b *Builder) copyPoliciesMap(oldIS *infoSchema) {
	is := b.infoSchema
	for _, v := range oldIS.AllPlacementPolicies() {
		is.policyMap[v.Name.L] = v
	}
}

func (b *Builder) copyResourceGroupMap(oldIS *infoSchema) {
	is := b.infoSchema
	for _, v := range oldIS.AllResourceGroups() {
		is.resourceGroupMap[v.Name.L] = v
	}
}

func (b *Builder) copyTemporaryTableIDsMap(oldIS *infoSchema) {
	is := b.infoSchema
	if len(oldIS.temporaryTableIDs) == 0 {
		is.temporaryTableIDs = nil
		return
	}

	is.temporaryTableIDs = make(map[int64]struct{})
	for tblID := range oldIS.temporaryTableIDs {
		is.temporaryTableIDs[tblID] = struct{}{}
	}
}

func (b *Builder) copyReferredForeignKeyMap(oldIS *infoSchema) {
	for k, v := range oldIS.referredForeignKeyMap {
		b.infoSchema.referredForeignKeyMap[k] = v
	}
}

func (b *Builder) initMisc(dbInfos []*model.DBInfo, policies []*model.PolicyInfo, resourceGroups []*model.ResourceGroupInfo) {
	info := b.infoSchema
	// build the policies.
	for _, policy := range policies {
		info.setPolicy(policy)
	}

	// build the groups.
	for _, group := range resourceGroups {
		info.setResourceGroup(group)
	}

	// Maintain foreign key reference information.
	for _, di := range dbInfos {
		for _, t := range di.Tables {
			b.infoSchema.addReferredForeignKeys(di.Name, t)
		}
	}
}
