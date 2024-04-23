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
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type BuilderV3 struct {
	builderV1 *Builder
	builderV2 *Builder
}

func NewBuilderV3(r autoid.Requirement, factory func() (pools.Resource, error), infoData *Data) *BuilderV3 {
	v3 := &BuilderV3{
		builderV1: NewBuilder(r, factory, infoData),
		builderV2: NewBuilder(r, factory, infoData),
	}
	v3.builderV1.enableV2 = false
	v3.builderV2.enableV2 = true
	return v3
}

func (b *BuilderV3) InitWithOldInfoSchema(oldSchema InfoSchema) (*BuilderV3, error) {
	oldSchemaV1 := oldSchema.(*InfoSchemaWithTS).InfoSchema.(*infoschemaV3).infoV1
	oldSchemaV2 := oldSchema.(*InfoSchemaWithTS).InfoSchema.(*infoschemaV3).infoV2

	_, err1 := b.builderV1.InitWithOldInfoSchema(oldSchemaV1)
	_, err2 := b.builderV2.InitWithOldInfoSchema(oldSchemaV2)
	if !errors.ErrorEqual(err1, err2) {
		panic("err1 != err2")
	}
	if err1 != nil {
		return nil, err1
	}
	return b, nil
}

func (b *BuilderV3) InitWithDBInfos(dbInfos []*model.DBInfo, policies []*model.PolicyInfo, resourceGroups []*model.ResourceGroupInfo, schemaVersion int64) (*BuilderV3, error) {
	_, err1 := b.builderV1.InitWithDBInfos(dbInfos, policies, resourceGroups, schemaVersion)
	_, err2 := b.builderV2.InitWithDBInfos(dbInfos, policies, resourceGroups, schemaVersion)
	if !errors.ErrorEqual(err1, err2) {
		panic("err1 != err2")
	}
	if err1 != nil {
		return nil, err1
	}
	return b, nil
}

func (b *BuilderV3) Build(schemaTS uint64) InfoSchema {
	v1 := b.builderV1.Build(schemaTS)
	v2 := b.builderV2.Build(schemaTS)
	return &infoschemaV3{infoV1: v1.(*infoSchema), infoV2: v2.(*infoschemaV2)}
}

func (b *BuilderV3) SetDeltaUpdateBundles() {
	b.builderV1.SetDeltaUpdateBundles()
	b.builderV2.SetDeltaUpdateBundles()
}

func (b *BuilderV3) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	rs1, err1 := b.builderV1.ApplyDiff(m, diff)
	rs2, err2 := b.builderV2.ApplyDiff(m, diff)
	if !errors.ErrorEqual(err1, err2) {
		panic("err1 != err2")
	}
	if err1 != nil {
		return nil, err1
	}
	if len(rs1) != len(rs2) {
		panic("len(rs1) != len(rs2)")
	}
	for i := range rs1 {
		if rs1[i] != rs2[i] {
			panic("rs1[i] != rs2[i]")
		}
	}
	return rs1, nil
}
