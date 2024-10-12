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

package logicalop

import (
	"bytes"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// TiKVSingleGather is a leaf logical operator of TiDB layer to gather
// tuples from TiKV regions.
type TiKVSingleGather struct {
	LogicalSchemaProducer
	Source *DataSource
	// IsIndexGather marks if this TiKVSingleGather gathers tuples from an IndexScan.
	// in implementation phase, we need this flag to determine whether to generate
	// PhysicalTableReader or PhysicalIndexReader.
	IsIndexGather bool
	Index         *model.IndexInfo
}

// Init initializes TiKVSingleGather.
func (sg TiKVSingleGather) Init(ctx base.PlanContext, offset int) *TiKVSingleGather {
	sg.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeTiKVSingleGather, &sg, offset)
	return &sg
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (sg *TiKVSingleGather) ExplainInfo() string {
	buffer := bytes.NewBufferString(sg.Source.ExplainInfo())
	if sg.IsIndexGather {
		buffer.WriteString(", index:" + sg.Index.Name.String())
	}
	return buffer.String()
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown inherits BaseLogicalPlan.LogicalPlan.<1st> implementation.

// PruneColumns inherits BaseLogicalPlan.LogicalPlan.<2nd> implementation.

// FindBestTask inherits BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (*TiKVSingleGather) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = childSchema[0].Keys
}

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats inherits BaseLogicalPlan.LogicalPlan.<11th> implementation.

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (*TiKVSingleGather) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// ExhaustPhysicalPlans inherits BaseLogicalPlan.LogicalPlan.<14th> implementation.

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************
