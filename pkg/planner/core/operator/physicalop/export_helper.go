// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// ExportPhysicalExchangeReceiver is an exported alias for PhysicalExchangeReceiver.
type ExportPhysicalExchangeReceiver = PhysicalExchangeReceiver

// ExportPhysicalExchangeSender is an exported alias for PhysicalExchangeSender.
type ExportPhysicalExchangeSender = PhysicalExchangeSender

// ExportPhysicalHashJoin is an exported alias for PhysicalHashJoin.
type ExportPhysicalHashJoin = PhysicalHashJoin

// ExportPhysicalLimit is an exported alias for PhysicalLimit.
type ExportPhysicalLimit = PhysicalLimit

// ExportPhysicalSelection is an exported alias for PhysicalSelection.
type ExportPhysicalSelection = PhysicalSelection

// ExportPhysicalProjection is an exported alias for PhysicalProjection.
type ExportPhysicalProjection = PhysicalProjection

// ExportPhysicalTableReader is an exported alias for PhysicalTableReader.
type ExportPhysicalTableReader = PhysicalTableReader

// ExportPhysicalLocalIndexLookUp is an exported alias for PhysicalLocalIndexLookUp.
type ExportPhysicalLocalIndexLookUp = PhysicalLocalIndexLookUp

// ExportPhysicalIndexScan is an exported alias for PhysicalIndexScan.
type ExportPhysicalIndexScan = PhysicalIndexScan

// ExportPhysicalTableScan is an exported alias for PhysicalTableScan.
type ExportPhysicalTableScan = PhysicalTableScan

// ExportPhysicalCTE is an exported alias for PhysicalCTE.
type ExportPhysicalCTE = PhysicalCTE

// ExportPhysicalUnionAll is an exported alias for PhysicalUnionAll.
type ExportPhysicalUnionAll = PhysicalUnionAll

// ExportFragment is an exported alias for Fragment.
type ExportFragment = Fragment

// ExportFlattenListPushDownPlan is an exported alias for FlattenListPushDownPlan.
func ExportFlattenListPushDownPlan(p base.PhysicalPlan) []base.PhysicalPlan {
	return FlattenListPushDownPlan(p)
}

// ExportFlattenTreePushDownPlan is an exported alias for FlattenTreePushDownPlan.
func ExportFlattenTreePushDownPlan(p base.PhysicalPlan) ([]base.PhysicalPlan, map[int]int) {
	return FlattenTreePushDownPlan(p)
}

// ExportFragmentInit is an exported wrapper for Fragment.init method.
func ExportFragmentInit(f *ExportFragment, p base.PhysicalPlan) error {
	return f.init(p)
}

// ExportFragmentGetSingleton is an exported getter for Fragment.singleton field.
func ExportFragmentGetSingleton(f *ExportFragment) bool {
	return f.singleton
}

// ExportFragmentSetSingleton is an exported setter for Fragment.singleton field.
func ExportFragmentSetSingleton(f *ExportFragment, value bool) {
	f.singleton = value
}
