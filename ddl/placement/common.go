// Copyright 2021 PingCAP, Inc.
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

package placement

import (
	"fmt"
)

const (
	// BundleIDPrefix is the bundle prefix of all rule bundles from TiDB_DDL statements.
	BundleIDPrefix = "TiDB_DDL_"
	// PDBundleID is the bundle name of pd, the default bundle for all regions.
	PDBundleID = "pd"
)

// GroupID accepts a tableID or whatever integer, and encode the integer into a valid GroupID for PD.
func GroupID(id int64) string {
	return fmt.Sprintf("%s%d", BundleIDPrefix, id)
}

const (
	// RuleIndexTable is the index for a rule of table.
	RuleIndexTable = 40
	// RuleIndexPartition is the index for a rule of partition.
	RuleIndexPartition = 80
	// RuleIndexTiFlash is the index for a rule of TiFlash.
	RuleIndexTiFlash = 120
)

const (
	// DCLabelKey indicates the key of label which represents the dc for Store.
	// FIXME: currently we assumes "zone" is the dcLabel key in Store
	DCLabelKey = "zone"
	// EngineLabelKey is the label that indicates the backend of store instance:
	// tikv or tiflash. TiFlash instance will contain a label of 'engine: tiflash'.
	EngineLabelKey = "engine"
	// EngineLabelTiFlash is the label value, which a TiFlash instance will have with
	// a label key of EngineLabelKey.
	EngineLabelTiFlash = "tiflash"
	// EngineLabelTiKV is the label value used in some tests. And possibly TiKV will
	// set the engine label with a value of EngineLabelTiKV.
	EngineLabelTiKV = "tikv"
)
