// Copyright 2020 PingCAP, Inc.
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

package placement

// BundleIDPrefix is the bundle prefix of all rules from TiDB_DDL statements.
const BundleIDPrefix = "TiDB_DDL_"

const (
	// RuleIndexDefault is the default index for a rule, check Rule.Index.
	RuleIndexDefault int = iota
	// RuleIndexDatabase is the index for a rule of database.
	RuleIndexDatabase
	// RuleIndexTable is the index for a rule of table.
	RuleIndexTable
	// RuleIndexPartition is the index for a rule of partition.
	RuleIndexPartition
	// RuleIndexIndex is the index for a rule of index.
	RuleIndexIndex
)

// DCLabelKey indicates the key of label which represents the dc for Store.
// FIXME: currently we assumes "zone" is the dcLabel key in Store
const DCLabelKey = "zone"
