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

package property

import (
	"github.com/pingcap/tidb/pkg/expression"
)

// Exported types for testing

// ExportedMPPPartitionColumn exports MPPPartitionColumn for testing
type ExportedMPPPartitionColumn = MPPPartitionColumn

// ExportedNewMPPPartitionColumn creates a new MPPPartitionColumn for testing
func ExportedNewMPPPartitionColumn(col *expression.Column, collateID int32) *MPPPartitionColumn {
	return &MPPPartitionColumn{
		Col:       col,
		CollateID: collateID,
	}
}

// ExportedSliceFromExportedMPPPartitionColumn converts a slice of ExportedMPPPartitionColumn to a slice of MPPPartitionColumn
func ExportedSliceFromExportedMPPPartitionColumn(cols []*ExportedMPPPartitionColumn) []*MPPPartitionColumn {
	return cols
}
