// Copyright 2023 PingCAP, Inc.
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

package precheck

import (
	"context"
)

// CheckType represents the check type.
type CheckType string

// CheckType constants.
const (
	Critical CheckType = "critical"
	Warn     CheckType = "performance"
)

// CheckItemID is the ID of a precheck item
type CheckItemID string

// CheckItemID constants
const (
	CheckLargeDataFile            CheckItemID = "CHECK_LARGE_DATA_FILES"
	CheckSourcePermission         CheckItemID = "CHECK_SOURCE_PERMISSION"
	CheckTargetTableEmpty         CheckItemID = "CHECK_TARGET_TABLE_EMPTY"
	CheckSourceSchemaValid        CheckItemID = "CHECK_SOURCE_SCHEMA_VALID"
	CheckCheckpoints              CheckItemID = "CHECK_CHECKPOINTS"
	CheckCSVHeader                CheckItemID = "CHECK_CSV_HEADER"
	CheckTargetClusterSize        CheckItemID = "CHECK_TARGET_CLUSTER_SIZE"
	CheckTargetClusterEmptyRegion CheckItemID = "CHECK_TARGET_CLUSTER_EMPTY_REGION"
	CheckTargetClusterRegionDist  CheckItemID = "CHECK_TARGET_CLUSTER_REGION_DISTRIBUTION"
	CheckTargetClusterVersion     CheckItemID = "CHECK_TARGET_CLUSTER_VERSION"
	CheckLocalDiskPlacement       CheckItemID = "CHECK_LOCAL_DISK_PLACEMENT"
	CheckLocalTempKVDir           CheckItemID = "CHECK_LOCAL_TEMP_KV_DIR"
	CheckTargetUsingCDCPITR       CheckItemID = "CHECK_TARGET_USING_CDC_PITR"
)

var (
	// CheckItemIDToDisplayName is a map from CheckItemID to its display name
	checkItemIDToDisplayName = map[CheckItemID]string{
		CheckLargeDataFile:            "Large data file",
		CheckSourcePermission:         "Source permission",
		CheckTargetTableEmpty:         "Target table empty",
		CheckSourceSchemaValid:        "Source schema valid",
		CheckCheckpoints:              "Checkpoints",
		CheckCSVHeader:                "CSV header",
		CheckTargetClusterSize:        "Target cluster size",
		CheckTargetClusterEmptyRegion: "Target cluster empty region",
		CheckTargetClusterRegionDist:  "Target cluster region dist",
		CheckTargetClusterVersion:     "Target cluster version",
		CheckLocalDiskPlacement:       "Local disk placement",
		CheckLocalTempKVDir:           "Local temp KV dir",
		CheckTargetUsingCDCPITR:       "Target using CDC/PITR",
	}
)

// DisplayName returns display name for it.
func (c CheckItemID) DisplayName() string {
	return checkItemIDToDisplayName[c]
}

// CheckResult is the result of a precheck item
type CheckResult struct {
	Item     CheckItemID
	Severity CheckType
	Passed   bool
	Message  string
}

// Checker is the interface for precheck items
type Checker interface {
	// Check checks whether it meet some prerequisites for importing
	// If the check is skipped, the returned `CheckResult` is nil
	Check(ctx context.Context) (*CheckResult, error)
	GetCheckItemID() CheckItemID
}
