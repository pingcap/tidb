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
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"crypto/sha256"
	"fmt"
)

// TableIdentifier contains all the information needed to uniquely identify a restore task
type TableIdentifier struct {
	StartTS           uint64
	EndTS             uint64
	Filter            []string
	UpstreamClusterID uint64
	TaskType          string
}

// GenerateTableNameSuffix generates a unique suffix for checkpoint table names based on restore parameters
func (t *TableIdentifier) GenerateTableNameSuffix() string {
	// combine all parameters into a unique string
	uniqueStr := fmt.Sprintf("%d-%d-%s-%s-%d-%s",
		t.StartTS,
		t.EndTS,
		t.Filter,
		t.UpstreamClusterID,
		t.TaskType)

	// use hash to avoid conflicts and keep names short
	hash := sha256.Sum256([]byte(uniqueStr))
	return fmt.Sprintf("_%x", hash[:8])
}

// GetCheckpointTableName returns the checkpoint table name with the suffix
func GetCheckpointTableName(baseTableName string, suffix string) string {
	return baseTableName + suffix
}
