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

package domain

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/tidb/pkg/domain/infosync"
)

// GetOptimizerTraceDirName returns optimizer trace directory path.
// The path is a relative path for external storage.
func GetOptimizerTraceDirName() string {
	instanceID := ""
	if info, err := infosync.GetServerInfo(); err == nil && info != nil {
		instanceID = info.ID
	}
	// If instanceID is empty, use the process ID as the instance ID.
	// This is a fallback for the case where the instance ID is not set.
	if instanceID == "" {
		instanceID = strconv.Itoa(os.Getpid())
	}
	return filepath.Join("optimizer_trace", instanceID)
}
