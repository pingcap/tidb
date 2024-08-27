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
)

// GetOptimizerTraceDirName returns optimizer trace directory path.
// The path is related to the process id.
func GetOptimizerTraceDirName() string {
	return filepath.Join(os.TempDir(), "optimizer_trace", strconv.Itoa(os.Getpid()))
}
