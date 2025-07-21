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

package ingest

import (
	"fmt"
	"os"

	"github.com/pingcap/tidb/pkg/metrics"
)

// CheckIngestLeakageForTest is only used in test.
func CheckIngestLeakageForTest(exitCode int) {
	if exitCode == 0 {
		if registeredJob := metrics.GetRegisteredJob(); len(registeredJob) > 0 {
			fmt.Fprintf(os.Stderr, "add index metrics leakage: %v\n", registeredJob)
			os.Exit(1)
		}
	}
	os.Exit(exitCode)
}
