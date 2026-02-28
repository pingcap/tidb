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

package mvservice

import (
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

func calcTSOBeforeDuration(currentTSO uint64, retention time.Duration) uint64 {
	if retention <= 0 {
		return currentTSO
	}
	cutoffPhysical := oracle.ExtractPhysical(currentTSO) - int64(retention/time.Millisecond)
	if cutoffPhysical < 0 {
		cutoffPhysical = 0
	}
	return oracle.ComposeTS(cutoffPhysical, 0)
}
