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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

<<<<<<< HEAD:pkg/bindinfo/internal/logutil/logutil.go
import (
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// BindLogger with category "sql-bind" is used to log statistic related messages.
func BindLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String("category", "sql-bind"))
=======
var longTests = map[string][]string{
	"pkg/ttl/ttlworker": {
		"TestParallelLockNewJob",
		"TestParallelLockNewTask",
		"TestJobManagerWithFault",
	},
	"pkg/ttl/cache": {
		"TestRegionDisappearDuringSplitRange",
	},
>>>>>>> fc28ff6fa1b (ttl: fix the issue that TTL cannot start if regions are merged frequently (#61530)):tools/check/longtests.go
}
