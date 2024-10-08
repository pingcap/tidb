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

package rule

import "github.com/pingcap/tidb/pkg/planner/core/rule/util"

// rule/pkg should rely on operator/pkg to do type check and dig in and out,
// rule/util doesn't have to rely on rule/pkg, but it can be put with rule
// handling logic, and be referenced by operator/pkg.
// the core usage only care and call about the rule/pkg and operator/pkg.

func init() {
	util.BuildKeyInfoPortal = buildKeyInfo
	util.SetPredicatePushDownFlag = setPredicatePushDownFlag
}
