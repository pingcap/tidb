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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
)

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()
	tk.MustExec("create table test.t (id int);")

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("IMPORT INTO test.t FROM '/file.csv'", "[planner:8132]Feature 'IMPORT INTO from server disk' is not supported when security enhanced mode is enabled")
}
