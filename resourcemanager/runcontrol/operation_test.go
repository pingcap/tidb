// Copyright 2022 PingCAP, Inc.
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

package runcontrol_test

import (
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
	requires "github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

var (
	errUnsupportedOpType = strconv.Itoa(mysql.ErrUnsupportedBackendOpType)
	errDupPauseOp        = strconv.Itoa(mysql.ErrDuplicatePauseOperation)
	errOnExecution       = strconv.Itoa(mysql.ErrOnExecOperation)
	errNoPause           = strconv.Itoa(mysql.ErrNoCounterPauseForResume)
)

// TestBackendTaskPauseResume backend task operations table and history has been created.
func TestBackendTaskPauseResume(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Test unsupported backend type.
	_, err := tk.Exec("admin pause abc")
	requires.ErrorContains(t, err, errUnsupportedOpType)

	// Test issue the pause|resume commands
	tk.MustExec("admin pause ddl")
	tk.MustExec("admin resume ddl")

	// Test issue only resume command, no pause command;
	_, err = tk.Exec("admin resume ddl")
	requires.ErrorContains(t, err, errNoPause)

	// Test issue the pause|resume commands
	tk.MustExec("admin pause ddl force")
	_, err = tk.Exec("admin pause ddl")
	requires.ErrorContains(t, err, errDupPauseOp)
}
