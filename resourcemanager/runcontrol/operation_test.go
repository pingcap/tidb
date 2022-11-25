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
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
	requires "github.com/stretchr/testify/require"
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
	tk.MustExec("admin pause ddl")
	_, err = tk.Exec("admin resume abc")
	requires.ErrorContains(t, err, errUnsupportedOpType)
	tk.MustExec("admin resume ddl")

	// Test pause|resume command pairs execution
	tk.MustExec("admin pause ddl")
	tk.MustExec("admin resume ddl")

	// Test issue only resume command, no pause command;
	_, err = tk.Exec("admin resume ddl")
	requires.ErrorContains(t, err, errNoPause)

	// Test issue one more pause command.
	tk.MustExec("admin pause ddl force")
	_, err = tk.Exec("admin pause ddl")
	requires.ErrorContains(t, err, errDupPauseOp)
}

// TestBackendOperationfailure mock backend operation execution failure.
func TestBackendOperationfailure(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Test execution pause command error.
	requires.NoError(t, failpoint.Enable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionPauseOnError", "return(true)"))
	_, err := tk.Exec("admin pause ddl")
	requires.ErrorContains(t, err, errOnExecution)
	requires.NoError(t, failpoint.Disable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionPauseOnError"))

	// Test execution get pause error.
	requires.NoError(t, failpoint.Enable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionGetPauseOnError", "return(true)"))
	tk.MustExec("admin pause ddl force")
	_, err = tk.Exec("admin resume ddl")
	requires.ErrorContains(t, err, errOnExecution)
	requires.NoError(t, failpoint.Disable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionGetPauseOnError"))
	tk.MustExec("admin resume ddl")

	// Test execution resume command error.
	requires.NoError(t, failpoint.Enable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionResumeOnError", "return(true)"))
	tk.MustExec("admin pause ddl force")
	_, err = tk.Exec("admin resume ddl")
	requires.ErrorContains(t, err, errOnExecution)
	requires.NoError(t, failpoint.Disable("github.com/pingcap/tidb/resourcemanager/runcontrol/executionResumeOnError"))
	tk.MustExec("admin resume ddl")
}
