// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestRunMain(t *testing.T) {
	t.Run("run-main", func(t *testing.T) {
		if _, isIntegrationTest := os.LookupEnv("INTEGRATION_TEST"); !isIntegrationTest {
			// override exit to pass unit test.
			exit = func(code int) {}
		}

		var args []string
		for _, arg := range os.Args {
			switch {
			case arg == "DEVEL":
			case strings.HasPrefix(arg, "-test."):
			default:
				args = append(args, arg)
			}
		}

		waitCh := make(chan struct{}, 1)

		os.Args = args
		go func() {
			main()
			close(waitCh)
		}()

		<-waitCh
	})

	t.Run("checkpoint table not found does not print stack", func(t *testing.T) {
		err := common.ErrCheckpointTableNotFound.GenWithStackByArgs("`db`.`table`")
		formatted := formatFatalError(err)
		require.Contains(t, formatted, err.Error())
		require.NotContains(t, formatted, "main_test.go")
		require.Contains(t, formatted, "--checkpoint-error-ignore='`db`.`table`'")
		require.Contains(t, formatted, "--checkpoint-error-destroy='`db`.`table`'")
	})

	t.Run("generic errors still print stack", func(t *testing.T) {
		err := errors.New("boom")
		formatted := formatFatalError(err)
		require.NotEqual(t, err.Error(), formatted)
		require.Contains(t, formatted, "main_test.go")
	})
}
