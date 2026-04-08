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

package executor

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

// https://github.com/pingcap/tidb/issues/45690
func TestGetAnalyzePanicErr(t *testing.T) {
	errMsg := fmt.Sprintf("%s", getAnalyzePanicErr(exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(123)))
	require.NotContains(t, errMsg, `%!(EXTRA`)
}
