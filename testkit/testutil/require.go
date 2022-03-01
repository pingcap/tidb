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

//go:build !codes
// +build !codes

package testutil

import (
	"testing"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

// DatumEqual verifies that the actual value is equal to the expected value. For string datum, they are compared by the binary collation.
func DatumEqual(t *testing.T, expected, actual types.Datum, msgAndArgs ...interface{}) {
	sc := new(stmtctx.StatementContext)
	res, err := actual.Compare(sc, &expected, collate.GetBinaryCollator())
	require.NoError(t, err, msgAndArgs)
	require.Zero(t, res, msgAndArgs)
}
