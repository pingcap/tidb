// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLError(t *testing.T) {
	e := NewErrf(ErrNoDB, "no db error", nil)
	require.Greater(t, len(e.Error()), 0)

	e = NewErrf(0, "customized error", nil)
	require.Greater(t, len(e.Error()), 0)

	e = NewErr(ErrNoDB)
	require.Greater(t, len(e.Error()), 0)

	e = NewErr(0, "customized error", nil)
	require.Greater(t, len(e.Error()), 0)
}
