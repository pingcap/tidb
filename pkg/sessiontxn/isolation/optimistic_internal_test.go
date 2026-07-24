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
// See the License for the specific language governing permissions and
// limitations under the License.

package isolation

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestOptimisticTxnContextProviderResetAfterCopy(t *testing.T) {
	sctx := mock.NewContext()
	var original OptimisticTxnContextProvider
	original.ResetForNewTxn(sctx, false)
	require.Same(t, &original, original.callbackOwner)

	copied := original
	copied.ResetForNewTxn(sctx, true)
	require.Same(t, &copied, copied.callbackOwner)
	require.Same(t, &original, original.callbackOwner)
	require.True(t, copied.causalConsistencyOnly)
}
