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

func TestFlags(t *testing.T) {
	require.True(t, HasNotNullFlag(NotNullFlag))
	require.True(t, HasUniKeyFlag(UniqueKeyFlag))
	require.True(t, HasNotNullFlag(NotNullFlag))
	require.True(t, HasNoDefaultValueFlag(NoDefaultValueFlag))
	require.True(t, HasAutoIncrementFlag(AutoIncrementFlag))
	require.True(t, HasUnsignedFlag(UnsignedFlag))
	require.True(t, HasZerofillFlag(ZerofillFlag))
	require.True(t, HasBinaryFlag(BinaryFlag))
	require.True(t, HasPriKeyFlag(PriKeyFlag))
	require.True(t, HasMultipleKeyFlag(MultipleKeyFlag))
	require.True(t, HasTimestampFlag(TimestampFlag))
	require.True(t, HasOnUpdateNowFlag(OnUpdateNowFlag))
}
