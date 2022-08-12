// Copyright 2016 PingCAP, Inc.
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

package copr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBalanceBatchCopTaskWithEmptyTaskSet(t *testing.T) {
	{
		var nilTaskSet []*batchCopTask
		nilResult := balanceBatchCopTask(nil, nil, nilTaskSet, nil, time.Second)
		require.True(t, nilResult == nil)
	}

	{
		emptyTaskSet := make([]*batchCopTask, 0)
		emptyResult := balanceBatchCopTask(nil, nil, emptyTaskSet, nil, time.Second)
		require.True(t, emptyResult != nil)
		require.True(t, len(emptyResult) == 0)
	}
}
