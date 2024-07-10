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

package timeutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSleep(t *testing.T) {
	const contextTimeout, sleepTime = 10 * time.Millisecond, 10 * time.Second
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()
	Sleep(ctx, sleepTime)
	since := time.Since(now)
	require.Greater(t, since, contextTimeout)
	require.Less(t, since, sleepTime)
}
