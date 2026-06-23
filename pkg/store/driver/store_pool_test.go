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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorePoolBlocksWhenFull(t *testing.T) {
	p := newStorePool(1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	require.NoError(t, p.Run(func() {
		close(firstStarted)
		<-releaseFirst
	}))
	<-firstStarted

	secondRunReturned := make(chan error, 1)
	secondTaskStarted := make(chan struct{})
	go func() {
		secondRunReturned <- p.Run(func() {
			close(secondTaskStarted)
		})
	}()

	select {
	case err := <-secondRunReturned:
		require.Failf(t, "expected Run to block", "unexpectedly returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	require.NoError(t, <-secondRunReturned)

	select {
	case <-secondTaskStarted:
	case <-time.After(time.Second):
		require.Fail(t, "expected second task to start")
	}
}

func TestStorePoolCloseUnblocksRun(t *testing.T) {
	p := newStorePool(1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	require.NoError(t, p.Run(func() {
		close(firstStarted)
		<-releaseFirst
	}))
	<-firstStarted

	runReturned := make(chan error, 1)
	go func() {
		runReturned <- p.Run(func() {})
	}()

	select {
	case err := <-runReturned:
		require.Failf(t, "expected Run to block", "unexpectedly returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	p.Close()
	err := <-runReturned
	require.ErrorIs(t, err, errStorePoolClosed)

	close(releaseFirst)
}
