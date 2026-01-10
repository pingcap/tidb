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
