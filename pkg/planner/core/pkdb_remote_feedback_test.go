package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRemotePlanFeedbackDisableAfterConsecutiveLarge(t *testing.T) {
	var fb RemotePlanFeedback
	now := time.Unix(0, 123)

	const (
		threshold    = int64(100)
		disableAfter = int32(2)
	)
	cooldown := 10 * time.Second

	require.False(t, fb.ForwardingDisabled(now))
	require.False(t, fb.RecordResult(now, threshold+1, threshold, disableAfter, cooldown))
	require.False(t, fb.ForwardingDisabled(now))

	require.True(t, fb.RecordResult(now, threshold+1, threshold, disableAfter, cooldown))
	require.True(t, fb.ForwardingDisabled(now))
	require.False(t, fb.ForwardingDisabled(now.Add(cooldown).Add(time.Nanosecond)))
}

func TestRemotePlanFeedbackResetOnSmallResult(t *testing.T) {
	var fb RemotePlanFeedback
	now := time.Unix(0, 123)

	const (
		threshold    = int64(100)
		disableAfter = int32(2)
	)
	cooldown := 10 * time.Second

	require.False(t, fb.RecordResult(now, threshold+1, threshold, disableAfter, cooldown))
	require.False(t, fb.RecordResult(now, threshold-1, threshold, disableAfter, cooldown))
	require.False(t, fb.RecordResult(now, threshold+1, threshold, disableAfter, cooldown))
	require.False(t, fb.ForwardingDisabled(now))
}
