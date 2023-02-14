// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package errors_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsContextCanceled(t *testing.T) {
	require.False(t, berrors.IsContextCanceled(nil))
	require.False(t, berrors.IsContextCanceled(errors.New("connection closed")))
	require.True(t, berrors.IsContextCanceled(context.Canceled))
	require.True(t, berrors.IsContextCanceled(context.DeadlineExceeded))
	require.True(t, berrors.IsContextCanceled(errors.Trace(context.Canceled)))
	require.True(t, berrors.IsContextCanceled(errors.Trace(context.DeadlineExceeded)))
	require.True(t, berrors.IsContextCanceled(&url.Error{Err: context.Canceled}))
	require.True(t, berrors.IsContextCanceled(&url.Error{Err: context.DeadlineExceeded}))
}

func TestEqual(t *testing.T) {
	err := errors.Annotate(berrors.ErrPDBatchScanRegion, "test error equla")
	r := berrors.ErrPDBatchScanRegion.Equal(err)
	require.True(t, r)
}
