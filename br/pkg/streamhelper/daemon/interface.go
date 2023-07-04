// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package daemon

import "context"

// Interface describes the lifetime hook of a daemon application.
type Interface interface {
	// OnStart start the service whatever the tidb-server is owner or not.
	OnStart(ctx context.Context)
	// OnBecomeOwner would be called once become the owner.
	// The context passed in would be canceled once it is no more the owner.
	OnBecomeOwner(ctx context.Context)
	// OnTick would be called periodically.
	// The error can be recorded.
	OnTick(ctx context.Context) error
	// Name returns the name which is used for tracing the daemon.
	Name() string
}
