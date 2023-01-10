// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package daemon_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/owner"
	"github.com/stretchr/testify/require"
)

type anApp struct {
	sync.Mutex
	begun bool

	tickingMessenger     chan struct{}
	tickingMessengerOnce *sync.Once
	stopMessenger        chan struct{}
	startMessenger       chan struct{}

	tCtx *testing.T
}

func newTestApp(t *testing.T) *anApp {
	return &anApp{
		tCtx:           t,
		startMessenger: make(chan struct{}),
	}
}

// OnStart would be called once become the owner.
// The context passed in would be canceled once it is no more the owner.
func (a *anApp) OnStart(ctx context.Context) {
	a.Lock()
	defer a.Unlock()
	if a.begun {
		a.tCtx.Fatalf("failed: an app is started twice")
	}
	a.begun = true
	a.tickingMessenger = make(chan struct{})
	a.tickingMessengerOnce = new(sync.Once)
	a.stopMessenger = make(chan struct{})
	go func() {
		<-ctx.Done()

		a.Lock()
		defer a.Unlock()

		a.begun = false
		a.tickingMessenger = nil
		a.startMessenger = make(chan struct{})
		close(a.stopMessenger)
	}()
	close(a.startMessenger)
}

// OnTick would be called periodically.
// The error can be recorded.
func (a *anApp) OnTick(ctx context.Context) error {
	log.Info("tick")
	a.Lock()
	defer a.Unlock()
	if !a.begun {
		a.tCtx.Fatal("failed: an app is ticking before start")
	}
	a.tickingMessengerOnce.Do(func() {
		log.Info("close")
		close(a.tickingMessenger)
	})
	return nil
}

// Name returns the name which is used for tracing the daemon.
func (a *anApp) Name() string {
	return "testing"
}

func (a *anApp) Running() bool {
	a.Lock()
	defer a.Unlock()

	return a.begun
}

func (a *anApp) AssertTick(timeout time.Duration) {
	a.Lock()
	messenger := a.tickingMessenger
	a.Unlock()
	log.Info("waiting")
	select {
	case <-messenger:
	case <-time.After(timeout):
		a.tCtx.Fatalf("tick not triggered after %s", timeout)
	}
}

func (a *anApp) AssertNotRunning(timeout time.Duration) {
	a.Lock()
	messenger := a.stopMessenger
	a.Unlock()
	select {
	case <-messenger:
	case <-time.After(timeout):
		a.tCtx.Fatalf("stop not triggered after %s", timeout)
	}
}

func (a *anApp) AssertStart(timeout time.Duration) {
	a.Lock()
	messenger := a.startMessenger
	a.Unlock()
	select {
	case <-messenger:
	case <-time.After(timeout):
		a.tCtx.Fatalf("start not triggered after %s", timeout)
	}
}

func TestDaemon(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := require.New(t)
	app := newTestApp(t)
	ow := owner.NewMockManager(ctx, "owner_daemon_test")
	d := daemon.New(app, ow, 100*time.Millisecond)

	f, err := d.Begin(ctx)
	req.NoError(err)
	go f()
	app.AssertStart(1 * time.Second)
	app.AssertTick(1 * time.Second)
	ow.RetireOwner()
	req.False(ow.IsOwner())
	app.AssertNotRunning(1 * time.Second)
	ow.CampaignOwner()
	req.True(ow.IsOwner())
	app.AssertStart(1 * time.Second)
	app.AssertTick(1 * time.Second)
}
