// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync2

import (
	"fmt"
	"testing"
	"time"
)

type testService struct {
	activated AtomicInt64
	t         *testing.T
}

func (ts *testService) service(svc *ServiceContext) error {
	if !ts.activated.CompareAndSwap(0, 1) {
		ts.t.Fatalf("service called more than once")
	}
	for svc.IsRunning() {
		time.Sleep(10 * time.Millisecond)

	}
	if !ts.activated.CompareAndSwap(1, 0) {
		ts.t.Fatalf("service ended more than once")
	}
	return nil
}

func (ts *testService) selectService(svc *ServiceContext) error {
	if !ts.activated.CompareAndSwap(0, 1) {
		ts.t.Fatalf("service called more than once")
	}
serviceLoop:
	for svc.IsRunning() {
		select {
		case <-time.After(1 * time.Second):
			ts.t.Errorf("service didn't stop when shutdown channel was closed")
		case <-svc.ShuttingDown:
			break serviceLoop
		}
	}
	if !ts.activated.CompareAndSwap(1, 0) {
		ts.t.Fatalf("service ended more than once")
	}
	return nil
}

func TestServiceManager(t *testing.T) {
	ts := &testService{t: t}
	var sm ServiceManager
	if sm.StateName() != "Stopped" {
		t.Errorf("want Stopped, got %s", sm.StateName())
	}
	result := sm.Go(ts.service)
	if !result {
		t.Errorf("want true, got false")
	}
	if sm.StateName() != "Running" {
		t.Errorf("want Running, got %s", sm.StateName())
	}
	time.Sleep(5 * time.Millisecond)
	if val := ts.activated.Get(); val != 1 {
		t.Errorf("want 1, got %d", val)
	}
	result = sm.Go(ts.service)
	if result {
		t.Errorf("want false, got true")
	}
	result = sm.Stop()
	if !result {
		t.Errorf("want true, got false")
	}
	if val := ts.activated.Get(); val != 0 {
		t.Errorf("want 0, got %d", val)
	}
	result = sm.Stop()
	if result {
		t.Errorf("want false, got true")
	}
	sm.state.Set(SERVICE_SHUTTING_DOWN)
	if sm.StateName() != "ShuttingDown" {
		t.Errorf("want ShuttingDown, got %s", sm.StateName())
	}
}

func TestServiceManagerSelect(t *testing.T) {
	ts := &testService{t: t}
	var sm ServiceManager
	if sm.StateName() != "Stopped" {
		t.Errorf("want Stopped, got %s", sm.StateName())
	}
	result := sm.Go(ts.selectService)
	if !result {
		t.Errorf("want true, got false")
	}
	if sm.StateName() != "Running" {
		t.Errorf("want Running, got %s", sm.StateName())
	}
	time.Sleep(5 * time.Millisecond)
	if val := ts.activated.Get(); val != 1 {
		t.Errorf("want 1, got %d", val)
	}
	result = sm.Go(ts.service)
	if result {
		t.Errorf("want false, got true")
	}
	result = sm.Stop()
	if !result {
		t.Errorf("want true, got false")
	}
	if val := ts.activated.Get(); val != 0 {
		t.Errorf("want 0, got %d", val)
	}
	result = sm.Stop()
	if result {
		t.Errorf("want false, got true")
	}
	sm.state.Set(SERVICE_SHUTTING_DOWN)
	if sm.StateName() != "ShuttingDown" {
		t.Errorf("want ShuttingDown, got %s", sm.StateName())
	}
}

func TestServiceManagerWaitNotRunning(t *testing.T) {
	done := make(chan struct{})
	var sm ServiceManager
	go func() {
		sm.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Errorf("Wait() blocked even though service wasn't running.")
	}
}

func TestServiceManagerWait(t *testing.T) {
	done := make(chan struct{})
	stop := make(chan struct{})
	var sm ServiceManager
	sm.Go(func(*ServiceContext) error {
		<-stop
		return nil
	})
	go func() {
		sm.Wait()
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-done:
		t.Errorf("Wait() didn't block while service was still running.")
	default:
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Wait() didn't unblock when service stopped.")
	}
}

func TestServiceManagerJoin(t *testing.T) {
	want := "error 123"
	var sm ServiceManager
	sm.Go(func(*ServiceContext) error {
		return fmt.Errorf("error 123")
	})
	if got := sm.Join().Error(); got != want {
		t.Errorf("Join().Error() = %#v, want %#v", got, want)
	}
}
