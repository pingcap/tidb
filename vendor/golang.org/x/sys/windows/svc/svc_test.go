// Copyright 2012 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build windows

package svc_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

func getState(t *testing.T, s *mgr.Service) svc.State {
	status, err := s.Query()
	if err != nil {
		t.Fatalf("Query(%s) failed: %s", s.Name, err)
	}
	return status.State
}

func testState(t *testing.T, s *mgr.Service, want svc.State) {
	have := getState(t, s)
	if have != want {
		t.Fatalf("%s state is=%d want=%d", s.Name, have, want)
	}
}

func waitState(t *testing.T, s *mgr.Service, want svc.State) {
	for i := 0; ; i++ {
		have := getState(t, s)
		if have == want {
			return
		}
		if i > 10 {
			t.Fatalf("%s state is=%d, waiting timeout", s.Name, have)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func TestExample(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode - it modifies system services")
	}

	const name = "myservice"

	m, err := mgr.Connect()
	if err != nil {
		t.Fatalf("SCM connection failed: %s", err)
	}
	defer m.Disconnect()

	dir, err := ioutil.TempDir("", "svc")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	exepath := filepath.Join(dir, "a.exe")
	o, err := exec.Command("go", "build", "-o", exepath, "golang.org/x/sys/windows/svc/example").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build service program: %v\n%v", err, string(o))
	}

	s, err := m.OpenService(name)
	if err == nil {
		err = s.Delete()
		if err != nil {
			s.Close()
			t.Fatalf("Delete failed: %s", err)
		}
		s.Close()
	}
	s, err = m.CreateService(name, exepath, mgr.Config{DisplayName: "my service"}, "is", "auto-started")
	if err != nil {
		t.Fatalf("CreateService(%s) failed: %v", name, err)
	}
	defer s.Close()

	testState(t, s, svc.Stopped)
	err = s.Start("is", "manual-started")
	if err != nil {
		t.Fatalf("Start(%s) failed: %s", s.Name, err)
	}
	waitState(t, s, svc.Running)
	time.Sleep(1 * time.Second)

	// testing deadlock from issues 4.
	_, err = s.Control(svc.Interrogate)
	if err != nil {
		t.Fatalf("Control(%s) failed: %s", s.Name, err)
	}
	_, err = s.Control(svc.Interrogate)
	if err != nil {
		t.Fatalf("Control(%s) failed: %s", s.Name, err)
	}
	time.Sleep(1 * time.Second)

	_, err = s.Control(svc.Stop)
	if err != nil {
		t.Fatalf("Control(%s) failed: %s", s.Name, err)
	}
	waitState(t, s, svc.Stopped)

	err = s.Delete()
	if err != nil {
		t.Fatalf("Delete failed: %s", err)
	}
}
