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

package mlflow

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
)

const (
	defaultPython        = "python3"
	defaultWorkers       = 2
	defaultTimeout       = 2 * time.Second
	defaultCacheEntries  = 64
	defaultStartupWindow = 5 * time.Second
)

// Config defines the MLflow sidecar settings.
type Config struct {
	Python       string
	Workers      int
	Timeout      time.Duration
	CacheEntries int
}

// DefaultConfig returns the default sidecar configuration.
func DefaultConfig() Config {
	return Config{
		Python:       defaultPython,
		Workers:      defaultWorkers,
		Timeout:      defaultTimeout,
		CacheEntries: defaultCacheEntries,
	}
}

// SidecarPool manages a pool of MLflow sidecar workers.
type SidecarPool struct {
	cfg       Config
	mu        sync.Mutex
	started   bool
	socketDir string
	sockets   []string
	cmds      []*exec.Cmd
	nextIndex int
}

//go:embed sidecar.py
var sidecarScript []byte

// NewSidecarPool constructs a new pool with the provided config.
func NewSidecarPool(cfg Config) *SidecarPool {
	return &SidecarPool{cfg: normalizeConfig(cfg)}
}

// Dial connects to a running sidecar worker.
func (p *SidecarPool) Dial(ctx context.Context) (net.Conn, error) {
	if err := p.ensureStarted(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	idx := p.nextIndex % len(p.sockets)
	p.nextIndex++
	socket := p.sockets[idx]
	timeout := p.cfg.Timeout
	p.mu.Unlock()

	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, "unix", socket)
}

func (p *SidecarPool) ensureStarted(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil
	}
	cfg := p.cfg

	if cfg.Workers <= 0 {
		return errors.New("mlflow sidecar workers must be positive")
	}
	socketDir, err := os.MkdirTemp("", "tidb-mlflow-")
	if err != nil {
		return errors.Trace(err)
	}
	scriptPath, err := writeSidecarScript(socketDir)
	if err != nil {
		_ = os.RemoveAll(socketDir)
		return err
	}
	sockets := make([]string, 0, cfg.Workers)
	cmds := make([]*exec.Cmd, 0, cfg.Workers)
	cleanup := func() {
		for _, cmd := range cmds {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
		_ = os.RemoveAll(socketDir)
	}
	for i := range cfg.Workers {
		socketPath := filepath.Join(socketDir, fmt.Sprintf("mlflow-%d.sock", i))
		//nolint:gosec // cfg.Python is configured by operators, not user input.
		cmd := exec.CommandContext(context.Background(), cfg.Python, scriptPath,
			"--socket", socketPath,
			"--cache-entries", fmt.Sprintf("%d", cfg.CacheEntries),
		)
		cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			cleanup()
			return errors.Annotate(err, "start mlflow sidecar")
		}
		if err := waitForSocket(ctx, socketPath); err != nil {
			cleanup()
			return err
		}
		sockets = append(sockets, socketPath)
		cmds = append(cmds, cmd)
	}

	p.started = true
	p.socketDir = socketDir
	p.sockets = sockets
	p.cmds = cmds
	return nil
}

func normalizeConfig(cfg Config) Config {
	if cfg.Python == "" {
		cfg.Python = defaultPython
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.CacheEntries <= 0 {
		cfg.CacheEntries = defaultCacheEntries
	}
	return cfg
}

func writeSidecarScript(dir string) (string, error) {
	path := filepath.Join(dir, "sidecar.py")
	if err := os.WriteFile(path, sidecarScript, 0o700); err != nil {
		return "", errors.Trace(err)
	}
	return path, nil
}

func waitForSocket(ctx context.Context, socketPath string) error {
	deadline := time.Now().Add(defaultStartupWindow)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("mlflow sidecar did not start")
		}
		conn, err := net.DialTimeout("unix", socketPath, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Close terminates workers and cleans up temporary socket/script files.
func (p *SidecarPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return nil
	}
	for _, cmd := range p.cmds {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	if p.socketDir != "" {
		_ = os.RemoveAll(p.socketDir)
	}
	p.started = false
	p.socketDir = ""
	p.sockets = nil
	p.cmds = nil
	p.nextIndex = 0
	return nil
}
