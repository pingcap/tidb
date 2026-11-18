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

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tidb/pkg/util"
	"golang.org/x/term"
)

func readPassword(tty *os.File) (password string, err error) {
	fd := int(tty.Fd())
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("--ask-password requires a terminal")
	}
	state, err := term.GetState(fd)
	if err != nil {
		return "", fmt.Errorf("save password terminal: %w", err)
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(interrupts)
	done := make(chan struct{})
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		select {
		case <-interrupts:
			// ReadPassword cannot cancel its blocking read. Restore the terminal
			// and exit instead of returning with a reader still running.
			if err := term.Restore(fd, state); err != nil {
				fmt.Fprintf(os.Stderr, "restore password terminal: %v\n", err)
			}
			_, _ = fmt.Fprintln(tty)
			fmt.Fprintln(os.Stderr, "password prompt interrupted")
			os.Exit(1)
		case <-done:
		}
	})
	defer func() {
		close(done)
		wg.Wait()
		if restoreErr := term.Restore(fd, state); restoreErr != nil {
			password = ""
			err = fmt.Errorf("restore password terminal: %w", restoreErr)
		}
		_, _ = fmt.Fprintln(tty)
	}()
	if _, err := fmt.Fprint(tty, "Enter password: "); err != nil {
		return "", fmt.Errorf("write password prompt: %w", err)
	}
	input, err := term.ReadPassword(fd)
	defer clear(input)
	if err != nil {
		return "", fmt.Errorf("read password from terminal: %w", err)
	}
	return string(input), nil
}
