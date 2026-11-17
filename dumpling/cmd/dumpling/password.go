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
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"unicode/utf8"

	"golang.org/x/term"
)

func readPassword(tty *os.File) (password string, err error) {
	fd := int(tty.Fd())
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("--ask-password requires a terminal")
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(interrupts)
	state, err := term.MakeRaw(fd)
	if err != nil {
		return "", fmt.Errorf("prepare password terminal: %w", err)
	}
	defer func() {
		if restoreErr := term.Restore(fd, state); restoreErr != nil {
			password = ""
			err = fmt.Errorf("restore password terminal: %w", restoreErr)
		}
	}()
	defer func() { _, _ = fmt.Fprint(tty, "\r\n") }()
	if _, err := fmt.Fprint(tty, "Enter password: "); err != nil {
		return "", fmt.Errorf("write password prompt: %w", err)
	}

	type passwordResult struct {
		password string
		err      error
	}
	result := make(chan passwordResult, 1)
	// Only this goroutine's caller changes terminal modes, so cancellation can
	// restore them even while input is blocked. The CLI closes tty on return.
	go func() {
		password, err := readPasswordLine(tty)
		result <- passwordResult{password: password, err: err}
	}()
	select {
	case res := <-result:
		if res.err != nil {
			return "", fmt.Errorf("read password from terminal: %w", res.err)
		}
		return res.password, nil
	case <-interrupts:
		return "", fmt.Errorf("password prompt interrupted")
	}
}

// readPasswordLine retains password bytes without a line editor's character
// filtering or length limit. In raw mode it also handles Enter and cancellation.
func readPasswordLine(input io.Reader) (string, error) {
	reader := bufio.NewReader(input)
	var password []byte
	defer func() { clear(password) }()
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		switch b {
		case '\r', '\n':
			return string(password), nil
		case '\x03':
			return "", fmt.Errorf("password prompt interrupted")
		case '\x04':
			return "", io.EOF
		case '\b', '\x7f':
			_, size := utf8.DecodeLastRune(password)
			clear(password[len(password)-size:])
			password = password[:len(password)-size]
		case '\x15':
			clear(password)
			password = password[:0]
		default:
			password = append(password, b)
		}
	}
}
