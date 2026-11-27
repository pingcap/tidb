//go:build linux || darwin

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
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestReadPasswordTerminal(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    string
		password string
		signal   os.Signal
		wantErr  bool
		wantExit bool
	}{
		{name: "spaces tab and unicode", input: " secret\t密码 \r", password: " secret\t密码 "},
		{name: "empty", input: "\r"},
		{name: "backspace", input: "ab\x7fz\r", password: "az"},
		{name: "clear line", input: "secret\x15replacement\r", password: "replacement"},
		{name: "Ctrl-C", input: "secret\x03", wantErr: true, wantExit: true},
		{name: "Ctrl-D before Enter", input: "\x04\r"},
		{name: "Ctrl-D after text", input: "secret\x04\r", password: "secret"},
		{name: "SIGINT", signal: syscall.SIGINT, wantErr: true, wantExit: true},
		{name: "SIGTERM", signal: syscall.SIGTERM, wantErr: true, wantExit: true},
		{name: "SIGHUP", signal: syscall.SIGHUP, wantErr: true, wantExit: true},
		{name: "SIGQUIT", signal: syscall.SIGQUIT, wantErr: true, wantExit: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			master, tty := openPasswordPTY(t)
			state := passwordTerminalState(t, master)
			cmd := exec.Command(os.Args[0], "-test.run=^TestReadPasswordTerminalProcess$")
			cmd.Env = append(os.Environ(),
				// The helper must run even when its parent is filtered or sharded by Bazel.
				"TEST_TOTAL_SHARDS=1", "TEST_SHARD_INDEX=0", "TESTBRIDGE_TEST_ONLY=", "GO_TEST_WRAP=0",
				"DUMPLING_PASSWORD_TEST_CHILD=1",
				"DUMPLING_PASSWORD_TEST_EXPECTED="+testCase.password,
				fmt.Sprintf("DUMPLING_PASSWORD_TEST_ERROR=%t", testCase.wantErr))
			cmd.ExtraFiles = []*os.File{tty}
			// Canonical terminal input delivers Ctrl-C to the foreground process group.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 3}
			cmd.Stdin = strings.NewReader("stdin-marker")
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			require.NoError(t, cmd.Start())
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })

			prompt := readPasswordPTY(t, master, len("Enter password: "))
			require.Equal(t, "Enter password: ", string(prompt))
			require.Eventually(t, func() bool {
				return passwordTerminalState(t, master).Lflag&unix.ECHO == 0
			}, 10*time.Second, time.Millisecond, "password input must disable echo")
			if testCase.signal != nil {
				require.NoError(t, cmd.Process.Signal(testCase.signal))
			} else {
				written := make(chan error, 1)
				go func() {
					_, err := master.WriteString(testCase.input)
					written <- err
				}()
				select {
				case err := <-written:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("timed out writing password terminal")
				}
			}
			// A controlling terminal can wait for output to drain while its session exits.
			newline := readPasswordPTY(t, master, 2)
			require.Equal(t, "\r\n", string(newline), "input must not be echoed")
			select {
			case err := <-done:
				if testCase.wantExit {
					require.Error(t, err, "%s", output.String())
					require.Equal(t, 1, cmd.ProcessState.ExitCode(), "%s", output.String())
					require.Contains(t, output.String(), "password prompt interrupted")
				} else {
					require.NoError(t, err, "%s", output.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("password prompt did not finish")
			}
			restored := passwordTerminalState(t, master)
			require.Equal(t, state, restored)
		})
	}
}

func TestPasswordPromptBeforeVersion(t *testing.T) {
	master, tty := openPasswordPTY(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestReadPasswordTerminalProcess$")
	cmd.Env = append(os.Environ(),
		"TEST_TOTAL_SHARDS=1", "TEST_SHARD_INDEX=0", "TESTBRIDGE_TEST_ONLY=", "GO_TEST_WRAP=0",
		"DUMPLING_PASSWORD_TEST_CHILD=1", "DUMPLING_PASSWORD_TEST_CLI=1")
	cmd.ExtraFiles = []*os.File{tty}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 3}
	cmd.Stdout, cmd.Stderr = tty, tty
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	prompt := readPasswordPTY(t, master, len("Enter password: "))
	require.Equal(t, "Enter password: ", string(prompt), "password prompt must precede the version banner")
	require.Eventually(t, func() bool {
		return passwordTerminalState(t, master).Lflag&unix.ECHO == 0
	}, 10*time.Second, time.Millisecond)
	_, err := master.WriteString("\r")
	require.NoError(t, err)
	require.Equal(t, "\r\n", string(readPasswordPTY(t, master, 2)))
	require.Equal(t, "Release version: ", string(readPasswordPTY(t, master, len("Release version: "))))
	go func() { _, _ = io.Copy(io.Discard, master) }()
	select {
	case err := <-done:
		// The CLI exits after failing to connect to the deliberately unused port.
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("dumpling did not finish")
	}
}

func readPasswordPTY(t *testing.T, master *os.File, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(master, data)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
		return data
	case <-time.After(10 * time.Second):
		t.Fatal("timed out reading password terminal")
		return nil
	}
}

func TestReadPasswordTerminalProcess(t *testing.T) {
	if os.Getenv("DUMPLING_PASSWORD_TEST_CHILD") != "1" {
		return
	}
	if os.Getenv("DUMPLING_PASSWORD_TEST_CLI") == "1" {
		os.Args = []string{os.Args[0], "--ask-password", "--host=127.0.0.1", "--port=1"}
		main()
		return
	}
	tty := os.NewFile(3, "password-tty")
	defer tty.Close()
	password, err := readPassword(tty)
	if os.Getenv("DUMPLING_PASSWORD_TEST_ERROR") == "true" {
		require.Error(t, err)
		require.Empty(t, password)
	} else {
		require.NoError(t, err)
		require.Equal(t, os.Getenv("DUMPLING_PASSWORD_TEST_EXPECTED"), password)
	}
	input, err := io.ReadAll(os.Stdin)
	require.NoError(t, err)
	require.Equal(t, "stdin-marker", string(input), "password prompt must not consume stdin")
}

func passwordTerminalState(t *testing.T, tty *os.File) *unix.Termios {
	t.Helper()
	state, err := unix.IoctlGetTermios(int(tty.Fd()), passwordTermiosRequest)
	require.NoError(t, err)
	// BSD may set PENDIN when leaving raw mode to reprocess pending input.
	state.Lflag &^= unix.PENDIN
	return state
}
