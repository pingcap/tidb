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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/pkdbcredential"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

const (
	invalidArgumentsMessage      = "invalid arguments; use 'tidb-credential --help'"
	passwordPrompt               = "Enter database password: "
	passwordConfirmationPrompt   = "Confirm database password: "
	encryptedFileCreatedTemplate = "encrypted password file created: %s\n"
)

var (
	errKeyFileRequired        = errors.New("--key-file is required and must not be empty")
	errOutputRequired         = errors.New("--output is required and must not be empty")
	errPositionalArguments    = errors.New("positional arguments are not accepted")
	errOutputPathMustNameFile = errors.New("output path must name a file")
)

type safeCommandError struct {
	err error
}

func (err *safeCommandError) Error() string {
	return err.err.Error()
}

func (err *safeCommandError) Unwrap() error {
	return err.err
}

func markSafe(err error) error {
	return &safeCommandError{err: err}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	command := newRootCommand()
	command.SetArgs(args)
	command.SetIn(stdin)
	command.SetOut(stdout)
	command.SetErr(stderr)

	if err := command.Execute(); err != nil {
		var safeErr *safeCommandError
		if errors.As(err, &safeErr) {
			_, _ = fmt.Fprintln(stderr, safeErr.err)
		} else {
			_, _ = fmt.Fprintln(stderr, invalidArgumentsMessage)
		}
		return 1
	}
	return 0
}

func newRootCommand() *cobra.Command {
	command := &cobra.Command{
		Use:                "tidb-credential",
		Short:              "Create TiDB encrypted credential files",
		Version:            mysql.TiDBReleaseVersion,
		SilenceErrors:      true,
		SilenceUsage:       true,
		DisableSuggestions: true,
		Args:               cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	command.CompletionOptions.DisableDefaultCmd = true
	command.AddCommand(newEncryptCommand())
	return command
}

func newEncryptCommand() *cobra.Command {
	var keyFile, outputFile string
	command := &cobra.Command{
		Use:   "encrypt --key-file <path> --output <path>",
		Short: "Create an Encrypted Password File",
		Args:  rejectPositionalArguments,
		RunE: func(command *cobra.Command, _ []string) error {
			return encrypt(command, keyFile, outputFile)
		},
	}
	command.Flags().StringVar(&keyFile, "key-file", "", "Password Key File path")
	command.Flags().StringVar(&outputFile, "output", "", "new Encrypted Password File path")
	return command
}

func rejectPositionalArguments(_ *cobra.Command, args []string) error {
	if len(args) != 0 {
		return markSafe(errPositionalArguments)
	}
	return nil
}

func encrypt(command *cobra.Command, keyFile, outputFile string) error {
	if keyFile == "" {
		return markSafe(errKeyFileRequired)
	}
	if outputFile == "" {
		return markSafe(errOutputRequired)
	}
	if strings.HasSuffix(outputFile, "/") && filepath.Clean(outputFile) != "/" {
		return markSafe(pkdbcredential.ErrOutputWrite.Wrap(errOutputPathMustNameFile).GenWithStackByArgs(outputFile))
	}

	absoluteOutput, err := filepath.Abs(outputFile)
	if err != nil {
		return markSafe(pkdbcredential.ErrOutputWrite.Wrap(err).GenWithStackByArgs(outputFile))
	}
	password := passwordReader(command.InOrStdin(), command.ErrOrStderr())
	if err := pkdbcredential.CreateEncryptedPasswordFile(password, keyFile, outputFile); err != nil {
		return markSafe(err)
	}

	_, _ = fmt.Fprintf(command.OutOrStdout(), encryptedFileCreatedTemplate, absoluteOutput)
	return nil
}

type fileDescriptorReader interface {
	io.Reader
	Fd() uintptr
}

func passwordReader(input io.Reader, promptOutput io.Writer) io.Reader {
	terminalInput, ok := input.(fileDescriptorReader)
	if !ok || !term.IsTerminal(int(terminalInput.Fd())) {
		return input
	}
	return &confirmingPasswordReader{
		fileDescriptor: int(terminalInput.Fd()),
		promptOutput:   promptOutput,
		readPassword:   term.ReadPassword,
	}
}

type confirmingPasswordReader struct {
	fileDescriptor int
	promptOutput   io.Writer
	readPassword   func(int) ([]byte, error)
	password       *bytes.Reader
	err            error
	initialized    bool
}

func (reader *confirmingPasswordReader) Read(buffer []byte) (int, error) {
	if !reader.initialized {
		reader.initialized = true
		password, err := reader.readConfirmedPassword()
		reader.err = err
		if err == nil {
			reader.password = bytes.NewReader(password)
		}
	}
	if reader.err != nil {
		return 0, reader.err
	}
	return reader.password.Read(buffer)
}

func (reader *confirmingPasswordReader) readConfirmedPassword() ([]byte, error) {
	password, err := reader.readPasswordEntry(passwordPrompt)
	if err != nil {
		return nil, err
	}
	confirmation, err := reader.readPasswordEntry(passwordConfirmationPrompt)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(password, confirmation) {
		return nil, pkdbcredential.ErrPasswordMismatch.GenWithStackByArgs()
	}
	return password, nil
}

func (reader *confirmingPasswordReader) readPasswordEntry(prompt string) ([]byte, error) {
	if err := writeTerminalText(reader.promptOutput, prompt); err != nil {
		return nil, pkdbcredential.ErrPasswordInput.GenWithStackByArgs()
	}
	password, readErr := reader.readPassword(reader.fileDescriptor)
	newlineErr := writeTerminalText(reader.promptOutput, "\n")
	if readErr != nil || newlineErr != nil {
		return nil, pkdbcredential.ErrPasswordInput.GenWithStackByArgs()
	}
	return password, nil
}

func writeTerminalText(output io.Writer, text string) error {
	written, err := io.WriteString(output, text)
	if err == nil && written != len(text) {
		err = io.ErrShortWrite
	}
	return err
}
