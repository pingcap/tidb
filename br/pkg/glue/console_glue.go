// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/fatih/color"
)

// ConsoleOperations are some operations based on ConsoleGlue.
type ConsoleOperations struct {
	ConsoleGlue
}

// PromptBool prompts a boolean from the user.
func (ops ConsoleOperations) PromptBool(p string) bool {
	if !ops.IsInteractive() {
		return true
	}
	for {
		ans := ""
		ops.Print(p + "(y/N) ")
		if n, err := ops.Scanln(&ans); err != nil || n == 0 {
			// EOF or reply nothing.
			return false
		}
		trimed := strings.TrimSpace(ans)
		if strings.ToLower(trimed) == "y" {
			return true
		}
		if trimed == "" || strings.ToLower(trimed) == "n" {
			return false
		}
	}
}

func (ops *ConsoleOperations) CreateTable() *Table {
	return &Table{
		console: ops,
	}
}

func (ops ConsoleOperations) Print(args ...interface{}) {
	fmt.Fprint(ops, args...)
}

func (ops ConsoleOperations) Println(args ...interface{}) {
	fmt.Fprintln(ops, args...)
}

func (ops ConsoleOperations) Printf(format string, args ...interface{}) {
	fmt.Fprintf(ops, format, args...)
}

type Table struct {
	console *ConsoleOperations
	items   [][2]string
}

func (t *Table) Add(key, value string) {
	t.items = append(t.items, [...]string{key, value})
}

func (t *Table) maxKeyLen() int {
	maxLen := 0
	for _, item := range t.items {
		if len(item[0]) > maxLen {
			maxLen = len(item[0])
		}
	}
	return maxLen
}

func (t *Table) Print() {
	value := color.New(color.Bold)
	maxLen := t.maxKeyLen()
	for _, item := range t.items {
		t.console.Println(fmt.Sprintf("%*s: %s", maxLen, item[0], value.Sprint(item[1])))
	}
}

// ConsoleGlue is the glue between BR and some type of console,
// which is the port for interact with the user.
type ConsoleGlue interface {
	io.Writer

	// IsInteractive checks whether the shell supports input.
	IsInteractive() bool
	Scanln(args ...interface{}) (int, error)
}

type NoOPConsoleGlue struct{}

func (NoOPConsoleGlue) Write([]byte) (int, error) {
	return 0, nil
}

func (NoOPConsoleGlue) IsInteractive() bool {
	return false
}

func (NoOPConsoleGlue) Scanln(args ...interface{}) (int, error) {
	return 0, nil
}

func GetConsole(g Glue) ConsoleOperations {
	if cg, ok := g.(ConsoleGlue); ok {
		return ConsoleOperations{ConsoleGlue: cg}
	}
	return ConsoleOperations{ConsoleGlue: NoOPConsoleGlue{}}
}

type StdIOGlue struct{}

func (s StdIOGlue) Write(p []byte) (n int, err error) {
	return os.Stdout.Write(p)
}

// IsInteractive checks whether the shell supports input.
func (s StdIOGlue) IsInteractive() bool {
	// should we detach whether we are in a interactive tty here?
	return true
}

func (s StdIOGlue) Scanln(args ...interface{}) (int, error) {
	return fmt.Scanln(args...)
}
