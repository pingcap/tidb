// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"fmt"
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

func (ops ConsoleOperations) Println(args ...interface{}) {
	ops.Print(args...)
	ops.Print("\n")
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
	Print(args ...interface{})

	// IsInteractive checks whether the shell supports input.
	IsInteractive() bool
	Scanln(args ...interface{}) (int, error)
}

type NoOPConsoleGlue struct{}

func (NoOPConsoleGlue) Print(args ...interface{}) {}

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
