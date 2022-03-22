// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import "strings"

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

func (ops ConsoleOperations) Println(args ...interface{}) {
	ops.Print(args...)
	ops.Print("\n")
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
