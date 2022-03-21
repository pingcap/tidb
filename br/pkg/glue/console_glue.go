// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import "strings"

// ConsoleOperations are some operations based on ConsoleGlue.
type ConsoleOperations struct {
	ConsoleGlue
}

// PromptBool prompts a boolean from the user.
func (ops ConsoleOperations) PromptBool(p string) bool {
	if !ops.SupportsScan() {
		return true
	}
	for {
		ans := ""
		ops.Print(p + "(y/N) ")
		ops.Scanln(&ans)
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

	// SupportsScan checks whether the shell supports input.
	SupportsScan() bool
	Scanln(args ...interface{}) (int, error)
}

type NoOPConsoleGlue struct{}

func (NoOPConsoleGlue) Print(args ...interface{}) {}

func (NoOPConsoleGlue) SupportsScan() bool {
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
