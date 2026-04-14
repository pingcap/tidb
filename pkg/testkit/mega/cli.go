// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mega

import (
	"flag"
	"fmt"
	"strings"
	"testing"
)
// globToRegex converts a glob pattern to a regex pattern.
// "*" matches any sequence of characters (like in glob).
func globToRegex(pattern string) string {
	// Escape regex special chars except *
	result := strings.Builder{}
	for _, ch := range pattern {
		switch ch {
		case '*':
			result.WriteString(".*")
		default:
			// Escape regex special chars
			if isRegexSpecial(ch) {
				result.WriteByte('\\')
			}
			result.WriteRune(ch)
		}
	}
	return result.String()
}

func isRegexSpecial(ch rune) bool {
	return ch == '.' || ch == '+' || ch == '?' || ch == '|' ||
		ch == '(' || ch == ')' || ch == '[' || ch == ']' ||
		ch == '{' || ch == '}' || ch == '^' || ch == '$' || ch == '\\'
}



// Subcommand constants.
const (
	cmdHelp = "help"
	cmdList = "list"
	cmdRun  = "run"
	cmdTest = "test"
)

// knownSubcommands is the set of recognized subcommands.
var knownSubcommands = map[string]bool{
	cmdHelp: true,
	cmdList: true,
	cmdRun:  true,
	cmdTest: true,
	"-h":    true,
	"--help": true,
}

// HandleCLI is called from TestMain before m.Run().
//
// It intercepts subcommands (help, list, run) and returns whether the caller
// should proceed with m.Run().  When it returns false, the caller should
// os.Exit with the returned exit code.
func HandleCLI() (shouldRunTests bool, exitCode int) {
	// Parse flags first to access -mega.list and -mega.run (used internally by subprocess)
	flag.Parse()

	// Check for subcommand (first positional arg after flags)
	args := flag.Args()
	if len(args) == 0 {
		// No subcommand — proceed with normal go test execution
		return true, 0
	}

	subcmd := args[0]

	// Handle subcommands
	switch subcmd {
	case cmdHelp, "-h", "--help":
		printHelp()
		return false, 0

	case cmdList:
		// List all registered tests
		t := &testing.T{}
		listTests(t)
		if t.Failed() {
			return false, 1
		}
		return false, 0

	case cmdRun:
		// Orchestrator mode or direct run depending on remaining args
		runArgs := args[1:]
		if len(runArgs) > 0 {
			// Run specific tests - set internal flag and proceed to m.Run()
			// Convert glob pattern to regex
			pattern := globToRegex(runArgs[0])
			if !strings.Contains(pattern, "/") {
				pattern = ".*/" + pattern
			}
			*flagMegaRun = pattern
			// Proceed to normal test execution
			return true, 0
		} else {
			// Orchestrator mode - spawn subprocesses
			RunOrchestrator()
			return false, 1 // RunOrchestrator calls os.Exit
		}
		return false, 0

	case cmdTest:
		// Alias for 'run'
		runArgs := args[1:]
		if len(runArgs) > 0 {
			pattern := globToRegex(runArgs[0])
			if !strings.Contains(pattern, "/") {
				pattern = ".*/" + pattern
			}
			*flagMegaRun = pattern
			return true, 0
		}
		// No pattern provided - run all tests
		return true, 0

	default:
		// Unknown subcommand - assume it's a test pattern
		pattern := globToRegex(subcmd)
		if !strings.Contains(pattern, "/") {
			pattern = ".*/" + pattern
		}
		*flagMegaRun = pattern
		return true, 0
	}
}

// printHelp prints usage information.
func printHelp() {
	fmt.Printf(`Mega test framework - self-contained monolithic test binary

USAGE:
    mega.test [SUBCOMMAND] [OPTIONS]
    mega.test help
    mega.test list
    mega.test run [PATTERN] [OPTIONS]
    mega.test run [OPTIONS]

SUBCOMMANDS:
    help        Print this help message
    list        List all registered tests
    run         Run tests (with optional pattern filter)

    When 'run' is called without a pattern, it operates in orchestrator mode:
    - Lists all registered tests
    - Spawns subprocesses for each test with parallelism
    - Each subprocess runs a single test in an isolated environment
    - Useful for CI and full test suite execution

    When 'run' is called with a pattern (e.g., 'ddl/*', '*/GetTimeZone'):
    - Runs matching tests in current process
    - Useful for debugging individual tests

PATTERNS:
    ddl/              All tests in ddl package
    */GetTimeZone      All tests named GetTimeZone in any package
    executor/Inspe*   Tests in executor package with names matching Inspe*

OPTIONS (for orchestrator mode):
    -mega.p N           Number of parallel workers (default: 8)
    -mega.timeout D      Per-test timeout (default: 3m)

EXAMPLES:
    mega.test list
    mega.test run
    mega.test run ddl/*
    mega.test run */GetTimeZone
    mega.test run executor/InspectionResult -mega.p 16

INTERNAL FLAGS (used by orchestrator subprocesses):
    -test.run          Internal: go test filter
    -mega.run          Internal: exact test pattern
    -mega.list         Internal: list tests mode
`)
}