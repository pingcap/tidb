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
	"os"
	"os/exec" //nolint:gosec
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit/mega/register"
)

var (
	flagMegaRun  = flag.String("mega.run", "", "Run tests matching pattern (e.g., 'ddl/*', '*/GetTimeZone')")
	flagMegaList = flag.Bool("mega.list", false, "List all registered tests and exit")
	// Legacy -ut flag removed, use "mega.test run" instead
	flagMegaP      = flag.Int("mega.p", 8, "Number of parallel workers in orchestrator mode")
	flagMegaTimeout  = flag.Duration("mega.timeout", 3*time.Minute, "Per-test timeout in orchestrator mode")
)

var helpCalled = false

// init sets custom flag.Usage to print our help instead of go test's default.
func init() {
	originalUsage := flag.Usage
	flag.Usage = func() {
		// Print mega help first
		printHelpCLI()
		helpCalled = true
		// Print go test's standard help for other flags
		originalUsage()
	}
}

// printHelpCLI prints help without calling os.Exit (used by flag.Usage).
func printHelpCLI() {
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
    mega.test help
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

// RunMega runs all registered tests from various packages.
// This test serves as the entry point for monolithic testing,
// where tests from multiple packages are compiled together
// and run in a single binary to reduce link time.
func RunMega(t *testing.T) {
	// Handle -mega.list flag
	if *flagMegaList {
		listTests(t)
		return
	}

	t.Log("Mega test framework initialized")

	if *flagMegaRun != "" {
		t.Log("Running tests matching pattern:", *flagMegaRun)
		register.GlobalRegistry().RunByPattern(t, *flagMegaRun)
	} else {
		t.Log("Running all registered tests")
		register.GlobalRegistry().RunAll(t)
	}
}

// RunOrchestrator is the orchestrator mode: lists tests, then spawns itself as
// subprocesses for each test with parallelism.
func RunOrchestrator() {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot find self executable:", err)
		os.Exit(1)
	}

	// Build test list by listing ourselves with -mega.list
	tests, err := listTestsInternal(self)
	if err != nil {
		fmt.Fprintln(os.Stderr, "list tests error:", err)
		os.Exit(1)
	}

	// Filter tests by CLI args (everything after -ut)
	args := parseFilterArgs()
	if len(args) > 0 {
		tests = filterTests(tests, args)
	}

	if len(tests) == 0 {
		fmt.Println("No tests to run")
		os.Exit(0)
	}

	fmt.Printf("=== Running %d mega tests (%d parallel, %s timeout) ===\n", len(tests), *flagMegaP, *flagMegaTimeout)

	ok := runTests(self, tests)
	if !ok {
		os.Exit(1)
	}
	os.Exit(0)
}

// listTestsInternal runs the binary with -test.run TestMega -mega.list to get test list.
func listTestsInternal(binary string) ([]task, error) {
	//nolint:gosec
	cmd := exec.Command(binary, "-test.run", "^TestMega$", "-mega.list")
	cmd.Dir = workDir()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list tests: %w\noutput: %s", err, string(output))
	}
	return parseTestList(string(output))
}

// workDir returns the repository root directory.
func workDir() string {
	// Walk up from executable to find go.mod
	dir := filepath.Dir(os.Args[0])
	for range 20 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Fallback to current directory
	wd, _ := os.Getwd()
	return wd
}

// parseFilterArgs extracts filter arguments from os.Args.
// These are positional args after the flags, or explicitly passed args.
func parseFilterArgs() []string {
	args := make([]string, 0, len(os.Args))
	// Collect non-flag arguments from os.Args
	// Skip known subcommands (run, test, list, help)
	skipNext := false
	for i, arg := range os.Args {
		if skipNext {
			skipNext = false
			continue
		}
		if i == 0 {
			continue // skip program name
		}
		// Skip subcommands
		if arg == "run" || arg == "test" || arg == "list" || arg == "help" {
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue // skip flags
		}
		args = append(args, arg)
	}
	return args
}

// task represents a single test to run.
type task struct {
	pkg  string
	name string
}

func (t task) String() string {
	return t.pkg + "/" + t.name
}

// parseTestList parses the output of -mega.list.
func parseTestList(output string) ([]task, error) {
	var tasks []task
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "===") || strings.HasPrefix(trimmed, "Total") {
			continue
		}
		if strings.HasPrefix(trimmed, "Package ") {
			continue
		}
		// Parse lines like "  - parser/ast/AddQueryWatchStmtRestore"
		if len(line) >= 4 && line[:2] == "  " && line[2] == '-' {
			testPath := strings.TrimPrefix(line, "  - ")
			lastSlash := strings.LastIndex(testPath, "/")
			if lastSlash > 0 {
				tasks = append(tasks, task{
					pkg:  testPath[:lastSlash],
					name: testPath[lastSlash+1:],
				})
			}
		}
	}
	return tasks, nil
}

// filterTests filters tests based on command line arguments.
func filterTests(tests []task, args []string) []task {
	var result []task
	seen := make(map[string]bool)

	for _, arg := range args {
		if strings.Contains(arg, "/") {
			lastSlash := strings.LastIndex(arg, "/")
			pkgPattern := arg[:lastSlash]
			namePattern := arg[lastSlash+1:]
			altPkgPattern := strings.ReplaceAll(pkgPattern, "/", "_")
			altArg := strings.ReplaceAll(arg, "/", "_")

			// Check for exact package match
			exactPkg := false
			for _, t := range tests {
				if t.pkg == arg || t.pkg == altArg {
					exactPkg = true
					break
				}
			}

			// Check for exact test match
			exactTest := false
			if !exactPkg && namePattern != "*" {
				for _, t := range tests {
					if (t.pkg == pkgPattern || t.pkg == altPkgPattern) && t.name == namePattern {
						exactTest = true
						break
					}
				}
			}

			for _, t := range tests {
				key := t.String()
				if seen[key] {
					continue
				}
				if exactPkg && (t.pkg == arg || t.pkg == altArg) {
					result = append(result, t)
					seen[key] = true
					continue
				}
				if exactTest {
					if (t.pkg == pkgPattern || t.pkg == altPkgPattern) && t.name == namePattern {
						result = append(result, t)
						seen[key] = true
					}
					continue
				}
				if namePattern == "*" {
					if t.pkg == pkgPattern || t.pkg == altPkgPattern ||
						strings.Contains(t.pkg, pkgPattern) || strings.Contains(t.pkg, altPkgPattern) {
						result = append(result, t)
						seen[key] = true
					}
					continue
				}
				nameMatch := strings.Contains(t.name, namePattern)
				pkgMatch := t.pkg == pkgPattern || t.pkg == altPkgPattern ||
					strings.Contains(t.pkg, pkgPattern) || strings.Contains(t.pkg, altPkgPattern)
				if nameMatch && pkgMatch {
					result = append(result, t)
					seen[key] = true
				}
			}
		} else {
			for _, t := range tests {
				key := t.String()
				if seen[key] {
					continue
				}
				if strings.Contains(t.pkg, arg) {
					result = append(result, t)
					seen[key] = true
				}
			}
		}
	}
	return result
}

// runTests runs all tests with parallelism.
func runTests(binary string, tasks []task) bool {
	taskCh := make(chan task, 100)
	workers := make([]worker, *flagMegaP)
	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)
		go workers[i].run(&wg, binary, taskCh)
	}

	// Shuffle for better load distribution
	shuffle(tasks)

	start := time.Now()
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()

	fmt.Printf("\n=== Done: %d tests in %v ===\n", len(tasks), time.Since(start).Round(time.Second))

	var failures int
	for _, w := range workers {
		failures += w.failures
	}
	return failures == 0
}

// shuffle randomly shuffles tasks.
func shuffle(tasks []task) {
	// Simple Fisher-Yates with current time seed
	for i := len(tasks) - 1; i > 0; i-- {
		j := int(time.Now().UnixNano()) % (i + 1)
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type worker struct {
	failures int
}

type result struct {
	pkg      string
	name     string
	duration time.Duration
	err      error
	output   string
}

func (w *worker) run(wg *sync.WaitGroup, binary string, ch chan task) {
	defer wg.Done()
	for t := range ch {
		res := w.runOne(binary, t)
		if res.err != nil {
			w.failures++
			fmt.Printf("[FAIL]     %s/%s  %.2fs\n", t.pkg, t.name, res.duration.Seconds())
			if res.output != "" {
				fmt.Fprintf(os.Stderr, "--- FAIL: %s/%s ---\n%s\n--- END ---\n",
					t.pkg, t.name, filterOutput(res.output))
			}
		} else {
			fmt.Printf("[PASS]     %s/%s  %.2fs\n", t.pkg, t.name, res.duration.Seconds())
		}
	}
}

func (w *worker) runOne(binary string, t task) result {
	pattern := "^" + regexp.QuoteMeta(t.pkg+"/"+t.name) + "$"
	start := time.Now()

	var output []byte
	var err error
	timedOut := false

	for range 3 {
		//nolint:gosec
		cmd := exec.Command(binary, "-test.run", "^TestMega$", "-mega.run", pattern)
		cmd.Dir = tryDir(t.pkg)
		var buf buffer
		cmd.Stdout = &buf
		cmd.Stderr = &buf

		timer := time.AfterFunc(*flagMegaTimeout, func() {
			if cmd.Process != nil {
				cmd.Process.Kill()
				timedOut = true
			}
		})

		err = cmd.Run()
		timer.Stop()
		output = buf.Bytes()

		if err != nil {
			s := string(output)
			if strings.Contains(s, "signal: segmentation fault") ||
				strings.Contains(s, "signal: trace/breakpoint trap") ||
				strings.Contains(s, "panic during panic") {
				continue
			}
		}
		break
	}

	result := result{
		pkg:      t.pkg,
		name:     t.name,
		duration: time.Since(start),
		err:      err,
		output:   string(output),
	}

	if timedOut {
		fmt.Printf("[TIMEOUT]  %s/%s  %.2fs\n", t.pkg, t.name, result.duration.Seconds())
	}

	return result
}

// buffer is a simple thread-safe byte buffer.
type buffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *buffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out
}

// tryDir finds the package directory for a mega test.
func tryDir(pkg string) string {
	wd := workDir()
	candidates := []string{
		filepath.Join(wd, "pkg", pkg),
		filepath.Join(wd, "pkg", strings.ReplaceAll(pkg, "_", string(os.PathSeparator))),
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	return wd
}

// filterOutput removes verbose framework boilerplate from test output.
func filterOutput(output string) string {
	filtered := make([]string, 0, 128)
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "Registered tests:") {
			continue
		}
		if strings.Contains(line, "Mega test framework initialized") ||
			strings.Contains(line, "Running tests matching pattern") {
			continue
		}
		filtered = append(filtered, line)
	}
	if len(filtered) > 80 {
		filtered = filtered[len(filtered)-80:]
	}
	return strings.Join(filtered, "\n")
}

// listTests prints all registered tests in a formatted way.
func listTests(t *testing.T) {
	fmt.Println("=== Mega Test Registry ===")
	allTests := register.GlobalRegistry().ListAll()
	fmt.Printf("Total tests: %d\n", len(allTests))
	fmt.Println()

	testsByPkg := make(map[string][]string)
	for _, testFullName := range allTests {
		lastSlashIndex := strings.LastIndex(testFullName, "/")
		if lastSlashIndex > 0 {
			pkg := testFullName[:lastSlashIndex]
			name := testFullName[lastSlashIndex+1:]
			testsByPkg[pkg] = append(testsByPkg[pkg], name)
		}
	}

	packages := make([]string, 0, len(testsByPkg))
	for pkg := range testsByPkg {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	for _, pkg := range packages {
		tests := testsByPkg[pkg]
		sort.Strings(tests)
		fmt.Printf("Package %s (%d tests):\n", pkg, len(tests))
		for _, test := range tests {
			fmt.Printf("  - %s/%s\n", pkg, test)
		}
		fmt.Println()
	}
	os.Exit(0)
}
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