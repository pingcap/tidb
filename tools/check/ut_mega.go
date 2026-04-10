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
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// mega mode support for running tests through the monolithic test framework

var (
	megaMode      bool
	megaBinary    string
	megaTestCount int
)

// cmdMegaRun runs tests using the mega test framework.
// Only runs the mega binary — build is handled by Makefile (bazel build).
func cmdMegaRun(args ...string) bool {
	// Set mega binary path (built by Makefile's bazel build step)
	workDir, _ := os.Getwd()
	megaBinary = filepath.Join(workDir, "bazel-bin/pkg/testkit/mega/mega_test_/mega_test")

	if _, err := os.Stat(megaBinary); err != nil {
		log.Fatalf("mega test binary not found at %s — run 'make ut-mega' which handles building", megaBinary)
	}

	// Get the list of all available tests from mega
	tests, err := listMegaTests()
	if err != nil {
		log.Println("list mega tests error", err)
		return false
	}

	// Filter tests based on arguments
	tasks := filterMegaTests(tests, args)

	// Apply --except and --only filters
	if except != "" {
		list, err := parseCaseListFromFile(except)
		if err != nil {
			log.Println("parse --except file error", err)
			return false
		}
		tmp := tasks[:0]
		for _, task := range tasks {
			if _, ok := list[task.String()]; !ok {
				tmp = append(tmp, task)
			}
		}
		tasks = tmp
	}

	if only != "" {
		list, err := parseCaseListFromFile(only)
		if err != nil {
			log.Println("parse --only file error", err)
			return false
		}
		tmp := tasks[:0]
		for _, task := range tasks {
			if _, ok := list[task.String()]; ok {
				tmp = append(tmp, task)
			}
		}
		tasks = tmp
	}

	fmt.Printf("=== Running %d mega tests (%d parallel, 3min timeout) ===\n", len(tasks), p)

	if len(tasks) == 0 {
		fmt.Println("No mega tests to run")
		return true
	}

	return runMegaTests(tasks)
}

// buildMegaTestBinary compiles the mega test binary
func buildMegaTestBinary() error {
	start := time.Now()

	// Bazel needs failpoint-disabled source code to compile.
	// Save current failpoint state and disable before building.
	fpWasEnabled := isFailpointEnabled()
	if fpWasEnabled {
		fmt.Println("Temporarily disabling failpoints for bazel build...")
		cmd := exec.Command("tools/bin/failpoint-ctl", "disable")
		cmd.Dir = workDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return withTrace(fmt.Errorf("disable failpoints for bazel: %w", err))
		}
	}

	megaBinary = filepath.Join(workDir, "mega.test")

	// Build the mega test binary using bazel
	cmd := exec.Command("bazel", "build", "//pkg/testkit/mega:mega_test")
	cmd.Dir = workDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return withTrace(fmt.Errorf("build mega test binary with bazel: %w", err))
	}

	// Copy the bazel-built binary to the expected location
	bazelBinary := filepath.Join(workDir, "bazel-bin", "pkg", "testkit", "mega", "mega_test_", "mega_test")
	src, err := os.Open(bazelBinary)
	if err != nil {
		return withTrace(fmt.Errorf("open bazel mega test binary: %w", err))
	}
	defer src.Close()

	dst, err := os.Create(megaBinary)
	if err != nil {
		return withTrace(fmt.Errorf("create mega test binary: %w", err))
	}
	defer dst.Close()

	if _, err := dst.ReadFrom(src); err != nil {
		return withTrace(fmt.Errorf("copy mega test binary: %w", err))
	}
	dst.Chmod(0o755)

	// Re-enable failpoints if they were enabled before
	if fpWasEnabled {
		fmt.Println("Re-enabling failpoints...")
		enableCmd := exec.Command("tools/bin/failpoint-ctl", "enable")
		enableCmd.Dir = workDir
		enableCmd.Stdout = os.Stdout
		enableCmd.Stderr = os.Stderr
		if err := enableCmd.Run(); err != nil {
			return withTrace(fmt.Errorf("re-enable failpoints: %w", err))
		}
	}

	fmt.Printf("built mega test binary in %v\n", time.Since(start))
	return nil
}

// isFailpointEnabled checks if failpoints are currently enabled by looking for
// failpoint binding files in a known package.
func isFailpointEnabled() bool {
	// Check a known package for failpoint binding files
	pattern := filepath.Join(workDir, "pkg", "util", "replayer", "binding__failpoint_binding__*.go")
	matches, _ := filepath.Glob(pattern)
	return len(matches) > 0
}

// listMegaTests gets all available tests from the mega framework
func listMegaTests() ([]megaTask, error) {
	// Use the mega test binary directly to list tests
	if megaBinary == "" {
		megaBinary = filepath.Join(workDir, "mega.test")
	}
	cmd := exec.Command(megaBinary, "-test.run", "^TestMega$", "-mega.list")
	cmd.Dir = workDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, withTrace(fmt.Errorf("list mega tests: %w\noutput: %s", err, string(output)))
	}

	return parseMegaTestList(string(output))
}

// megaTask represents a test to run in mega framework
type megaTask struct {
	pkg  string
	name string
}

func (t megaTask) String() string {
	return t.pkg + " " + t.name
}

// parseMegaTestList parses the output of `mega -list`
func parseMegaTestList(output string) ([]megaTask, error) {
	var tasks []megaTask
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		// Don't trim the entire line - we need to preserve leading spaces for parsing
		trimmed := strings.TrimSpace(line)

		if trimmed == "" || strings.HasPrefix(trimmed, "===") || strings.HasPrefix(trimmed, "Total") {
			continue
		}

		// Skip package header lines like "Package bindinfo (43 tests):"
		if strings.HasPrefix(trimmed, "Package ") {
			continue
		}

		// Parse lines like "  - bindinfo/BestPlanInBaselines" (note: two spaces before the dash)
		if len(line) >= 4 && line[0:2] == "  " && line[2] == '-' {
			// Remove the leading "  - " prefix
			testPath := strings.TrimPrefix(line, "  - ")
			parts := strings.SplitN(testPath, "/", 2)
			if len(parts) == 2 {
				// The mega CLI displays package names with underscores (planner_indexadvisor)
				// but the actual registered names use slashes (planner/indexadvisor)
				// Convert underscores back to slashes for proper lookup
				pkg := strings.ReplaceAll(parts[0], "_", "/")
				tasks = append(tasks, megaTask{pkg: pkg, name: parts[1]})
			}
		}
	}

	return tasks, nil
}

// filterMegaTests filters tests based on command line arguments
func filterMegaTests(tests []megaTask, args []string) []megaTask {
	if len(args) == 0 {
		return tests
	}

	// Handle multiple args - each arg can be "pkg/name", "pkg/subpkg/name", "pkg/*", or "pkg"
	var result []megaTask
	seen := make(map[string]bool)

	for _, arg := range args {
		// Check if the arg is in "pkg/name" format
		if strings.Contains(arg, "/") {
			// Find the last slash to handle hierarchical package names like "planner/indexadvisor/TestName"
			lastSlashIndex := strings.LastIndex(arg, "/")
			if lastSlashIndex > 0 {
				pkgPattern := arg[:lastSlashIndex]
				namePattern := arg[lastSlashIndex+1:]

				// Create alternative pattern with underscores (e.g., "planner/core" -> "planner_core")
				// to handle mixed format package names
				altPkgPattern := strings.ReplaceAll(pkgPattern, "/", "_")
				altArg := strings.ReplaceAll(arg, "/", "_")

				// First, try to find exact package matches (arg is a package name)
				// e.g., "planner/memo" should match all tests in the planner_memo package
				exactPkgMatches := false
				for _, t := range tests {
					if t.pkg == arg || t.pkg == altArg {
						exactPkgMatches = true
						break
					}
				}

				// Check for exact test matches (pkg/name format)
				hasExactTestMatch := false
				if !exactPkgMatches && namePattern != "*" {
					for _, t := range tests {
						if (t.pkg == pkgPattern || t.pkg == altPkgPattern) && t.name == namePattern {
							hasExactTestMatch = true
							break
						}
					}
				}

				for _, t := range tests {
					key := t.String()
					if seen[key] {
						continue
					}

					// If this is an exact package name, match all tests in that package
					if exactPkgMatches && (t.pkg == arg || t.pkg == altArg) {
						result = append(result, t)
						seen[key] = true
						continue
					}

					// If there's an exact test match, only match that exact test
					if hasExactTestMatch {
						if (t.pkg == pkgPattern || t.pkg == altPkgPattern) && t.name == namePattern {
							result = append(result, t)
							seen[key] = true
						}
						continue
					}

					// Handle wildcard patterns: pkg/* matches all tests in pkg
					if namePattern == "*" {
						if t.pkg == pkgPattern || t.pkg == altPkgPattern ||
							strings.Contains(t.pkg, pkgPattern) || strings.Contains(t.pkg, altPkgPattern) {
							result = append(result, t)
							seen[key] = true
						}
						continue
					}

					// Handle substring matching only if no exact matches found
					nameMatch := strings.Contains(t.name, namePattern)
					pkgMatch := t.pkg == pkgPattern || t.pkg == altPkgPattern ||
						strings.Contains(t.pkg, pkgPattern) || strings.Contains(t.pkg, altPkgPattern)
					if nameMatch && pkgMatch {
						result = append(result, t)
						seen[key] = true
					}
				}
			}
		} else {
			// Filter by package pattern
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

// runMegaTests runs tests using the mega test binary with concurrency
func runMegaTests(tasks []megaTask) bool {
	if len(tasks) == 0 {
		fmt.Println("no tests to run")
		return true
	}

	// Create workers for concurrent execution — one task per process, just like ut
	testWorkerCount := p
	taskCh := make(chan megaTask, 100)
	works := make([]megaWorker, testWorkerCount)
	var wg sync.WaitGroup

	for i := range testWorkerCount {
		wg.Add(1)
		go works[i].worker(&wg, taskCh)
	}

	// Shuffle for better load distribution
	shuffleMegaTasks(tasks)

	start := time.Now()
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()

	fmt.Println("run all mega tests takes", time.Since(start))

	// Collect and write results
	if junitfile != "" {
		if err := writeMegaJUnitResults(works, junitfile); err != nil {
			fmt.Println("write junit file error:", err)
			return false
		}
	}

	if coverprofile != "" {
		// Mega tests use a single binary, so coverage is already in one file
		// We need to rename it to the target location
		srcCover := megaBinary + ".coverprofile"
		if _, err := os.Stat(srcCover); err == nil {
			if err := os.Rename(srcCover, coverprofile); err != nil {
				fmt.Println("rename cover file error:", err)
			}
		}
	}

	for _, work := range works {
		if work.Fail {
			return false
		}
	}
	return true
}

// shuffleMegaTasks randomly shuffles mega tasks
func shuffleMegaTasks(tasks []megaTask) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(tasks), func(i, j int) {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	})
}

// megaWorker represents a worker that runs mega tests
type megaWorker struct {
	Fail    bool
	results []megaTestResult
}

// worker runs mega tests from the task channel — one test per process
func (w *megaWorker) worker(wg *sync.WaitGroup, ch chan megaTask) {
	defer wg.Done()
	for t := range ch {
		res := w.runSingleMegaTest(t)
		w.results = append(w.results, res)
		if res.err != nil {
			w.Fail = true
		}
	}
}

// megaTestResult represents the result of running a mega test
type megaTestResult struct {
	pkg      string
	testName string
	duration time.Duration
	err      error
	output   string
}

// runSingleMegaTest runs a single test using mega.test binary in an isolated process
func (w *megaWorker) runSingleMegaTest(t megaTask) megaTestResult {
	pattern := fmt.Sprintf("%s/%s", t.pkg, t.name)

	start := time.Now()
	var output bytes.Buffer
	var err error
	timedOut := false

	// Retry logic for flaky tests
	for range 3 {
		cmd := exec.Command(megaBinary,
			"-test.run", "^TestMega$",
			"-mega.run", pattern,
		)

		if coverprofile != "" {
			// Each test run generates its own cover profile
			// We'll merge them later
			cmd.Args = append(cmd.Args, "-test.coverprofile",
				filepath.Join(coverFileTempDir,
					strings.ReplaceAll(t.pkg, "/", "_")+"_"+t.name+".cov"))
		}

		cmd.Stdout = &output
		cmd.Stderr = &output

		// Add 3 minute timeout to prevent hanging tests
		timer := time.AfterFunc(3*time.Minute, func() {
			if cmd.Process != nil {
				cmd.Process.Kill()
				timedOut = true
			}
		})

		err = cmd.Run()
		timer.Stop()
		if err != nil {
			// Check for retryable errors
			if strings.Contains(output.String(), "signal: segmentation fault") ||
				strings.Contains(output.String(), "signal: trace/breakpoint trap") ||
				strings.Contains(output.String(), "panic during panic") {
				output.Reset()
				timedOut = false
				continue
			}
		}
		break
	}

	result := megaTestResult{
		pkg:      t.pkg,
		testName: t.name,
		duration: time.Since(start),
		err:      err,
		output:   output.String(),
	}

	// Output test result immediately — PASS to stdout, FAIL/TIMEOUT details to stderr
	if timedOut {
		fmt.Printf("[TIMEOUT]  %s/%s  %.2fs\n", t.pkg, t.name, result.duration.Seconds())
	} else if err != nil {
		fmt.Printf("[FAIL]     %s/%s  %.2fs\n", t.pkg, t.name, result.duration.Seconds())
		// Print error output to stderr for debugging
		if output.Len() > 0 {
			fmt.Fprintf(os.Stderr, "--- FAIL: %s/%s ---\n%s\n--- END ---\n",
				t.pkg, t.name, filterMegaOutput(output.String()))
		}
	} else {
		fmt.Printf("[PASS]     %s/%s  %.2fs\n", t.pkg, t.name, result.duration.Seconds())
	}

	return result
}

// writeMegaJUnitResults writes JUnit test results from mega workers
func writeMegaJUnitResults(workers []megaWorker, filename string) error {
	version := goVersion()
	pkgs := make(map[string][]JUnitTestCase)
	durations := make(map[string]time.Duration)

	for _, w := range workers {
		for _, res := range w.results {
			if res.testName == "" {
				continue
			}

			classname := filepath.Join(modulePath, res.pkg)
			tc := JUnitTestCase{
				Classname: classname,
				Name:      res.testName,
				Time:      formatDurationAsSeconds(res.duration),
			}

			if res.err != nil {
				tc.Failure = &JUnitFailure{
					Message:  "Failed",
					Contents: res.output,
				}
			}

			pkgs[classname] = append(pkgs[classname], tc)
			durations[classname] = durations[classname] + res.duration
		}
	}

	suites := JUnitTestSuites{}
	for pkg, cases := range pkgs {
		suite := JUnitTestSuite{
			Tests:      len(cases),
			Failures:   failureCases(cases),
			Time:       formatDurationAsSeconds(durations[pkg]),
			Name:       pkg,
			Properties: packageProperties(version),
			TestCases:  cases,
		}
		suites.Suites = append(suites.Suites, suite)
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return write(f, suites)
}

// listMegaTestCases lists test cases for mega mode (for compatibility)
func listMegaTestCases(pkg string, tasks []task) ([]task, error) {
	// In mega mode, we don't use the traditional task structure
	// This is a placeholder for compatibility
	return tasks, nil
}

// buildMegaTestBinaryMulti is a faster build method for mega tests
func buildMegaTestBinaryMulti(pkgs []string) error {
	// In mega mode, we only build one binary
	return buildMegaTestBinary()
}

// getMegaTestsForPackages gets all mega tests for the given packages
func getMegaTestsForPackages(pkgs []string) ([]megaTask, error) {
	allTests, err := listMegaTests()
	if err != nil {
		return nil, err
	}

	// Filter by packages
	packageSet := make(map[string]struct{})
	for _, pkg := range pkgs {
		packageSet[pkg] = struct{}{}
	}

	var result []megaTask
	for _, test := range allTests {
		if _, ok := packageSet[test.pkg]; ok {
			result = append(result, test)
		}
	}

	return result, nil
}

// convertMegaTasksToTasks converts megaTask to task for compatibility
func convertMegaTasksToTasks(megaTasks []megaTask) []task {
	tasks := make([]task, len(megaTasks))
	for i, mt := range megaTasks {
		tasks[i] = task{pkg: mt.pkg, test: mt.name}
	}
	return tasks
}

// cmdMegaList lists all mega tests
func cmdMegaList(args ...string) bool {
	// Verify the mega binary exists (should be built by Makefile already)
	workDir, _ := os.Getwd()
	megaBinary = filepath.Join(workDir, "bazel-bin/pkg/testkit/mega/mega_test_/mega_test")
	if _, err := os.Stat(megaBinary); err != nil {
		log.Fatalf("mega test binary not found at %s — run 'make bazel-mega-binary' first", megaBinary)
	}

	tests, err := listMegaTests()
	if err != nil {
		log.Println("list mega tests error", err)
		return false
	}

	// Filter if args provided
	if len(args) > 0 {
		tests = filterMegaTests(tests, args)
	}

	// Print tests
	for _, t := range tests {
		fmt.Printf("%s/%s\n", t.pkg, t.name)
	}

	return true
}

// shuffleStrings randomly shuffles a string slice
func shuffleStrings(slice []string) {
	for i := range slice {
		pos := int(time.Now().UnixNano()+int64(i)) % len(slice)
		if pos < 0 {
			pos = -pos
		}
		slice[i], slice[pos] = slice[pos], slice[i]
	}
}

// filterMegaOutput removes verbose registry dump lines from mega test output
func filterMegaOutput(output string) string {
	var filtered []string
	for _, line := range strings.Split(output, "\n") {
		// Skip the huge "Registered tests: [...]" line
		if strings.Contains(line, "Registered tests:") {
			continue
		}
		filtered = append(filtered, line)
	}
	// Only keep last 30 lines to avoid flooding stderr
	if len(filtered) > 30 {
		filtered = filtered[len(filtered)-30:]
	}
	return strings.Join(filtered, "\n")
}
