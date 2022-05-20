// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	// Set the correct value when it runs inside docker.
	_ "go.uber.org/automaxprocs"
	"golang.org/x/tools/cover"
)

func usage() bool {
	msg := `// run all tests
ut

// show usage
ut -h

// list all packages
ut list

// list test cases of a single package
ut list $package

// list test cases that match a pattern
ut list $package 'r:$regex'

// run all tests
ut run

// run test all cases of a single package
ut run $package

// run test cases of a single package
ut run $package $test

// run test cases that match a pattern
ut run $package 'r:$regex'

// build all test package
ut build

// build a test package
ut build xxx

// write the junitfile
ut run --junitfile xxx

// test with race flag
ut run --race`

	fmt.Println(msg)
	return true
}

const modulePath = "github.com/pingcap/tidb"

type task struct {
	pkg  string
	test string
}

func (t *task) String() string {
	return t.pkg + " " + t.test
}

var P int
var workDir string

func cmdList(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		fmt.Println("list package error", err)
		return false
	}

	// list all packages
	if len(args) == 0 {
		for _, pkg := range pkgs {
			fmt.Println(pkg)
		}
		return false
	}

	// list test case of a single package
	if len(args) == 1 || len(args) == 2 {
		pkg := args[0]
		pkgs = filter(pkgs, func(s string) bool { return s == pkg })
		if len(pkgs) != 1 {
			fmt.Println("package not exist", pkg)
			return false
		}

		err := buildTestBinary(pkg)
		if err != nil {
			fmt.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			fmt.Println("check test binary existance error", err)
			return false
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}

		res, err := listTestCases(pkg, nil)
		if err != nil {
			fmt.Println("list test cases for package error", err)
			return false
		}

		if len(args) == 2 {
			res, err = filterTestCases(res, args[1])
			if err != nil {
				fmt.Println("filter test cases error", err)
				return false
			}
		}

		for _, x := range res {
			fmt.Println(x.test)
		}
	}
	return true
}

func cmdBuild(args ...string) bool {
	pkgs, err := listPackages()
	if err != nil {
		fmt.Println("list package error", err)
		return false
	}

	// build all packages
	if len(args) == 0 {
		err := buildTestBinaryMulti(pkgs)
		if err != nil {
			fmt.Println("build package error", pkgs, err)
			return false
		}
		return true
	}

	// build test binary of a single package
	if len(args) >= 1 {
		pkg := args[0]
		err := buildTestBinary(pkg)
		if err != nil {
			fmt.Println("build package error", pkg, err)
			return false
		}
	}
	return true
}

func cmdRun(args ...string) bool {
	var err error
	pkgs, err := listPackages()
	if err != nil {
		fmt.Println("list packages error", err)
		return false
	}
	tasks := make([]task, 0, 5000)
	start := time.Now()
	// run all tests
	if len(args) == 0 {
		err := buildTestBinaryMulti(pkgs)
		if err != nil {
			fmt.Println("build package error", pkgs, err)
			return false
		}

		for _, pkg := range pkgs {
			exist, err := testBinaryExist(pkg)
			if err != nil {
				fmt.Println("check test binary existance error", err)
				return false
			}
			if !exist {
				fmt.Println("no test case in ", pkg)
				continue
			}

			tasks, err = listTestCases(pkg, tasks)
			if err != nil {
				fmt.Println("list test cases error", err)
				return false
			}
		}
	}

	// run tests for a single package
	if len(args) == 1 {
		pkg := args[0]
		err := buildTestBinary(pkg)
		if err != nil {
			fmt.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			fmt.Println("check test binary existance error", err)
			return false
		}

		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}
		tasks, err = listTestCases(pkg, tasks)
		if err != nil {
			fmt.Println("list test cases error", err)
			return false
		}
	}

	// run a single test
	if len(args) == 2 {
		pkg := args[0]
		err := buildTestBinary(pkg)
		if err != nil {
			fmt.Println("build package error", pkg, err)
			return false
		}
		exist, err := testBinaryExist(pkg)
		if err != nil {
			fmt.Println("check test binary existance error", err)
			return false
		}
		if !exist {
			fmt.Println("no test case in ", pkg)
			return false
		}

		tasks, err = listTestCases(pkg, tasks)
		if err != nil {
			fmt.Println("list test cases error", err)
			return false
		}
		tasks, err = filterTestCases(tasks, args[1])
		if err != nil {
			fmt.Println("filter test cases error", err)
			return false
		}
	}

	if except != "" {
		list, err := parseCaseListFromFile(except)
		if err != nil {
			fmt.Println("parse --except file error", err)
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
			fmt.Println("parse --only file error", err)
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

	fmt.Printf("building task finish, maxproc=%d, count=%d, takes=%v\n", P, len(tasks), time.Since(start))

	taskCh := make(chan task, 100)
	works := make([]numa, P)
	var wg sync.WaitGroup
	for i := 0; i < P; i++ {
		wg.Add(1)
		go works[i].worker(&wg, taskCh)
	}

	shuffle(tasks)

	start = time.Now()
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	fmt.Println("run all tasks takes", time.Since(start))

	if junitfile != "" {
		out := collectTestResults(works)
		f, err := os.Create(junitfile)
		if err != nil {
			fmt.Println("create junit file fail:", err)
			return false
		}
		if err := write(f, out); err != nil {
			fmt.Println("write junit file error:", err)
			return false
		}
	}

	for _, work := range works {
		if work.Fail {
			return false
		}
	}
	if coverprofile != "" {
		collectCoverProfileFile()
	}
	return true
}

func parseCaseListFromFile(fileName string) (map[string]struct{}, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, withTrace(err)
	}
	defer f.Close()

	ret := make(map[string]struct{})
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Bytes()
		ret[string(line)] = struct{}{}
	}
	if err := s.Err(); err != nil {
		return nil, withTrace(err)
	}
	return ret, nil
}

// handleFlags strip the '--flag xxx' from the command line os.Args
// Example of the os.Args changes
// Before: ut run sessoin TestXXX --coverprofile xxx --junitfile yyy
// After: ut run session TestXXX
// The value of the flag is returned.
func handleFlags(flag string) string {
	var res string
	tmp := os.Args[:0]
	// Iter to the flag
	var i int
	for ; i < len(os.Args); i++ {
		if os.Args[i] == flag {
			i++
			break
		}
		tmp = append(tmp, os.Args[i])
	}
	// Handle the flag
	if i < len(os.Args) {
		res = os.Args[i]
		i++
	}
	// Iter the remain flags
	for ; i < len(os.Args); i++ {
		tmp = append(tmp, os.Args[i])
	}

	// os.Args is now the original flags with '--coverprofile XXX' removed.
	os.Args = tmp
	return res
}

func handleRaceFlag() (found bool) {
	tmp := os.Args[:0]
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--race" {
			found = true
			continue
		}
		tmp = append(tmp, os.Args[i])
	}
	os.Args = tmp
	return
}

var junitfile string
var coverprofile string
var coverFileTempDir string
var race bool

var except string
var only string

func main() {
	junitfile = handleFlags("--junitfile")
	coverprofile = handleFlags("--coverprofile")
	except = handleFlags("--except")
	only = handleFlags("--only")
	race = handleRaceFlag()

	if coverprofile != "" {
		var err error
		coverFileTempDir, err = os.MkdirTemp(os.TempDir(), "cov")
		if err != nil {
			fmt.Println("create temp dir fail", coverFileTempDir)
			os.Exit(1)
		}
		defer os.Remove(coverFileTempDir)
	}

	// Get the correct count of CPU if it's in docker.
	P = runtime.GOMAXPROCS(0)
	rand.Seed(time.Now().Unix())
	var err error
	workDir, err = os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd() error", err)
	}

	var isSucceed bool
	if len(os.Args) == 1 {
		// run all tests
		isSucceed = cmdRun()
	}

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "list":
			isSucceed = cmdList(os.Args[2:]...)
		case "build":
			isSucceed = cmdBuild(os.Args[2:]...)
		case "run":
			isSucceed = cmdRun(os.Args[2:]...)
		default:
			isSucceed = usage()
		}
	}
	if !isSucceed {
		os.Exit(1)
	}
}

func collectCoverProfileFile() {
	// Combine all the cover file of single test function into a whole.
	files, err := os.ReadDir(coverFileTempDir)
	if err != nil {
		fmt.Println("collect cover file error:", err)
		os.Exit(-1)
	}

	w, err := os.Create(coverprofile)
	if err != nil {
		fmt.Println("create cover file error:", err)
		os.Exit(-1)
	}
	defer w.Close()
	w.WriteString("mode: set\n")

	result := make(map[string]*cover.Profile)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		collectOneCoverProfileFile(result, file)
	}

	w1 := bufio.NewWriter(w)
	for _, prof := range result {
		for _, block := range prof.Blocks {
			fmt.Fprintf(w1, "%s:%d.%d,%d.%d %d %d\n",
				prof.FileName,
				block.StartLine,
				block.StartCol,
				block.EndLine,
				block.EndCol,
				block.NumStmt,
				block.Count,
			)
		}
		if err := w1.Flush(); err != nil {
			fmt.Println("flush data to cover profile file error:", err)
			os.Exit(-1)
		}
	}
}

func collectOneCoverProfileFile(result map[string]*cover.Profile, file os.DirEntry) {
	f, err := os.Open(path.Join(coverFileTempDir, file.Name()))
	if err != nil {
		fmt.Println("open temp cover file error:", err)
		os.Exit(-1)
	}
	defer f.Close()

	profs, err := cover.ParseProfilesFromReader(f)
	if err != nil {
		fmt.Println("parse cover profile file error:", err)
		os.Exit(-1)
	}
	mergeProfile(result, profs)
}

func mergeProfile(m map[string]*cover.Profile, profs []*cover.Profile) {
	for _, prof := range profs {
		sort.Sort(blocksByStart(prof.Blocks))
		old, ok := m[prof.FileName]
		if !ok {
			m[prof.FileName] = prof
			continue
		}

		// Merge samples from the same location.
		// The data has already been sorted.
		tmp := old.Blocks[:0]
		var i, j int
		for i < len(old.Blocks) && j < len(prof.Blocks) {
			v1 := old.Blocks[i]
			v2 := prof.Blocks[j]

			switch compareProfileBlock(v1, v2) {
			case -1:
				tmp = appendWithReduce(tmp, v1)
				i++
			case 1:
				tmp = appendWithReduce(tmp, v2)
				j++
			default:
				tmp = appendWithReduce(tmp, v1)
				tmp = appendWithReduce(tmp, v2)
				i++
				j++
			}
		}
		for ; i < len(old.Blocks); i++ {
			tmp = appendWithReduce(tmp, old.Blocks[i])
		}
		for ; j < len(prof.Blocks); j++ {
			tmp = appendWithReduce(tmp, prof.Blocks[j])
		}

		m[prof.FileName] = old
	}
}

// appendWithReduce works like append(), but it merge the duplicated values.
func appendWithReduce(input []cover.ProfileBlock, b cover.ProfileBlock) []cover.ProfileBlock {
	if len(input) >= 1 {
		last := &input[len(input)-1]
		if b.StartLine == last.StartLine &&
			b.StartCol == last.StartCol &&
			b.EndLine == last.EndLine &&
			b.EndCol == last.EndCol {
			if b.NumStmt != last.NumStmt {
				panic(fmt.Errorf("inconsistent NumStmt: changed from %d to %d", last.NumStmt, b.NumStmt))
			}
			// Merge the data with the last one of the slice.
			last.Count |= b.Count
			return input
		}
	}
	return append(input, b)
}

type blocksByStart []cover.ProfileBlock

func compareProfileBlock(x, y cover.ProfileBlock) int {
	if x.StartLine < y.StartLine {
		return -1
	}
	if x.StartLine > y.StartLine {
		return 1
	}

	// Now x.StartLine == y.StartLine
	if x.StartCol < y.StartCol {
		return -1
	}
	if x.StartCol > y.StartCol {
		return 1
	}

	return 0
}

func (b blocksByStart) Len() int      { return len(b) }
func (b blocksByStart) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b blocksByStart) Less(i, j int) bool {
	bi, bj := b[i], b[j]
	return bi.StartLine < bj.StartLine || bi.StartLine == bj.StartLine && bi.StartCol < bj.StartCol
}

func listTestCases(pkg string, tasks []task) ([]task, error) {
	newCases, err := listNewTestCases(pkg)
	if err != nil {
		fmt.Println("list test case error", pkg, err)
		return nil, withTrace(err)
	}
	for _, c := range newCases {
		tasks = append(tasks, task{pkg, c})
	}

	return tasks, nil
}

func filterTestCases(tasks []task, arg1 string) ([]task, error) {
	if strings.HasPrefix(arg1, "r:") {
		r, err := regexp.Compile(arg1[2:])
		if err != nil {
			return nil, err
		}
		tmp := tasks[:0]
		for _, task := range tasks {
			if r.MatchString(task.test) {
				tmp = append(tmp, task)
			}
		}
		return tmp, nil
	}
	tmp := tasks[:0]
	for _, task := range tasks {
		if strings.Contains(task.test, arg1) {
			tmp = append(tmp, task)
		}
	}
	return tmp, nil
}

func listPackages() ([]string, error) {
	cmd := exec.Command("go", "list", "./...")
	ss, err := cmdToLines(cmd)
	if err != nil {
		return nil, withTrace(err)
	}

	ret := ss[:0]
	for _, s := range ss {
		if !strings.HasPrefix(s, modulePath) {
			continue
		}
		pkg := s[len(modulePath)+1:]
		if skipDIR(pkg) {
			continue
		}
		ret = append(ret, pkg)
	}
	return ret, nil
}

type numa struct {
	Fail    bool
	results []testResult
}

func (n *numa) worker(wg *sync.WaitGroup, ch chan task) {
	defer wg.Done()
	for t := range ch {
		res := n.runTestCase(t.pkg, t.test)
		if res.Failure != nil {
			fmt.Println("[FAIL] ", t.pkg, t.test)
			fmt.Fprintf(os.Stderr, "err=%s\n%s", res.err.Error(), res.Failure.Contents)
			n.Fail = true
		}
		n.results = append(n.results, res)
	}
}

type testResult struct {
	JUnitTestCase
	d   time.Duration
	err error
}

func (n *numa) runTestCase(pkg string, fn string) testResult {
	res := testResult{
		JUnitTestCase: JUnitTestCase{
			Classname: path.Join(modulePath, pkg),
			Name:      fn,
		},
	}

	var buf bytes.Buffer
	var err error
	var start time.Time
	for i := 0; i < 3; i++ {
		cmd := n.testCommand(pkg, fn)
		cmd.Dir = path.Join(workDir, pkg)
		// Combine the test case output, so the run result for failed cases can be displayed.
		cmd.Stdout = &buf
		cmd.Stderr = &buf

		start = time.Now()
		err = cmd.Run()
		if err != nil {
			if _, ok := err.(*exec.ExitError); ok {
				// Retry 3 times to get rid of the weird error:
				switch err.Error() {
				case "signal: segmentation fault (core dumped)":
					buf.Reset()
					continue
				case "signal: trace/breakpoint trap (core dumped)":
					buf.Reset()
					continue
				}
				if strings.Contains(buf.String(), "panic during panic") {
					buf.Reset()
					continue
				}
			}
		}
		break
	}
	if err != nil {
		res.Failure = &JUnitFailure{
			Message:  "Failed",
			Contents: buf.String(),
		}
		res.err = err
	}

	res.d = time.Since(start)
	res.Time = formatDurationAsSeconds(res.d)
	return res
}

func collectTestResults(workers []numa) JUnitTestSuites {
	version := goVersion()
	// pkg => test cases
	pkgs := make(map[string][]JUnitTestCase)
	durations := make(map[string]time.Duration)

	// The test result in workers are shuffled, so group by the packages here
	for _, n := range workers {
		for _, res := range n.results {
			cases, ok := pkgs[res.Classname]
			if !ok {
				cases = make([]JUnitTestCase, 0, 10)
			}
			cases = append(cases, res.JUnitTestCase)
			pkgs[res.Classname] = cases
			durations[res.Classname] = durations[res.Classname] + res.d
		}
	}

	suites := JUnitTestSuites{}
	// Turn every package result to a suite.
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
	return suites
}

func failureCases(input []JUnitTestCase) int {
	sum := 0
	for _, v := range input {
		if v.Failure != nil {
			sum++
		}
	}
	return sum
}

func (n *numa) testCommand(pkg string, fn string) *exec.Cmd {
	args := make([]string, 0, 10)
	exe := "./" + testFileName(pkg)
	if coverprofile != "" {
		fileName := strings.ReplaceAll(pkg, "/", "_") + "." + fn
		tmpFile := path.Join(coverFileTempDir, fileName)
		args = append(args, "-test.coverprofile", tmpFile)
	}
	args = append(args, "-test.cpu", "1")
	if !race {
		// Don't set timeout for race because it takes a longer when race is enabled.
		args = append(args, []string{"-test.timeout", "2m"}...)
	}
	// session.test -test.run TestClusteredPrefixColum
	args = append(args, "-test.run", fn)

	return exec.Command(exe, args...)
}

func skipDIR(pkg string) bool {
	skipDir := []string{"br", "cmd", "dumpling"}
	for _, ignore := range skipDir {
		if strings.HasPrefix(pkg, ignore) {
			return true
		}
	}
	return false
}

func buildTestBinary(pkg string) error {
	// go test -c
	cmd := exec.Command("go", "test", "-c", "-vet", "off", "-o", testFileName(pkg))
	if coverprofile != "" {
		cmd.Args = append(cmd.Args, "-cover")
	}
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	cmd.Dir = path.Join(workDir, pkg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

// buildTestBinaryMulti is much faster than build the test packages one by one.
func buildTestBinaryMulti(pkgs []string) error {
	// go test --exec=xprog -cover -vet=off --count=0 $(pkgs)
	xprogPath := path.Join(workDir, "tools/bin/xprog")
	packages := make([]string, 0, len(pkgs))
	for _, pkg := range pkgs {
		packages = append(packages, path.Join(modulePath, pkg))
	}

	var cmd *exec.Cmd
	cmd = exec.Command("go", "test", "--exec", xprogPath, "-vet", "off", "-count", "0")
	if coverprofile != "" {
		cmd.Args = append(cmd.Args, "-cover")
	}
	if race {
		cmd.Args = append(cmd.Args, "-race")
	}
	cmd.Args = append(cmd.Args, packages...)
	cmd.Dir = workDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return withTrace(err)
	}
	return nil
}

func testBinaryExist(pkg string) (bool, error) {
	_, err := os.Stat(testFileFullPath(pkg))
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return false, nil
		}
	}
	return true, withTrace(err)
}

func testFileName(pkg string) string {
	_, file := path.Split(pkg)
	return file + ".test.bin"
}

func testFileFullPath(pkg string) string {
	return path.Join(workDir, pkg, testFileName(pkg))
}

func listNewTestCases(pkg string) ([]string, error) {
	exe := "./" + testFileName(pkg)

	// session.test -test.list Test
	cmd := exec.Command(exe, "-test.list", "Test")
	cmd.Dir = path.Join(workDir, pkg)
	res, err := cmdToLines(cmd)
	if err != nil {
		return nil, withTrace(err)
	}
	return filter(res, func(s string) bool {
		return strings.HasPrefix(s, "Test") && s != "TestT" && s != "TestBenchDaily"
	}), nil
}

func cmdToLines(cmd *exec.Cmd) ([]string, error) {
	res, err := cmd.Output()
	if err != nil {
		return nil, withTrace(err)
	}
	ss := bytes.Split(res, []byte{'\n'})
	ret := make([]string, len(ss))
	for i, s := range ss {
		ret[i] = string(s)
	}
	return ret, nil
}

func filter(input []string, f func(string) bool) []string {
	ret := input[:0]
	for _, s := range input {
		if f(s) {
			ret = append(ret, s)
		}
	}
	return ret
}

func shuffle(tasks []task) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < len(tasks); i++ {
		pos := rand.Intn(len(tasks))
		tasks[i], tasks[pos] = tasks[pos], tasks[i]
	}
}

type errWithStack struct {
	err error
	buf []byte
}

func (e *errWithStack) Error() string {
	return e.err.Error() + "\n" + string(e.buf)
}

func withTrace(err error) error {
	if err == nil {
		return err
	}
	if _, ok := err.(*errWithStack); ok {
		return err
	}
	var stack [4096]byte
	sz := runtime.Stack(stack[:], false)
	return &errWithStack{err, stack[:sz]}
}

func formatDurationAsSeconds(d time.Duration) string {
	return fmt.Sprintf("%f", d.Seconds())
}

func packageProperties(goVersion string) []JUnitProperty {
	return []JUnitProperty{
		{Name: "go.version", Value: goVersion},
	}
}

// goVersion returns the version as reported by the go binary in PATH. This
// version will not be the same as runtime.Version, which is always the version
// of go used to build the gotestsum binary.
//
// To skip the os/exec call set the GOVERSION environment variable to the
// desired value.
func goVersion() string {
	if version, ok := os.LookupEnv("GOVERSION"); ok {
		return version
	}
	cmd := exec.Command("go", "version")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimPrefix(strings.TrimSpace(string(out)), "go version ")
}

func write(out io.Writer, suites JUnitTestSuites) error {
	doc, err := xml.MarshalIndent(suites, "", "\t")
	if err != nil {
		return err
	}
	_, err = out.Write([]byte(xml.Header))
	if err != nil {
		return err
	}
	_, err = out.Write(doc)
	return err
}

// JUnitTestSuites is a collection of JUnit test suites.
type JUnitTestSuites struct {
	XMLName xml.Name `xml:"testsuites"`
	Suites  []JUnitTestSuite
}

// JUnitTestSuite is a single JUnit test suite which may contain many
// testcases.
type JUnitTestSuite struct {
	XMLName    xml.Name        `xml:"testsuite"`
	Tests      int             `xml:"tests,attr"`
	Failures   int             `xml:"failures,attr"`
	Time       string          `xml:"time,attr"`
	Name       string          `xml:"name,attr"`
	Properties []JUnitProperty `xml:"properties>property,omitempty"`
	TestCases  []JUnitTestCase
}

// JUnitTestCase is a single test case with its result.
type JUnitTestCase struct {
	XMLName     xml.Name          `xml:"testcase"`
	Classname   string            `xml:"classname,attr"`
	Name        string            `xml:"name,attr"`
	Time        string            `xml:"time,attr"`
	SkipMessage *JUnitSkipMessage `xml:"skipped,omitempty"`
	Failure     *JUnitFailure     `xml:"failure,omitempty"`
}

// JUnitSkipMessage contains the reason why a testcase was skipped.
type JUnitSkipMessage struct {
	Message string `xml:"message,attr"`
}

// JUnitProperty represents a key/value pair used to define properties.
type JUnitProperty struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// JUnitFailure contains data related to a failed test.
type JUnitFailure struct {
	Message  string `xml:"message,attr"`
	Type     string `xml:"type,attr"`
	Contents string `xml:",chardata"`
}
