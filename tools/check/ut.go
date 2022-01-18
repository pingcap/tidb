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
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
	"regexp"
	"encoding/xml"

	// "gotest.tools/gotestsum/log"
	// "gotest.tools/gotestsum/testjson"

	// Set the correct when it runs inside docker.
	_ "go.uber.org/automaxprocs"
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

// run all tests
ut run

// run test all cases of a single package
ut run $package

// run test cases of a single package
ut run $package $test

// build all test package
ut build

// build a test package
ut build xxx

// write the junitfile
ut run --junitfile xxx`
	fmt.Println(msg)
	return true
}

const modulePath = "github.com/pingcap/tidb"

type task struct {
	pkg  string
	test string
	old  bool
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
	if len(args) == 1 {
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
		for _, pkg := range pkgs {
			err := buildTestBinary(pkg)
			if err != nil {
				fmt.Println("build package error", pkg, err)
				return false
			}
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
	// run all tests
	if len(args) == 0 {
		for _, pkg := range pkgs {
			fmt.Println("handling package", pkg)
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
				continue
			}

			tasks, err = listTestCases(pkg, tasks)
			if err != nil {
				fmt.Println("list test cases error", err)
				return false
			}
		}
	}

	// run tests for packages
	if len(args) == 1 {
		re, err := regexp.Compile(args[0])
		if err != nil {
			fmt.Println("compile regexp error for", args[0])
		}

		for _, pkg := range pkgs {
			if !re.MatchString(pkg) {
				continue
			}
			fmt.Println("add package ====", pkg)
			
			err := buildTestBinary(pkg)
			if err != nil {
				fmt.Println("build package error", pkg, err)
				return false
			}
			exist, err := testBinaryExist(pkg)
			if err != nil {
				fmt.Println("check test binary existance error", err)
				continue
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
		// filter the test case to run
		tmp := tasks[:0]
		for _, task := range tasks {
			if strings.Contains(task.test, args[1]) {
				tmp = append(tmp, task)
			}
		}
		tasks = tmp
	}
	fmt.Println("building task finish...", len(tasks))

	taskCh := make(chan task, 100)
	works := make([]numa, P)
	var wg sync.WaitGroup
	for i := 0; i < P; i++ {
		wg.Add(1)
		go works[i].worker(&wg, taskCh)
	}

	shuffle(tasks)
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()

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
	return true
}

func handleJUnitFileFlag() string {
	var junitfile string
	// Handle the --junitfile flag, remove it and set the variable.
	tmp := os.Args[:0]
	// Iter to the flag
	var i int
	for ; i<len(os.Args); i++ {
		if os.Args[i] == "--junitfile" {
			i++
			break
		}
		tmp = append(tmp, os.Args[i])
	}
	// Handle the flag
	if i < len(os.Args) {
		junitfile = os.Args[i]
		i++
	}
	// Iter the remain flags
	for ; i<len(os.Args); i++ {
		tmp = append(tmp, os.Args[i])
	}

	// os.Args is now the original flags with '--junitfile XXX' removed.
	os.Args = tmp
	return junitfile
}

var junitfile string

func main() {
	junitfile = handleJUnitFileFlag()

	// Get the correct count of CPU if it's in docker.
	P = runtime.GOMAXPROCS(0)
	rand.Seed(time.Now().Unix())
	var err error
	workDir, err = os.Getwd()
	if err != nil {
		fmt.Println("os.Getwd() error", err)
	}

	if len(os.Args) == 1 {
		// run all tests
		cmdRun()
		return
	}

	if len(os.Args) >= 2 {
		var isSucceed bool
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
		if !isSucceed {
			os.Exit(1)
		}
	}
}

func listTestCases(pkg string, tasks []task) ([]task, error) {
	newCases, err := listNewTestCases(pkg)
	if err != nil {
		fmt.Println("list test case error", pkg, err)
		return nil, withTrace(err)
	}
	for _, c := range newCases {
		tasks = append(tasks, task{pkg, c, false})
	}

	oldCases, err := listOldTestCases(pkg)
	if err != nil {
		fmt.Println("list old test case error", pkg, err)
		return nil, withTrace(err)
	}
	for _, c := range oldCases {
		tasks = append(tasks, task{pkg, c, true})
	}
	return tasks, nil
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
		res := n.runTestCase(t.pkg, t.test, t.old)
		if res.Failure != nil {
			fmt.Println("[FAIL] ", t.pkg, t.test, t.old)
			n.Fail = true
		}
		n.results = append(n.results, res)
	}
}

type testResult struct {
	JUnitTestCase
	d time.Duration
}

func (n *numa) runTestCase(pkg string, fn string, old bool) testResult {
	res := testResult{
		JUnitTestCase: JUnitTestCase{
			Classname : pkg,
			Name : fn,
		},
	}
	
	exe := "./" + testFileName(pkg)
	var cmd *exec.Cmd
	cmd = n.testCommand(exe, fn, old)
	cmd.Dir = path.Join(workDir, pkg)
	// Combine the test case output, so the run result for failed cases can be displayed.
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	start := time.Now()
	if err := cmd.Run(); err != nil {
		res.Failure = &JUnitFailure{
			Message: "Failed",
			Contents: buf.String(),
		}
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
			cases, ok :=  pkgs[res.Classname]
			if !ok {
				cases = make([]JUnitTestCase, 0, 10)
			}
			cases = append(cases, res.JUnitTestCase)
			pkgs[res.Classname] = cases
			durations[res.Classname] = durations[res.Classname] + res.d
		}
	}

	suites := JUnitTestSuites{}
	// Turn every package resuts to a suite.
	for pkg, cases := range pkgs {
		suite := JUnitTestSuite{
			Tests: len(cases),
			Failures: failureCases(cases),
			Time: formatDurationAsSeconds(durations[pkg]),
			Name: pkg,
			Properties: packageProperties(version),
			TestCases: cases,
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

// func (n *numa) testCommandWithNumaCtl(exe string, fn string, old bool) *exec.Cmd {
// 	if old {
// 		// numactl --physcpubind 3 -- session.test -test.run '^TestT$' -check.f testTxnStateSerialSuite.TestTxnInfoWithPSProtoco
// 		return exec.Command(
// 			"numactl", "--physcpubind", n.cpu, "--",
// 			exe,
// 			// "-test.timeout", "20s",
// 			"-test.cpu", "1", "-test.run", "^TestT$", "-check.f", fn)
// 	}

// 	// numactl --physcpubind 3 -- session.test -test.run TestClusteredPrefixColum
// 	return exec.Command(
// 		"numactl", "--physcpubind", n.cpu, "--",
// 		exe,
// 		// "-test.timeout", "20s",
// 		"-test.cpu", "1", "-test.run", fn)
// }

func (n *numa) testCommand(exe string, fn string, old bool) *exec.Cmd {
	if old {
		// session.test -test.run '^TestT$' -check.f testTxnStateSerialSuite.TestTxnInfoWithPSProtoco
		return exec.Command(
			exe,
			// "-test.timeout", "20s",
			"-test.cpu", "1", "-test.run", "^TestT$", "-check.f", fn)
	}

	// session.test -test.run TestClusteredPrefixColum
	return exec.Command(
		exe,
		// "-test.timeout", "20s",
		"-test.cpu", "1", "-test.run", fn)
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
	cmd.Dir = path.Join(workDir, pkg)
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

func listOldTestCases(pkg string) (res []string, err error) {
	exe := "./" + testFileName(pkg)

	// Maybe the restructure is finish on this package.
	cmd := exec.Command(exe, "-h")
	cmd.Dir = path.Join(workDir, pkg)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		err = withTrace(err)
		return
	}
	if !bytes.Contains(buf, []byte("check.list")) {
		// there is no old test case in pkg
		return
	}

	// session.test -test.run TestT -check.list Test
	cmd = exec.Command(exe, "-test.run", "^TestT$", "-check.list", "Test")
	cmd.Dir = path.Join(workDir, pkg)
	res, err = cmdToLines(cmd)
	res = filter(res, func(s string) bool { return strings.Contains(s, "Test") })
	return res, withTrace(err)
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

// func generate(exec *testjson.Execution, cfg Config) JUnitTestSuites {
// 	cfg = configWithDefaults(cfg)
// 	version := goVersion()
// 	suites := JUnitTestSuites{}

// 	for _, pkgname := range exec.Packages() {
// 		pkg := exec.Package(pkgname)
// 		junitpkg := JUnitTestSuite{
// 			Name:       cfg.FormatTestSuiteName(pkgname),
// 			Tests:      pkg.Total,
// 			Time:       formatDurationAsSeconds(pkg.Elapsed()),
// 			Properties: packageProperties(version),
// 			TestCases:  packageTestCases(pkg, cfg.FormatTestCaseClassname),
// 			Failures:   len(pkg.Failed),
// 		}
// 		suites.Suites = append(suites.Suites, junitpkg)
// 	}
// 	return suites
// }

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

// func packageTestCases(pkg *testjson.Package, formatClassname FormatFunc) []JUnitTestCase {
// 	cases := []JUnitTestCase{}

// 	if pkg.TestMainFailed() {
// 		jtc := newJUnitTestCase(testjson.TestCase{Test: "TestMain"}, formatClassname)
// 		jtc.Failure = &JUnitFailure{
// 			Message:  "Failed",
// 			Contents: pkg.Output(0),
// 		}
// 		cases = append(cases, jtc)
// 	}

// 	for _, tc := range pkg.Failed {
// 		jtc := newJUnitTestCase(tc, formatClassname)
// 		jtc.Failure = &JUnitFailure{
// 			Message:  "Failed",
// 			Contents: strings.Join(pkg.OutputLines(tc), ""),
// 		}
// 		cases = append(cases, jtc)
// 	}

// 	for _, tc := range pkg.Skipped {
// 		jtc := newJUnitTestCase(tc, formatClassname)
// 		jtc.SkipMessage = &JUnitSkipMessage{
// 			Message: strings.Join(pkg.OutputLines(tc), ""),
// 		}
// 		cases = append(cases, jtc)
// 	}

// 	for _, tc := range pkg.Passed {
// 		jtc := newJUnitTestCase(tc, formatClassname)
// 		cases = append(cases, jtc)
// 	}
// 	return cases
// }

// func newJUnitTestCase(tc testjson.TestCase, formatClassname FormatFunc) JUnitTestCase {
// 	return JUnitTestCase{
// 		Classname: formatClassname(tc.Package),
// 		Name:      tc.Test.Name(),
// 		Time:      formatDurationAsSeconds(tc.Elapsed),
// 	}
// }

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
