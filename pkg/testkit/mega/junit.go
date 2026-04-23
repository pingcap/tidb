// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with in compliance with the License.
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
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// JUnit XML types for writing test results in JUnit format.
// These are used by the orchestrator to write --junitfile output.

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

// modulePath is the Go module path used as class name prefix in JUnit output.
var modulePath = "github.com/pingcap/tidb"

// formatDurationAsSeconds formats a duration as a decimal seconds string.
func formatDurationAsSeconds(d time.Duration) string {
	return fmt.Sprintf("%f", d.Seconds())
}

// goVersion returns the Go version from the GOVERSION env var or the go binary.
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

// packageProperties returns standard JUnit properties for a test suite.
func packageProperties(version string) []JUnitProperty {
	return []JUnitProperty{
		{Name: "go.version", Value: version},
	}
}

// writeJUnitXML writes JUnit test suites to the writer as indented XML.
func writeJUnitXML(out io.Writer, suites JUnitTestSuites) error {
	doc, err := xml.MarshalIndent(suites, "", "\t")
	if err != nil {
		return err
	}
	if _, err := out.Write([]byte(xml.Header)); err != nil {
		return err
	}
	_, err = out.Write(doc)
	return err
}

// failureCases counts the number of failed test cases.
func failureCases(cases []JUnitTestCase) int {
	sum := 0
	for _, v := range cases {
		if v.Failure != nil {
			sum++
		}
	}
	return sum
}
