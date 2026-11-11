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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDumpNewDepsBzlUsesGoModules(t *testing.T) {
	t.Setenv("RUNFILES_DIR", t.TempDir())
	t.Setenv("TEST_WORKSPACE", "mirror_test")
	zip := filepath.Join(t.TempDir(), "module.zip")
	if err := os.WriteFile(zip, []byte("module archive"), 0600); err != nil {
		t.Fatal(err)
	}
	previousMirror := isMirror
	isMirror = true // Legacy --mirror callers must not restore retired URLs.
	defer func() { isMirror = previousMirror }()

	listed := map[string]listedModule{
		"github.com/Azure/azure-sdk-for-go/sdk/azcore": {
			Path: "github.com/Azure/azure-sdk-for-go/sdk/azcore", Version: "v1.12.0",
		},
		"github.com/go-ldap/ldap/v3": {
			Path: "github.com/go-ldap/ldap/v3", Version: "v3.4.4",
			Replace: &listedModule{Path: "github.com/YangKeao/ldap/v3", Version: "v3.4.5-0.20230421065457-369a3bab1117"},
		},
	}
	downloaded := map[string]downloadedModule{
		"github.com/Azure/azure-sdk-for-go/sdk/azcore": {
			Sum: "h1:azure-module-sum", Version: "v1.12.0", Zip: zip,
		},
		"github.com/YangKeao/ldap/v3": {
			Sum: "h1:ldap-module-sum", Version: "v3.4.5-0.20230421065457-369a3bab1117", Zip: zip,
		},
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	stdout := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = stdout }()
	err = dumpNewDepsBzl(listed, downloaded)
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`sum = "h1:azure-module-sum"`, `version = "v1.12.0"`,
		`replace = "github.com/YangKeao/ldap/v3"`, `sum = "h1:ldap-module-sum"`,
		`version = "v3.4.5-0.20230421065457-369a3bab1117"`,
	} {
		if !strings.Contains(string(output), expected) {
			t.Errorf("missing module metadata %q in generated DEPS.bzl", expected)
		}
	}
	for _, retired := range []string{"pingcapmirror", "cache.hawkingrei.com", "urls =", "sha256 =", "strip_prefix ="} {
		if strings.Contains(string(output), retired) {
			t.Errorf("generated DEPS.bzl retains retired mirror metadata %q", retired)
		}
	}
}
