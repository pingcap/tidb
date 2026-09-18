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

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/util/mock"
)

func TestAuditLogRetrieverCleansLogPath(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "audit")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	line := []byte("[2026/09/18 19:17:37.321 +08:00] [INFO] [USER=test]\n")
	base := filepath.Join(logDir, "tidb-audit.log")
	if err := os.WriteFile(base, line, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "tidb-audit-2026-09-18.log"), line, 0o600); err != nil {
		t.Fatal(err)
	}

	uncleanPath := dir + "/audit/../audit/tidb-audit.log"
	files, err := (&auditLogRetriever{checker: &auditLogChecker{}}).getAllFiles(context.Background(), mock.NewContext(), uncleanPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, file := range files {
			_ = file.file.Close()
		}
	}()
	if len(files) != 2 {
		t.Fatalf("expected both current and rotated audit files, got %d", len(files))
	}
}
