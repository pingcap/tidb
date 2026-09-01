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

package export

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

const (
	csePackedPerfPrefix         = "CSE packed perf "
	maxCSEDumperDiagnosticBytes = 64 << 10
)

func readCSEDumperStderr(input io.Reader, observation *packedExportObservation) string {
	reader := bufio.NewReader(input)
	var diagnostics []string
	var line []byte
	diagnosticBytes := 0
	omitted := 0
	dropping := false
	consume := func() {
		if dropping {
			omitted++
			return
		}
		text := strings.TrimSuffix(string(line), "\r")
		if text == "" {
			return
		}
		if strings.HasPrefix(text, csePackedPerfPrefix) {
			observation.forwardCSE(text)
			return
		}
		if diagnosticBytes+len(text)+1 > maxCSEDumperDiagnosticBytes {
			omitted++
			return
		}
		diagnostics = append(diagnostics, text)
		diagnosticBytes += len(text) + 1
	}
	for {
		fragment, more, err := reader.ReadLine()
		if !dropping && len(line)+len(fragment) <= maxCSEDumperDiagnosticBytes {
			line = append(line, fragment...)
		} else {
			line = line[:0]
			dropping = true
		}
		if !more {
			consume()
			line = line[:0]
			dropping = false
		}
		if err != nil {
			if err != io.EOF {
				diagnostics = append(diagnostics, fmt.Sprintf("read cse-ctl stderr: %v", err))
			}
			break
		}
	}
	if omitted > 0 {
		diagnostics = append([]string{fmt.Sprintf("%d cse-ctl diagnostic lines omitted", omitted)}, diagnostics...)
	}
	return strings.Join(diagnostics, "\n")
}
