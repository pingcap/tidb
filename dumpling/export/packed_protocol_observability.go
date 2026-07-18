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
	"bytes"
	"fmt"
	"strings"
	"sync"
)

const (
	csePackedPerfPrefix         = "CSE packed perf "
	maxCSEDumperDiagnosticBytes = 64 << 10
	maxCSEDumperStderrLineBytes = 64 << 10
)

// cseDumperStderr forwards complete packed-perf lines and retains complete
// non-perf lines only for an eventual child-process error.
type cseDumperStderr struct {
	mu              sync.Mutex
	observation     *packedScanContext
	pending         []byte
	diagnosticLines []string
	diagBytes       int
	omitted         uint64
	dropping        bool
	finished        bool
}

func newCSEDumperStderr(observation *packedScanContext) cseDumperStderr {
	return cseDumperStderr{observation: observation}
}

func (w *cseDumperStderr) Write(data []byte) (int, error) {
	written := len(data)
	forward := make([]string, 0, 1)

	w.mu.Lock()
	for len(data) > 0 {
		if w.dropping {
			newline := bytes.IndexByte(data, '\n')
			if newline < 0 {
				data = nil
				continue
			}
			w.dropping = false
			w.omitted++
			data = data[newline+1:]
			continue
		}

		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			if len(w.pending)+len(data) > maxCSEDumperStderrLineBytes {
				w.pending = w.pending[:0]
				w.dropping = true
			} else {
				w.pending = append(w.pending, data...)
			}
			break
		}

		if len(w.pending)+newline > maxCSEDumperStderrLineBytes {
			w.pending = w.pending[:0]
			w.omitted++
		} else {
			w.pending = append(w.pending, data[:newline]...)
			w.consumeLine(&forward)
		}
		data = data[newline+1:]
	}
	w.mu.Unlock()

	for _, line := range forward {
		if w.observation != nil {
			w.observation.forwardCSE(line)
		}
	}
	return written, nil
}

func (w *cseDumperStderr) consumeLine(forward *[]string) {
	line := bytes.TrimSuffix(w.pending, []byte{'\r'})
	w.pending = w.pending[:0]
	if len(line) == 0 {
		return
	}
	if isCSEPackedPerfLine(line) {
		*forward = append(*forward, string(line))
		return
	}

	lineBytes := len(line) + 1
	if lineBytes > maxCSEDumperDiagnosticBytes {
		w.omitted++
		return
	}
	for w.diagBytes+lineBytes > maxCSEDumperDiagnosticBytes && len(w.diagnosticLines) > 0 {
		w.diagBytes -= len(w.diagnosticLines[0]) + 1
		w.diagnosticLines = w.diagnosticLines[1:]
		w.omitted++
	}
	w.diagnosticLines = append(w.diagnosticLines, string(line))
	w.diagBytes += lineBytes
}

func (w *cseDumperStderr) finish() {
	forward := make([]string, 0, 1)
	w.mu.Lock()
	if !w.finished {
		w.finished = true
		if w.dropping {
			w.dropping = false
			w.omitted++
		} else if len(w.pending) > 0 {
			w.consumeLine(&forward)
		}
	}
	w.mu.Unlock()
	for _, line := range forward {
		if w.observation != nil {
			w.observation.forwardCSE(line)
		}
	}
}

func isCSEPackedPerfLine(line []byte) bool {
	return bytes.HasPrefix(line, []byte(csePackedPerfPrefix))
}

func (w *cseDumperStderr) diagnostics() string {
	w.finish()
	w.mu.Lock()
	defer w.mu.Unlock()
	lines := make([]string, 0, len(w.diagnosticLines)+1)
	if w.omitted > 0 {
		lines = append(lines, fmt.Sprintf("%d cse-ctl diagnostic lines omitted", w.omitted))
	}
	lines = append(lines, w.diagnosticLines...)
	return strings.Join(lines, "\n")
}
