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

package summary

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// retainDiagnosticText lets tests inspect samples through the sanitizer callback.
func retainDiagnosticText(text string) string { return text }

// decodeFailureFields exercises JSON log encoding to check the emitted diagnostics.
func decodeFailureFields(t *testing.T, fields []zap.Field) map[string]any {
	t.Helper()
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	buffer, err := encoder.EncodeEntry(zapcore.Entry{Message: "br failure summary"}, fields)
	require.NoError(t, err)
	defer buffer.Free()
	var result map[string]any
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &result))
	return result
}

// memoryPressureError models the TiKV download failure that must survive cancellation.
func memoryPressureError() error {
	return fmt.Errorf("resource is not enough Memory usage too high, usage: 4111917056 bytes, mem limit 4294967296 bytes: %w",
		berrors.ErrKVDownloadFailed)
}

func TestObservedErrorsSurviveCancellation(t *testing.T) {
	r := &errorRecorder{}
	err := memoryPressureError()
	r.observe("tikv", "download_sst", 7, err)
	r.observe("tikv", "download_sst", 7, err)
	r.observe("tikv", "download_sst", 8, context.Canceled)
	r.observe("tikv", "download_sst", 8, nil)

	report := decodeFailureFields(t, r.fields(context.Canceled, retainDiagnosticText))
	require.Equal(t, "canceled", report["error-category"])
	require.Equal(t, "context canceled", report["error"])
	observations := report["observed-errors"].([]any)
	require.Len(t, observations, 1)
	observation := observations[0].(map[string]any)
	require.Equal(t, "tikv_memory_pressure", observation["category"])
	require.Equal(t, "BR:KV:ErrKVDownloadFailed", observation["error-code"])
	require.Equal(t, "tikv", observation["component"])
	require.Equal(t, "download_sst", observation["operation"])
	require.Equal(t, float64(7), observation["store-id"])
	require.Equal(t, float64(2), observation["count"])
	require.Equal(t, err.Error(), observation["sample-error"])
}

func TestObservedErrorsDoNotReplaceFinalCause(t *testing.T) {
	r := &errorRecorder{}
	r.observe("tikv", "download_sst", 1, memoryPressureError())
	report := decodeFailureFields(t, r.fields(berrors.ErrInvalidArgument, retainDiagnosticText))
	require.Equal(t, "configuration", report["error-category"])
	require.Equal(t, "BR:Common:ErrInvalidArgument", report["error-code"])
	require.Equal(t, berrors.ErrInvalidArgument.Error(), report["error"])
	require.Len(t, report["observed-errors"], 1)
}

func TestErrorObservationBoundsAndReset(t *testing.T) {
	r := &errorRecorder{}
	longError := fmt.Errorf("%s", strings.Repeat("x", maxErrorSampleLength+100))
	for storeID := range uint64(maxObservedErrors + 2) {
		r.observe("tikv", "backup", storeID, longError)
	}
	r.observe("tikv", "backup", 0, fmt.Errorf("another error"))
	report := decodeFailureFields(t, r.fields(context.DeadlineExceeded, retainDiagnosticText))
	require.Equal(t, float64(2), report["omitted-error-observations"])
	observations := report["observed-errors"].([]any)
	require.Len(t, observations, maxObservedErrors)
	first := observations[0].(map[string]any)
	require.Equal(t, float64(2), first["count"])
	require.Len(t, first["sample-error"], maxErrorSampleLength+3)
	require.Equal(t, float64(1), observations[1].(map[string]any)["store-id"])

	r.reset()
	report = decodeFailureFields(t, r.fields(context.DeadlineExceeded, retainDiagnosticText))
	require.NotContains(t, report, "observed-errors")
	require.NotContains(t, report, "omitted-error-observations")
}

func TestErrorObservationSampleTruncation(t *testing.T) {
	for _, character := range []string{"é", "界", "🙂"} {
		t.Run(character, func(t *testing.T) {
			for _, extra := range []string{"", "tail"} {
				t.Run(fmt.Sprintf("extra=%q", extra), func(t *testing.T) {
					prefix := strings.Repeat("x", maxErrorSampleLength-len(character))
					sample := prefix + character + extra
					want := prefix + character
					if extra != "" {
						// Make the byte limit fall within the final character.
						prefix += "x"
						sample = prefix + character + extra
						want = prefix + "..."
					}
					r := &errorRecorder{}
					r.observe("tikv", "backup", 1, fmt.Errorf("%s", sample))
					for _, observation := range r.observations {
						require.True(t, utf8.ValidString(observation.Sample))
						require.Equal(t, want, observation.Sample)
					}
					report := decodeFailureFields(t, r.fields(context.Canceled, retainDiagnosticText))
					observations := report["observed-errors"].([]any)
					require.Equal(t, want, observations[0].(map[string]any)["sample-error"])
				})
			}
		})
	}
}

func TestErrorObservationSanitizerExpansion(t *testing.T) {
	for _, test := range []struct {
		name     string
		redacted string
		want     string
	}{
		{
			name:     "within stored sample bound",
			redacted: strings.Repeat("x", maxErrorSampleLength+3),
			want:     strings.Repeat("x", maxErrorSampleLength+3),
		},
		{
			name:     "expands across rune boundary",
			redacted: strings.Repeat("x", maxErrorSampleLength-1) + "界tail",
			want:     strings.Repeat("x", maxErrorSampleLength-1) + "...",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &errorRecorder{}
			r.observe("tikv", "backup", 1, fmt.Errorf("short sample"))
			report := decodeFailureFields(t, r.fields(context.Canceled, func(string) string { return test.redacted }))
			observations := report["observed-errors"].([]any)
			require.Equal(t, test.want, observations[0].(map[string]any)["sample-error"])
			for _, observation := range r.observations {
				require.Equal(t, "short sample", observation.Sample)
			}
		})
	}
}

func TestConcurrentErrorObservations(t *testing.T) {
	r := &errorRecorder{}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			r.observe("tikv", "download_sst", 1, memoryPressureError())
		})
	}
	wg.Wait()
	report := decodeFailureFields(t, r.fields(memoryPressureError(), retainDiagnosticText))
	observations := report["observed-errors"].([]any)
	require.Len(t, observations, 1)
	require.Equal(t, float64(100), observations[0].(map[string]any)["count"])
}

type verboseDiagnosticError struct{}

func (verboseDiagnosticError) Error() string { return "original-secret" }

func (verboseDiagnosticError) Format(state fmt.State, _ rune) {
	fmt.Fprint(state, "verbose-secret")
}

func TestFailureFieldsSanitizeAllErrorRepresentations(t *testing.T) {
	r := &errorRecorder{}
	err := verboseDiagnosticError{}
	r.observe("tikv", "download_sst", 7, err)
	for _, redactText := range []func(string) string{
		func(string) string { return "(redacted)" },
		nil,
	} {
		report := decodeFailureFields(t, r.fields(err, redactText))
		require.NotContains(t, report, "errorVerbose")
		encoded, encodeErr := json.Marshal(report)
		require.NoError(t, encodeErr)
		require.NotContains(t, string(encoded), "original-secret")
		require.NotContains(t, string(encoded), "verbose-secret")
	}
	// Rendering a redacted report must leave retained samples and errors intact.
	report := decodeFailureFields(t, r.fields(err, retainDiagnosticText))
	observations := report["observed-errors"].([]any)
	require.Equal(t, "original-secret", report["error"])
	require.Equal(t, "original-secret", observations[0].(map[string]any)["sample-error"])
	require.Equal(t, "original-secret", err.Error())
}
