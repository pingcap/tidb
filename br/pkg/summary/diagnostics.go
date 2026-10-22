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
	"sort"
	"sync"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/zap"
)

const (
	maxObservedErrors    = 64
	maxErrorSampleLength = 4096
)

type errorObservationKey struct {
	category  berrors.Category
	component string
	operation string
	storeID   uint64
}

type errorObservation struct {
	Category  berrors.Category `json:"category"`
	Component string           `json:"component"`
	Operation string           `json:"operation"`
	StoreID   uint64           `json:"store-id,omitempty"`
	Code      string           `json:"error-code,omitempty"`
	Count     uint64           `json:"count"`
	Sample    string           `json:"sample-error"`
}

type errorRecorder struct {
	mu           sync.Mutex
	observations map[errorObservationKey]errorObservation
	omitted      uint64
}

var observedErrors = &errorRecorder{}

func (r *errorRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations = nil
	r.omitted = 0
}

// ObserveError records a diagnostic sample before retries or cancellation can
// obscure it. It does not affect the error returned or the retry policy.
func ObserveError(component, operation string, storeID uint64, err error) {
	observedErrors.observe(component, operation, storeID, err)
}

func (r *errorRecorder) observe(component, operation string, storeID uint64, err error) {
	if err == nil {
		return
	}
	classification := berrors.Classify(err)
	// Sibling goroutines routinely cancel each other after a meaningful failure.
	// The command's final cancellation is still reported by FailureFields.
	if classification.Category == berrors.CategoryCanceled {
		return
	}
	key := errorObservationKey{classification.Category, component, operation, storeID}
	r.mu.Lock()
	defer r.mu.Unlock()
	if observation, ok := r.observations[key]; ok {
		observation.Count++
		r.observations[key] = observation
		return
	}
	if len(r.observations) >= maxObservedErrors {
		r.omitted++
		return
	}
	if r.observations == nil {
		r.observations = make(map[errorObservationKey]errorObservation)
	}
	sample := err.Error()
	if len(sample) > maxErrorSampleLength {
		sample = sample[:maxErrorSampleLength] + "..."
	}
	r.observations[key] = errorObservation{
		Category:  classification.Category,
		Component: component,
		Operation: operation,
		StoreID:   storeID,
		Code:      classification.Code,
		Count:     1,
		Sample:    sample,
	}
}

// FailureFields describes the final error and, separately, errors observed
// during the task. An earlier retried error is not necessarily its final cause.
// redactText sanitizes error text for output without changing the original
// errors. A nil sanitizer suppresses error details.
func FailureFields(err error, redactText func(string) string) []zap.Field {
	return observedErrors.fields(err, redactText)
}

func redactDiagnosticText(text string, redactText func(string) string) string {
	if redactText == nil {
		return "(error details redacted)"
	}
	return redactText(text)
}

func (r *errorRecorder) fields(err error, redactText func(string) string) []zap.Field {
	classification := berrors.Classify(err)
	fields := []zap.Field{
		zap.String("error-category", string(classification.Category)),
	}
	if err != nil {
		// zap.Error can emit a second, verbose representation of an error that
		// would bypass the text sanitizer.
		fields = append(fields, zap.String("error", redactDiagnosticText(err.Error(), redactText)))
	}
	if classification.Code != "" {
		fields = append(fields, zap.String("error-code", classification.Code))
	}
	r.mu.Lock()
	observations := make([]errorObservation, 0, len(r.observations))
	for _, observation := range r.observations {
		observations = append(observations, observation)
	}
	omitted := r.omitted
	r.mu.Unlock()
	for i := range observations {
		sample := redactDiagnosticText(observations[i].Sample, redactText)
		if len(sample) > maxErrorSampleLength {
			sample = sample[:maxErrorSampleLength] + "..."
		}
		observations[i].Sample = sample
	}
	sort.Slice(observations, func(i, j int) bool {
		a, b := observations[i], observations[j]
		if a.Category != b.Category {
			return a.Category < b.Category
		}
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		return a.StoreID < b.StoreID
	})
	if len(observations) > 0 {
		fields = append(fields, zap.Any("observed-errors", observations))
	}
	if omitted > 0 {
		fields = append(fields, zap.Uint64("omitted-error-observations", omitted))
	}
	return fields
}
