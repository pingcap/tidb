// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schstatus

import "encoding/json"

const (
	// MinAmplifyFactor is the minimum amplify factor.
	MinAmplifyFactor = 1.0
	// MaxAmplifyFactor is the maximum amplify factor.
	MaxAmplifyFactor     = 10.0
	defaultAmplifyFactor = MinAmplifyFactor
)

// TuneFactors represents the resource tuning factors.
type TuneFactors struct {
	// AmplifyFactor is used to amplify the input data size and node count limit,
	// through this, we can amplify the calculated resource for new tasks.
	AmplifyFactor float64 `json:"amplify_factor,omitempty"`
}

// TTLTuneFactors represents the TTL info and tuning factors.
// only used for storage.
type TTLTuneFactors struct {
	TTLInfo
	TuneFactors
}

// String implements the fmt.Stringer interface.
func (f *TTLTuneFactors) String() string {
	bytes, _ := json.Marshal(f)
	return string(bytes)
}

// GetDefaultTuneFactors get the default tuning factors.
func GetDefaultTuneFactors() *TuneFactors {
	return &TuneFactors{
		AmplifyFactor: defaultAmplifyFactor,
	}
}
