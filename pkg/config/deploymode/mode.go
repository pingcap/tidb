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

package deploymode

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
)

const (
	premiumName         = "premium"
	premiumReservedName = "premium_reserved"
	starterName         = "starter"
)

// Mode is the deployment mode of the TiDB instance. It is only allowed when
// kerneltype.IsNextGen returns true.
type Mode int32

const (
	// Premium is the default deployment mode.
	Premium Mode = iota
	// PremiumReserved is the reserved premium deployment mode. In Premium Reserved,
	// resources are fixed when the cluster starts. TiDB-worker, TiKV-worker, and
	// coprocessor-worker are not scaled on demand.
	PremiumReserved
	// Starter is for deployments that support a large number of small tenants.
	Starter
)

var currentMode atomic.Int32

// Get returns the current deployment mode.
func Get() Mode {
	return Mode(currentMode.Load())
}

// IsPremiumReserved returns true if the current deployment mode is PremiumReserved.
func IsPremiumReserved() bool {
	return kerneltype.IsNextGen() && Get() == PremiumReserved
}

// IsStarter returns true if the current deployment mode is Starter.
func IsStarter() bool {
	return kerneltype.IsNextGen() && Get() == Starter
}

// Set sets the current deployment mode during TiDB startup.
//
// The deployment mode cannot be changed after it is set.
func Set(mode Mode) error {
	if !kerneltype.IsNextGen() {
		return fmt.Errorf("deploy mode can only be set for nextgen TiDB")
	}
	if !mode.Valid() {
		return fmt.Errorf("invalid deploy mode %d", mode)
	}
	currentMode.Store(int32(mode))
	return nil
}

// Parse returns the deployment mode for the given string.
func Parse(s string) (Mode, error) {
	switch strings.ToLower(s) {
	case premiumName:
		return Premium, nil
	case premiumReservedName:
		return PremiumReserved, nil
	case starterName:
		return Starter, nil
	default:
		return Premium, fmt.Errorf("invalid deploy mode %q", s)
	}
}

// String returns the string representation of the deployment mode.
func (m Mode) String() string {
	switch m {
	case Premium:
		return premiumName
	case PremiumReserved:
		return premiumReservedName
	case Starter:
		return starterName
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// Valid returns true if the deployment mode is valid.
func (m Mode) Valid() bool {
	switch m {
	case Premium, PremiumReserved, Starter:
		return true
	default:
		return false
	}
}

// ModeList returns all valid deployment modes.
func ModeList() []Mode {
	return []Mode{Premium, PremiumReserved, Starter}
}

// MarshalJSON implements json.Marshaler.
func (m Mode) MarshalJSON() ([]byte, error) {
	if !m.Valid() {
		return nil, fmt.Errorf("invalid deploy mode %d", m)
	}
	return json.Marshal(m.String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *Mode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	mode, err := Parse(s)
	if err != nil {
		return err
	}
	*m = mode
	return nil
}

// UnmarshalTOML implements toml.Unmarshaler.
func (m *Mode) UnmarshalTOML(v any) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("invalid deploy mode %v", v)
	}
	mode, err := Parse(s)
	if err != nil {
		return err
	}
	*m = mode
	return nil
}
