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

package configtypes

import (
	"fmt"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
)

// ByteSize is a retype uint64 for TOML and JSON.
type ByteSize uint64

// MarshalJSON returns the size as a JSON string.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(`"` + units.BytesSize(float64(b)) + `"`), nil
}

// MarshalText returns the size as a TOML string.
func (b ByteSize) MarshalText() ([]byte, error) {
	return []byte(units.BytesSize(float64(b))), nil
}

// UnmarshalJSON parses a JSON string into the byte size.
func (b *ByteSize) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	v, err := units.RAMInBytes(s)
	if err != nil {
		return errors.WithStack(err)
	}
	*b = ByteSize(v)
	return nil
}

// UnmarshalText parses a Toml string into the byte size.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := units.RAMInBytes(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	*b = ByteSize(v)
	return nil
}

// Duration is a wrapper of time.Duration for TOML and JSON.
type Duration struct {
	time.Duration
}

// MarshalJSON returns the duration as a JSON string.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, d.String()), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return errors.WithStack(err)
	}
	d.Duration = duration
	return nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return errors.WithStack(err)
}

// MarshalText returns the duration as a TOML string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}
