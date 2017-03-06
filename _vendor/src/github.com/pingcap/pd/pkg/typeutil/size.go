// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"strconv"

	gh "github.com/dustin/go-humanize"
	"github.com/juju/errors"
)

// ByteSize is a retype uint64 for TOML and JSON.
type ByteSize uint64

// MarshalJSON returns the size as a JSON string.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(`"` + gh.Bytes(uint64(b)) + `"`), nil
}

// UnmarshalJSON parses a JSON string into the bytesize.
func (b *ByteSize) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	v, err := gh.ParseBytes(s)
	if err != nil {
		return errors.Trace(err)
	}
	*b = ByteSize(v)
	return nil
}

// UnmarshalText parses a Toml string into the bytesize.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := gh.ParseBytes(string(text))
	if err != nil {
		return errors.Trace(err)
	}
	*b = ByteSize(v)
	return nil
}
