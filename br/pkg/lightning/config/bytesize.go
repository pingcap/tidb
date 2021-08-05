// Copyright 2020 PingCAP, Inc.
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

package config

import (
	"encoding/json"

	"github.com/docker/go-units"
)

// ByteSize is an alias of int64 which accepts human-friendly strings like
// '10G' when read from TOML.
type ByteSize int64

// UnmarshalText implements encoding.TextUnmarshaler
func (size *ByteSize) UnmarshalText(b []byte) error {
	res, err := units.RAMInBytes(string(b))
	if err != nil {
		return err
	}
	*size = ByteSize(res)
	return nil
}

// UnmarshalJSON implements json.Unmarshaler (for testing)
func (size *ByteSize) UnmarshalJSON(b []byte) error {
	var res int64
	if err := json.Unmarshal(b, &res); err != nil {
		return err
	}
	*size = ByteSize(res)
	return nil
}
