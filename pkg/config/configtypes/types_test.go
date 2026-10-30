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
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

type byteSizeConfig struct {
	Size ByteSize `json:"size" toml:"size"`
}

type durationConfig struct {
	Duration Duration `json:"duration" toml:"duration"`
}

func TestByteSize(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		var cfg byteSizeConfig
		err := json.Unmarshal([]byte(`{"size":"1MiB"}`), &cfg)
		require.NoError(t, err)
		require.Equal(t, ByteSize(1024*1024), cfg.Size)

		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.Equal(t, `{"size":"1MiB"}`, string(data))
	})

	t.Run("toml", func(t *testing.T) {
		var cfg byteSizeConfig
		_, err := toml.Decode(`size = "512KiB"`, &cfg)
		require.NoError(t, err)
		require.Equal(t, ByteSize(512*1024), cfg.Size)

		var buf bytes.Buffer
		require.NoError(t, toml.NewEncoder(&buf).Encode(cfg))
		require.Equal(t, "size = \"512KiB\"\n", buf.String())
	})
}

func TestDuration(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		duration := Duration{Duration: time.Hour + 2*time.Minute + 3*time.Second}
		cfg := durationConfig{Duration: duration}
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.Equal(t, `{"duration":"1h2m3s"}`, string(data))

		var decoded durationConfig
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, duration.Duration, decoded.Duration.Duration)
	})

	t.Run("toml", func(t *testing.T) {
		var cfg durationConfig
		_, err := toml.Decode(`duration = "2m3s"`, &cfg)
		require.NoError(t, err)
		require.Equal(t, 2*time.Minute+3*time.Second, cfg.Duration.Duration)

		var buf bytes.Buffer
		require.NoError(t, toml.NewEncoder(&buf).Encode(cfg))
		require.Equal(t, "duration = \"2m3s\"\n", buf.String())
	})
}
