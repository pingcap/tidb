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

package config

import "context"

type ftsQueryContextKey struct{}

// WithFTSQuery marks the scan for TiFlash worker discovery.
func WithFTSQuery(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, ftsQueryContextKey{}, enabled)
}

// IsFTSQuery reports whether the current scan executes full-text search.
func IsFTSQuery(ctx context.Context) bool {
	enabled, _ := ctx.Value(ftsQueryContextKey{}).(bool)
	return enabled
}
