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

package vertex

import (
	"context"
	"os"

	"github.com/pingcap/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// NewTokenSource creates a token source using ADC or a service account JSON file.
func NewTokenSource(ctx context.Context, credentialFile string) (oauth2.TokenSource, error) {
	if credentialFile == "" {
		return google.DefaultTokenSource(ctx, cloudPlatformScope)
	}
	data, err := os.ReadFile(credentialFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	creds, err := google.CredentialsFromJSON(ctx, data, cloudPlatformScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return creds.TokenSource, nil
}
