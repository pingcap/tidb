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

package keyspace

import (
	"strings"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

// UsernamePolicy is the interface for username policy.
type UsernamePolicy interface {
	// ValidateUsername checks if the username is valid.
	ValidateUsername(username string) error
	// ValidateUsernameFormat checks if the username is in the expected format.
	ValidateUsernameFormat(username string) bool
	// GetUsernameVariants returns possible username variants for the given username.
	GetUsernameVariants(username string) []string
	// GetOriginalUsername returns the username with the current policy prefix removed.
	// It returns an empty string when the current policy does not transform the input.
	GetOriginalUsername(username string) string
}

// GetUsernamePolicy returns the username policy for the current deployment mode.
func GetUsernamePolicy() UsernamePolicy {
	if deploymode.IsStarter() {
		// An empty keyspace name keeps the policy permissive. Bootstrap and tests may
		// run before the keyspace name is configured.
		return prefixPolicy{userPrefix: GetKeyspaceNameBySettings()}
	}
	return defaultUsernamePolicy{}
}

type defaultUsernamePolicy struct{}

func (defaultUsernamePolicy) ValidateUsername(string) error {
	return nil
}

func (defaultUsernamePolicy) ValidateUsernameFormat(string) bool {
	return true
}

func (defaultUsernamePolicy) GetUsernameVariants(string) []string {
	return nil
}

func (defaultUsernamePolicy) GetOriginalUsername(string) string {
	return ""
}

type prefixPolicy struct {
	userPrefix string
}

func (prefixPolicy) ValidateUsernameFormat(username string) bool {
	return strings.Count(username, ".") == 1
}

func (p prefixPolicy) ValidateUsername(username string) error {
	if p.userPrefix != "" && !strings.HasPrefix(username, p.userPrefix+".") {
		return exeerrors.ErrUserNameNeedPrefix.GenWithStackByArgs(p.userPrefix, p.userPrefix, username)
	}
	return nil
}

func (p prefixPolicy) GetUsernameVariants(username string) []string {
	if p.userPrefix == "" || strings.HasPrefix(username, p.userPrefix+".") {
		return nil
	}
	return []string{p.userPrefix + "." + username}
}

func (p prefixPolicy) GetOriginalUsername(username string) string {
	if p.userPrefix != "" && strings.HasPrefix(username, p.userPrefix+".") {
		return username[len(p.userPrefix)+1:]
	}
	return ""
}
