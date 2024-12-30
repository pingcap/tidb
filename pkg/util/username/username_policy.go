// Copyright 2024 PingCAP, Inc.
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

package username

// UsernamePolicy is the interface for username policy.
type UsernamePolicy interface {
	// ValidateUsername checks if the username is valid.
	ValidateUsername(username string) error
	// ValidateUsernameFormat checks if the username is in the correct format.
	ValidateUsernameFormat(username string) bool
	// GetUsernameVariants returns a list of possible username variants for the given username.
	GetUsernameVariants(username string) []string
	// GetOriginalUsername returns the original username from the given username.
	GetOriginalUsername(username string) string
}

var globalUsernamePolicy = NewDefaultUsernamePolicy()

// SetUsernamePolicy sets the global username policy.
func SetUsernamePolicy(policy UsernamePolicy) {
	globalUsernamePolicy = policy
}

// GetUsernamePolicy returns the global username policy.
func GetUsernamePolicy() UsernamePolicy {
	return globalUsernamePolicy
}

type defaultUsernamePolicy struct{}

// NewDefaultUsernamePolicy creates a new default username policy.
func NewDefaultUsernamePolicy() UsernamePolicy {
	return &defaultUsernamePolicy{}
}

func (*defaultUsernamePolicy) ValidateUsername(_ string) error {
	return nil
}

func (*defaultUsernamePolicy) ValidateUsernameFormat(_ string) bool {
	return true
}

func (*defaultUsernamePolicy) GetUsernameVariants(_ string) []string {
	return nil
}

func (*defaultUsernamePolicy) GetOriginalUsername(_ string) string {
	return ""
}
