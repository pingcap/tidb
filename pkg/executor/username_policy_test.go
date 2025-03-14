// Copyright 2024 PingCAP, Inc.
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

package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/username"
)

func TestUsernamePolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	username.SetUsernamePolicy(NewPrefixPolicy("test_"))
	defer username.SetUsernamePolicy(username.NewDefaultUsernamePolicy())

	tk.MustExec("CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'password123'")
	tk.MustGetErrMsg("CREATE USER 'invalid_user'@'localhost' IDENTIFIED BY 'password123'", "username must have a prefix")

	tk.MustExec("ALTER USER 'test_user'@'localhost' IDENTIFIED BY 'newpassword'")

	// auto add prefix
	tk.MustExec("ALTER USER 'user'@'localhost' IDENTIFIED BY 'newpassword'")
}

type prefixPolicy struct {
	userPrefix string
}

// NewPrefixPolicy creates a new policy that requires the username to have a specific prefix.
func NewPrefixPolicy(userPrefix string) username.Policy {
	return &prefixPolicy{userPrefix: userPrefix}
}

func (p prefixPolicy) ValidateUsernameFormat(username string) bool {
	return strings.HasPrefix(username, p.userPrefix)
}

func (p prefixPolicy) ValidateUsername(username string) error {
	if p.userPrefix != "" && !strings.HasPrefix(username, p.userPrefix) {
		return errors.New("username must have a prefix")
	}
	return nil
}

func (p prefixPolicy) GetUsernameVariants(username string) []string {
	if p.userPrefix != "" {
		return []string{p.userPrefix + username}
	}
	return nil
}

func (p prefixPolicy) GetOriginalUsername(username string) string {
	if p.userPrefix != "" && strings.HasPrefix(username, p.userPrefix) {
		return username[len(p.userPrefix):]
	}
	return ""
}
