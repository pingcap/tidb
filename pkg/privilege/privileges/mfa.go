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

package privileges

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// DecodeMultiFactorAuth decodes the MySQL-compatible MFA section of
// User_attributes. A missing section means that the account has no additional
// factors. The returned error is deliberately independent of the account name
// or authentication string so malformed metadata cannot leak credentials.
func DecodeMultiFactorAuth(attributes types.BinaryJSON) ([]AuthFactor, error) {
	path, err := types.ParseJSONPathExpr("$.multi_factor_authentication")
	if err != nil {
		return nil, err
	}
	mfa, found := attributes.Extract([]types.JSONPathExpression{path})
	if !found {
		return nil, nil
	}
	if mfa.TypeCode != types.JSONTypeCodeArray {
		return nil, fmt.Errorf("multi_factor_authentication must be an array")
	}
	mfaBytes, err := mfa.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var factors []AuthFactor
	if err := json.Unmarshal(mfaBytes, &factors); err != nil {
		return nil, fmt.Errorf("invalid multi_factor_authentication: %w", err)
	}
	if len(factors) > 2 {
		return nil, fmt.Errorf("multi_factor_authentication has too many factors")
	}
	for _, factor := range factors {
		if factor.Plugin == "" || !strings.EqualFold(factor.Plugin, mysql.AuthLDAPSimple) ||
			factor.RequiresRegistration != 0 || factor.Passwordless != 0 {
			return nil, fmt.Errorf("unsupported multi_factor_authentication factor")
		}
	}
	return factors, nil
}
