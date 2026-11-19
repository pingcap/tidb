// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package privileges

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDecodeMultiFactorAuth(t *testing.T) {
	valid, err := types.ParseBinaryJSONFromString(`{"multi_factor_authentication":[{"plugin":"authentication_ldap_simple","authentication_string":"uid=second","requires_registration":0,"passwordless":0}]}`)
	require.NoError(t, err)
	factors, err := DecodeMultiFactorAuth(valid)
	require.NoError(t, err)
	require.Equal(t, []AuthFactor{{Plugin: "authentication_ldap_simple", AuthenticationString: "uid=second"}}, factors)

	malformed, err := types.ParseBinaryJSONFromString(`{"multi_factor_authentication":{"plugin":"authentication_ldap_simple"}}`)
	require.NoError(t, err)
	_, err = DecodeMultiFactorAuth(malformed)
	require.Error(t, err)

	missing, err := types.ParseBinaryJSONFromString(`{"metadata":{"email":"user@example.com"}}`)
	require.NoError(t, err)
	factors, err = DecodeMultiFactorAuth(missing)
	require.NoError(t, err)
	require.Empty(t, factors)
}
