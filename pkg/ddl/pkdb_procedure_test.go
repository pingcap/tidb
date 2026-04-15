// Copyright 2025 PingCAP, Inc.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockPrivManager struct {
	tmock.Mock
	privilege.Manager
}

func (m *mockPrivManager) RequestVerification(
	activeRole []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType,
) bool {
	return m.Called(activeRole, db, table, column, priv).Bool(0)
}

func TestCheckRoutineDefinerPrivilege(t *testing.T) {
	// This test focuses on parsing logic: splitting "user@host" on the LAST '@'.
	// MySQL stores definer as "user@host" string. Since usernames can contain '@',
	// we must split on the LAST '@' to handle cases like "user@domain@localhost".
	tests := []struct {
		name         string
		definer      string
		currentUser  string
		currentHost  string
		expectError  bool
		errorIs      error
		hasSuperPriv *bool // nil = no priv check, true = has SUPER, false = no SUPER
	}{
		{
			name:        "empty definer",
			definer:     "",
			currentUser: "root",
			currentHost: "localhost",
			expectError: false,
		},
		{
			name:        "same user and host",
			definer:     "root@localhost",
			currentUser: "root",
			currentHost: "localhost",
			expectError: false,
		},
		{
			name:         "different user - has SUPER privilege",
			definer:      "admin@localhost",
			currentUser:  "root",
			currentHost:  "localhost",
			expectError:  false,
			hasSuperPriv: boolPtr(true),
		},
		{
			name:         "different user - no SUPER privilege",
			definer:      "admin@localhost",
			currentUser:  "root",
			currentHost:  "localhost",
			expectError:  true,
			errorIs:      exeerrors.ErrSpecificAccessDenied,
			hasSuperPriv: boolPtr(false),
		},
		{
			name:         "different host - has SUPER privilege",
			definer:      "root@127.0.0.1",
			currentUser:  "root",
			currentHost:  "localhost",
			expectError:  false,
			hasSuperPriv: boolPtr(true),
		},
		{
			name:         "different host - no SUPER privilege",
			definer:      "root@127.0.0.1",
			currentUser:  "root",
			currentHost:  "localhost",
			expectError:  true,
			errorIs:      exeerrors.ErrSpecificAccessDenied,
			hasSuperPriv: boolPtr(false),
		},
		{
			name:        "username with @ symbol",
			definer:     "user@domain@localhost",
			currentUser: "user@domain",
			currentHost: "localhost",
			expectError: false,
		},
		{
			name:        "username with multiple @ symbols",
			definer:     "user@domain@company@127.0.0.1",
			currentUser: "user@domain@company",
			currentHost: "127.0.0.1",
			expectError: false,
		},
		{
			name:         "username with @ symbol - different user - has SUPER",
			definer:      "user@domain@localhost",
			currentUser:  "user",
			currentHost:  "localhost",
			expectError:  false,
			hasSuperPriv: boolPtr(true),
		},
		{
			name:         "username with @ symbol - different user - no SUPER",
			definer:      "user@domain@localhost",
			currentUser:  "user",
			currentHost:  "localhost",
			expectError:  true,
			errorIs:      exeerrors.ErrSpecificAccessDenied,
			hasSuperPriv: boolPtr(false),
		},
		{
			name:        "no @ symbol - invalid format",
			definer:     "invalid_definer",
			currentUser: "root",
			currentHost: "localhost",
			expectError: true, // Parsing error: missing '@' separator
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := mock.NewContextDeprecated()
			ctx.GetSessionVars().User = &auth.UserIdentity{
				AuthUsername: tc.currentUser,
				AuthHostname: tc.currentHost,
			}

			// Set up privilege manager if needed
			if tc.hasSuperPriv != nil {
				mgr := &mockPrivManager{}
				privilege.BindPrivilegeManager(ctx, mgr)
				mgr.On("RequestVerification",
					ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv).
					Return(*tc.hasSuperPriv).Once()
			}

			err := checkRoutineDefinerPrivilege(ctx, tc.definer)
			if tc.expectError {
				require.Error(t, err)
				if tc.errorIs != nil {
					require.ErrorIs(t, err, tc.errorIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func TestCheckRoutineDefinerPrivilegeNilUser(t *testing.T) {
	ctx := mock.NewContextDeprecated()
	ctx.GetSessionVars().User = nil

	err := checkRoutineDefinerPrivilege(ctx, "root@localhost")
	require.NoError(t, err)
}
