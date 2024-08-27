// Copyright 2021 PingCAP, Inc.
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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrivString(t *testing.T) {
	for i := 0; ; i++ {
		p := PrivilegeType(1 << i)
		if p > AllPriv {
			break
		}
		require.NotEqualf(t, "", p.String(), "%d-th", i)
	}
}

func TestPrivColumn(t *testing.T) {
	for _, p := range AllGlobalPrivs {
		require.NotEmptyf(t, p.ColumnString(), "%s", p)
		np, ok := NewPrivFromColumn(p.ColumnString())
		require.Truef(t, ok, "%s", p)
		require.Equal(t, p, np)
	}
	for _, p := range StaticGlobalOnlyPrivs {
		require.NotEmptyf(t, p.ColumnString(), "%s", p)
		np, ok := NewPrivFromColumn(p.ColumnString())
		require.Truef(t, ok, "%s", p)
		require.Equal(t, p, np)
	}
	for _, p := range AllDBPrivs {
		require.NotEmptyf(t, p.ColumnString(), "%s", p)
		np, ok := NewPrivFromColumn(p.ColumnString())
		require.Truef(t, ok, "%s", p)
		require.Equal(t, p, np)
	}
}

func TestPrivSetString(t *testing.T) {
	for _, p := range AllTablePrivs {
		require.NotEmptyf(t, p.SetString(), "%s", p)
		np, ok := NewPrivFromSetEnum(p.SetString())
		require.Truef(t, ok, "%s", p)
		require.Equal(t, p, np)
	}
	for _, p := range AllColumnPrivs {
		require.NotEmptyf(t, p.SetString(), "%s", p)
		np, ok := NewPrivFromSetEnum(p.SetString())
		require.Truef(t, ok, "%s", p)
		require.Equal(t, p, np)
	}
}

func TestPrivsHas(t *testing.T) {
	// it is a simple helper, does not handle all&dynamic privs
	privs := Privileges{AllPriv}
	require.True(t, privs.Has(AllPriv))
	require.False(t, privs.Has(InsertPriv))

	// multiple privs
	privs = Privileges{InsertPriv, SelectPriv}
	require.True(t, privs.Has(SelectPriv))
	require.True(t, privs.Has(InsertPriv))
	require.False(t, privs.Has(DropPriv))
}

func TestPrivAllConsistency(t *testing.T) {
	// AllPriv in mysql.user columns.
	for priv := CreatePriv; priv != AllPriv; priv = priv << 1 {
		_, ok := Priv2UserCol[priv]
		require.Truef(t, ok, "priv fail %d", priv)
	}

	require.Equal(t, len(AllGlobalPrivs)+1, len(Priv2UserCol))

	// USAGE privilege doesn't have a column in Priv2UserCol
	// ALL privilege doesn't have a column in Priv2UserCol
	// so it's +2
	require.Equal(t, len(Priv2UserCol)+2, len(Priv2Str))
}
