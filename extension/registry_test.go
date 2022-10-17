// Copyright 2022 PingCAP, Inc.
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

package extension_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestSetupExtensions(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	extension.Reset()
	require.NoError(t, extension.Setup())
	extensions, err := extension.GetExtensions()
	require.NoError(t, err)
	require.Equal(t, 0, len(extensions.Manifests()))

	extension.Reset()
	require.NoError(t, extension.Register("test1"))
	require.NoError(t, extension.Register("test2"))
	require.NoError(t, extension.Setup())
	extensions, err = extension.GetExtensions()
	require.NoError(t, err)
	require.Equal(t, 2, len(extensions.Manifests()))
	require.Equal(t, "test1", extensions.Manifests()[0].Name())
	require.Equal(t, "test2", extensions.Manifests()[1].Name())
}

func TestExtensionRegisterName(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	// test empty name
	extension.Reset()
	require.EqualError(t, extension.Register(""), "extension name should not be empty")

	// test dup name
	extension.Reset()
	require.NoError(t, extension.Register("test"))
	require.EqualError(t, extension.Register("test"), "extension with name 'test' already registered")
}

func TestRegisterExtensionWithClose(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	// normal register
	extension.Reset()
	cnt := 0
	require.NoError(t, extension.Register("test1", extension.WithClose(func() {
		cnt++
	})))
	require.NoError(t, extension.Setup())
	require.Equal(t, 0, cnt)

	// reset will call close
	extension.Reset()
	require.Equal(t, 1, cnt)

	// reset again has no effect
	extension.Reset()
	require.Equal(t, 1, cnt)

	// Auto close when error
	cnt = 0
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithClose(func() {
		cnt++
	})))
	require.NoError(t, extension.RegisterFactory("test2", func() ([]extension.Option, error) {
		return nil, errors.New("error abc")
	}))
	require.EqualError(t, extension.Setup(), "error abc")
	require.Equal(t, 1, cnt)
}

func TestRegisterExtensionWithDyncPrivs(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	origDynPrivs := privileges.GetDynamicPrivileges()
	origDynPrivs = append([]string{}, origDynPrivs...)

	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", "priv2"})))
	require.NoError(t, extension.Setup())
	privs := privileges.GetDynamicPrivileges()
	require.Equal(t, origDynPrivs, privs[:len(origDynPrivs)])
	require.Equal(t, []string{"PRIV1", "PRIV2"}, privs[len(origDynPrivs):])

	// test for empty dynamic privilege name
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", ""})))
	require.EqualError(t, extension.Setup(), "privilege name should not be empty")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())

	// test for duplicate name with builtin
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", "ROLE_ADMIN"})))
	require.EqualError(t, extension.Setup(), "privilege is already registered")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())

	// test for duplicate name with other extension
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithCustomDynPrivs([]string{"priv1"})))
	require.NoError(t, extension.Register("test2", extension.WithCustomDynPrivs([]string{"priv2", "priv1"})))
	require.EqualError(t, extension.Setup(), "privilege is already registered")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())
}

func TestRegisterExtensionWithSysVars(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	sysVar1 := &variable.SysVar{
		Scope: variable.ScopeGlobal | variable.ScopeSession,
		Name:  "var1",
		Value: variable.On,
		Type:  variable.TypeBool,
	}

	sysVar2 := &variable.SysVar{
		Scope: variable.ScopeSession,
		Name:  "var2",
		Value: "val2",
		Type:  variable.TypeStr,
	}

	// normal register
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1, sysVar2})))
	require.NoError(t, extension.Setup())
	require.Same(t, sysVar1, variable.GetSysVar("var1"))
	require.Same(t, sysVar2, variable.GetSysVar("var2"))

	// test for empty name
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{
		{Scope: variable.ScopeGlobal, Name: "", Value: "val3"},
	})))
	require.EqualError(t, extension.Setup(), "system var name should not be empty")
	require.Nil(t, variable.GetSysVar(""))

	// test for duplicate name with builtin
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{
		sysVar1,
		{Scope: variable.ScopeGlobal, Name: variable.TiDBSnapshot, Value: "val3"},
	})))
	require.EqualError(t, extension.Setup(), "system var 'tidb_snapshot' has already registered")
	require.Nil(t, variable.GetSysVar("var1"))
	require.Equal(t, "", variable.GetSysVar(variable.TiDBSnapshot).Value)
	require.Equal(t, variable.ScopeSession, variable.GetSysVar(variable.TiDBSnapshot).Scope)

	// test for duplicate name with other extension
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1, sysVar2})))
	require.NoError(t, extension.Register("test2", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1})))
	require.EqualError(t, extension.Setup(), "system var 'var1' has already registered")
	require.Nil(t, variable.GetSysVar("var1"))
	require.Nil(t, variable.GetSysVar("var2"))
}
