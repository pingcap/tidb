// Copyright 2022 PingCAP, Inc.
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

package utils

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestIsTypeCompatible(t *testing.T) {
	{
		// different unsigned flag
		src := types.NewFieldType(mysql.TypeInt24)
		src.AddFlag(mysql.UnsignedFlag)
		target := types.NewFieldType(mysql.TypeInt24)
		require.False(t, IsTypeCompatible(*src, *target))

		src.DelFlag(mysql.UnsignedFlag)
		target.AddFlag(mysql.UnsignedFlag)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// different not null flag
		src := types.NewFieldType(mysql.TypeInt24)
		src.AddFlag(mysql.NotNullFlag)
		target := types.NewFieldType(mysql.TypeInt24)
		require.False(t, IsTypeCompatible(*src, *target))

		src.DelFlag(mysql.NotNullFlag)
		target.AddFlag(mysql.NotNullFlag)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// different evaluation type
		src := types.NewFieldType(mysql.TypeInt24)
		target := types.NewFieldType(mysql.TypeFloat)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// src flen > target
		src := types.NewFieldType(mysql.TypeInt24)
		target := types.NewFieldType(mysql.TypeTiny)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// src flen > target
		src := types.NewFieldType(mysql.TypeVarchar)
		src.SetFlen(100)
		target := types.NewFieldType(mysql.TypeVarchar)
		target.SetFlag(99)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// src decimal > target
		src := types.NewFieldType(mysql.TypeNewDecimal)
		src.SetDecimal(5)
		target := types.NewFieldType(mysql.TypeNewDecimal)
		target.SetDecimal(4)
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// src has more elements
		src := types.NewFieldType(mysql.TypeEnum)
		src.SetElems([]string{"a", "b"})
		target := types.NewFieldType(mysql.TypeEnum)
		target.SetElems([]string{"a"})
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// incompatible enum
		src := types.NewFieldType(mysql.TypeEnum)
		src.SetElems([]string{"a", "b"})
		target := types.NewFieldType(mysql.TypeEnum)
		target.SetElems([]string{"a", "c", "d"})
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// incompatible charset
		src := types.NewFieldType(mysql.TypeVarchar)
		src.SetCharset("gbk")
		target := types.NewFieldType(mysql.TypeVarchar)
		target.SetCharset("utf8")
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		// incompatible collation
		src := types.NewFieldType(mysql.TypeVarchar)
		src.SetCharset("utf8")
		src.SetCollate("utf8_bin")
		target := types.NewFieldType(mysql.TypeVarchar)
		target.SetCharset("utf8")
		target.SetCollate("utf8_general_ci")
		require.False(t, IsTypeCompatible(*src, *target))
	}
	{
		src := types.NewFieldType(mysql.TypeVarchar)
		src.SetFlen(10)
		src.SetCharset("utf8")
		src.SetCollate("utf8_bin")
		target := types.NewFieldType(mysql.TypeVarchar)
		target.SetFlen(11)
		target.SetCharset("utf8")
		target.SetCollate("utf8_bin")
		require.True(t, IsTypeCompatible(*src, *target))
	}
	{
		src := types.NewFieldType(mysql.TypeBlob)
		target := types.NewFieldType(mysql.TypeLongBlob)
		require.True(t, IsTypeCompatible(*src, *target))
	}
	{
		src := types.NewFieldType(mysql.TypeEnum)
		src.SetElems([]string{"a", "b"})
		target := types.NewFieldType(mysql.TypeEnum)
		target.SetElems([]string{"a", "b", "c"})
		require.True(t, IsTypeCompatible(*src, *target))
	}
	{
		src := types.NewFieldType(mysql.TypeTimestamp)
		target := types.NewFieldType(mysql.TypeTimestamp)
		target.SetDecimal(3)
		require.True(t, IsTypeCompatible(*src, *target))
	}
}

func TestWithCleanUp(t *testing.T) {
	err1 := errors.New("meow?")
	err2 := errors.New("nya?")

	case1 := func() (err error) {
		defer WithCleanUp(&err, time.Second, func(ctx context.Context) error {
			return err1
		})
		return nil
	}
	require.ErrorIs(t, case1(), err1)

	case2 := func() (err error) {
		defer WithCleanUp(&err, time.Second, func(ctx context.Context) error {
			return err1
		})
		return err2
	}
	require.ElementsMatch(t, []error{err1, err2}, multierr.Errors(case2()))

	case3 := func() (err error) {
		defer WithCleanUp(&err, time.Second, func(ctx context.Context) error {
			return nil
		})
		return nil
	}
	require.NoError(t, case3())
}
