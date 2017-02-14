// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/encrypt"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
)

var (
	_ builtinFunc = &builtinAesDecryptSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
)

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinAesDecryptSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = true
	return bt, errors.Trace(err)
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	for _, arg := range args {
		// If either function argument is NULL, the function returns NULL.
		if arg.IsNull() {
			return
		}
	}
	key := args[1].GetBytes()
	if !(len(key) == 16 || len(key) == 24 || len(key) == 32) {
		// The key argument should be the AES key, either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
		// If AES_DECRYPT() detects invalid data or incorrect padding, it returns NULL.
		// However, it is possible for AES_DECRYPT() to return a non-NULL value (possibly garbage) if the input data or the key is invalid.
		return
	}
	cryptStr := args[0].GetBytes()
	// By default these functions implement AES with a 128-bit key length.
	// TODO: We only support aes-128-ecb now. We should support other mode latter.
	data, err := encrypt.AESDecryptWithECB(cryptStr, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetBytes(data)
	return
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinAesEncryptSig{newBaseBuiltinFunc(args, ctx)}
	return bt, errors.Trace(err)
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
// We only support aes-128-ecb mode.
// TODO: support other mode.
func (b *builtinAesEncryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	for _, arg := range args {
		// If either function argument is NULL, the function returns NULL.
		if arg.IsNull() {
			return
		}
	}
	str := args[0].GetBytes()
	key := args[1].GetBytes()
	// TOOD:
	// MySQL support key with other length, but we still do not know how MySQL handle those case.
	// We will hanlde those cases latter.
	crypted, err := encrypt.AESEncryptWithECB(str, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetBytes(crypted)
	return
}
