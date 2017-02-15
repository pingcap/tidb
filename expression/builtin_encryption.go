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

// TODO: support other mode
const (
	aes128ecb string = "aes-128-ecb"
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
	cryptStr := args[0].GetBytes()
	key := args[1].GetBytes()
	key = handleAESKey(key, aes128ecb)
	// By default these functions implement AES with a 128-bit key length.
	// TODO: We only support aes-128-ecb now. We should support other mode latter.
	data, err := encrypt.AESDecryptWithECB(cryptStr, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(string(data))
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
	key = handleAESKey(key, aes128ecb)
	crypted, err := encrypt.AESEncryptWithECB(str, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(string(crypted))
	return
}

// Transforms an arbitrary long key into a fixed length AES key.
func handleAESKey(key []byte, mode string) []byte {
	// TODO: get key size according to mode
	keySize := 16
	rKey := make([]byte, keySize)
	rIdx := 0
	for _, k := range key {
		if rIdx == keySize {
			rIdx = 0
		}
		rKey[rIdx] ^= k
		rIdx++
	}
	return rKey
}
