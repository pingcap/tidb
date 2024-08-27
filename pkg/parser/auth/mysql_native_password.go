// Copyright 2015 PingCAP, Inc.
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

package auth

import (
	"bytes"
	"crypto/sha1" //nolint: gosec
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// CheckScrambledPassword check scrambled password received from client.
// The new authentication is performed in following manner:
//
//	SERVER:  public_seed=create_random_string()
//	         send(public_seed)
//	CLIENT:  recv(public_seed)
//	         hash_stage1=sha1("password")
//	         hash_stage2=sha1(hash_stage1)
//	         reply=xor(hash_stage1, sha1(public_seed,hash_stage2)
//	         // this three steps are done in scramble()
//	         send(reply)
//	SERVER:  recv(reply)
//	         hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
//	         candidate_hash2=sha1(hash_stage1)
//	         check(candidate_hash2==hash_stage2)
//	         // this three steps are done in check_scramble()
func CheckScrambledPassword(salt, hpwd, auth []byte) bool {
	//nolint: gosec
	crypt := sha1.New()
	_, err := crypt.Write(salt)
	terror.Log(errors.Trace(err))
	_, err = crypt.Write(hpwd)
	terror.Log(errors.Trace(err))
	hash := crypt.Sum(nil)
	// token = scrambleHash XOR stage1Hash
	if len(auth) != len(hash) {
		return false
	}
	for i := range hash {
		hash[i] ^= auth[i]
	}

	return bytes.Equal(hpwd, Sha1Hash(hash))
}

// Sha1Hash is an util function to calculate sha1 hash.
func Sha1Hash(bs []byte) []byte {
	//nolint: gosec
	crypt := sha1.New()
	_, err := crypt.Write(bs)
	terror.Log(errors.Trace(err))
	return crypt.Sum(nil)
}

// EncodePassword converts plaintext password(type is string) to hashed hex string.
func EncodePassword(pwd string) string {
	if len(pwd) == 0 {
		return ""
	}
	hash1 := Sha1Hash([]byte(pwd))
	hash2 := Sha1Hash(hash1)

	return fmt.Sprintf("*%X", hash2)
}

// EncodePasswordBytes converts plaintext password(type is []byte) to hashed hex string.
func EncodePasswordBytes(pwd []byte) string {
	if len(pwd) == 0 {
		return ""
	}
	hash1 := Sha1Hash(pwd)
	hash2 := Sha1Hash(hash1)

	return fmt.Sprintf("*%X", hash2)
}

// DecodePassword converts hex string password without prefix '*' to byte array.
func DecodePassword(pwd string) ([]byte, error) {
	x, err := hex.DecodeString(pwd[1:])
	if err != nil {
		return nil, errors.Trace(err)
	}
	return x, nil
}
