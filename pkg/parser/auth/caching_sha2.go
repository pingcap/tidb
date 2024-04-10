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

package auth

// Resources:
// - https://dev.mysql.com/doc/refman/8.0/en/caching-sha2-pluggable-authentication.html
// - https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
// - https://dev.mysql.com/doc/dev/mysql-server/latest/namespacesha2__password.html
// - https://www.akkadia.org/drepper/SHA-crypt.txt
// - https://dev.mysql.com/worklog/task/?id=9591
//
// CREATE USER 'foo'@'%' IDENTIFIED BY 'foobar';
// SELECT HEX(authentication_string) FROM mysql.user WHERE user='foo';
// 24412430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537
//
// Format:
// Split on '$':
// - digest type ("A")
// - iterations (divided by ITERATION_MULTIPLIER)
// - salt+hash
//

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/mysql"
)

const (
	// MIXCHARS is the number of characters to use in the mix
	MIXCHARS = 32
	// SALT_LENGTH is the length of the salt
	SALT_LENGTH = 20 //nolint: revive
	// ITERATION_MULTIPLIER is the number of iterations to use
	ITERATION_MULTIPLIER = 1000 //nolint: revive
)

func b64From24bit(b []byte, n int, buf *bytes.Buffer) {
	b64t := []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	w := (int64(b[0]) << 16) | (int64(b[1]) << 8) | int64(b[2])
	for n > 0 {
		n--
		buf.WriteByte(b64t[w&0x3f])
		w >>= 6
	}
}

// Sha256Hash is an util function to calculate sha256 hash.
func Sha256Hash(input []byte) []byte {
	res := sha256.Sum256(input)
	return res[:]
}

// 'hash' function should return an array with 32 bytes, the same as SHA-256
func hashCrypt(plaintext string, salt []byte, iterations int, hash func([]byte) []byte) string {
	// Numbers in the comments refer to the description of the algorithm on https://www.akkadia.org/drepper/SHA-crypt.txt

	// 1, 2, 3
	bufA := bytes.NewBuffer(make([]byte, 0, 4096))
	bufA.WriteString(plaintext)
	bufA.Write(salt)

	// 4, 5, 6, 7, 8
	bufB := bytes.NewBuffer(make([]byte, 0, 4096))
	bufB.WriteString(plaintext)
	bufB.Write(salt)
	bufB.WriteString(plaintext)
	sumB := hash(bufB.Bytes())
	bufB.Reset()

	// 9, 10
	var i int
	for i = len(plaintext); i > MIXCHARS; i -= MIXCHARS {
		bufA.Write(sumB[:MIXCHARS])
	}
	bufA.Write(sumB[:i])

	// 11
	for i = len(plaintext); i > 0; i >>= 1 {
		if i%2 == 0 {
			bufA.WriteString(plaintext)
		} else {
			bufA.Write(sumB[:])
		}
	}

	// 12
	sumA := hash(bufA.Bytes())
	bufA.Reset()

	// 13, 14, 15
	bufDP := bufA
	for range []byte(plaintext) {
		bufDP.WriteString(plaintext)
	}
	sumDP := hash(bufDP.Bytes())
	bufDP.Reset()

	// 16
	p := make([]byte, 0, sha256.Size)
	for i = len(plaintext); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			p = append(p, sumDP[:]...)
		} else {
			p = append(p, sumDP[0:i]...)
		}
	}

	// 17, 18, 19
	bufDS := bufA
	for i = 0; i < 16+int(sumA[0]); i++ {
		bufDS.Write(salt)
	}
	sumDS := hash(bufDS.Bytes())
	bufDS.Reset()

	// 20
	s := make([]byte, 0, 32)
	for i = len(salt); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			s = append(s, sumDS[:]...)
		} else {
			s = append(s, sumDS[0:i]...)
		}
	}

	// 21
	bufC := bufA
	var sumC []byte
	for i = 0; i < iterations; i++ {
		bufC.Reset()
		if i&1 != 0 {
			bufC.Write(p)
		} else {
			bufC.Write(sumA[:])
		}
		if i%3 != 0 {
			bufC.Write(s)
		}
		if i%7 != 0 {
			bufC.Write(p)
		}
		if i&1 != 0 {
			bufC.Write(sumA[:])
		} else {
			bufC.Write(p)
		}
		sumC = hash(bufC.Bytes())
		sumA = sumC
	}

	// 22
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	buf.Write([]byte{'$', 'A', '$'})
	rounds := fmt.Sprintf("%03X", iterations/ITERATION_MULTIPLIER)
	buf.WriteString(rounds)
	buf.Write([]byte{'$'})
	buf.Write(salt)

	b64From24bit([]byte{sumC[0], sumC[10], sumC[20]}, 4, buf)
	b64From24bit([]byte{sumC[21], sumC[1], sumC[11]}, 4, buf)
	b64From24bit([]byte{sumC[12], sumC[22], sumC[2]}, 4, buf)
	b64From24bit([]byte{sumC[3], sumC[13], sumC[23]}, 4, buf)
	b64From24bit([]byte{sumC[24], sumC[4], sumC[14]}, 4, buf)
	b64From24bit([]byte{sumC[15], sumC[25], sumC[5]}, 4, buf)
	b64From24bit([]byte{sumC[6], sumC[16], sumC[26]}, 4, buf)
	b64From24bit([]byte{sumC[27], sumC[7], sumC[17]}, 4, buf)
	b64From24bit([]byte{sumC[18], sumC[28], sumC[8]}, 4, buf)
	b64From24bit([]byte{sumC[9], sumC[19], sumC[29]}, 4, buf)
	b64From24bit([]byte{0, sumC[31], sumC[30]}, 3, buf)

	return buf.String()
}

// CheckHashingPassword checks if a caching_sha2_password or tidb_sm3_password authentication string matches a password
func CheckHashingPassword(pwhash []byte, password string, hash string) (bool, error) {
	pwhashParts := bytes.Split(pwhash, []byte("$"))
	if len(pwhashParts) != 4 {
		return false, errors.New("failed to decode hash parts")
	}

	hashType := string(pwhashParts[1])
	if hashType != "A" {
		return false, errors.New("digest type is incompatible")
	}

	iterations, err := strconv.ParseInt(string(pwhashParts[2]), 16, 64)
	if err != nil {
		return false, errors.New("failed to decode iterations")
	}
	iterations = iterations * ITERATION_MULTIPLIER
	salt := pwhashParts[3][:SALT_LENGTH]

	var newHash string
	switch hash {
	case mysql.AuthCachingSha2Password:
		newHash = hashCrypt(password, salt, int(iterations), Sha256Hash)
	case mysql.AuthTiDBSM3Password:
		newHash = hashCrypt(password, salt, int(iterations), Sm3Hash)
	}

	return bytes.Equal(pwhash, []byte(newHash)), nil
}

// NewHashPassword creates a new password for caching_sha2_password or tidb_sm3_password
func NewHashPassword(pwd string, hash string) string {
	salt := make([]byte, SALT_LENGTH)
	rand.Read(salt)

	// Restrict to 7-bit to avoid multi-byte UTF-8
	for i := range salt {
		salt[i] = salt[i] &^ 128
		for salt[i] == 36 || salt[i] == 0 { // '$' or NUL
			newval := make([]byte, 1)
			rand.Read(newval)
			salt[i] = newval[0] &^ 128
		}
	}

	switch hash {
	case mysql.AuthCachingSha2Password:
		return hashCrypt(pwd, salt, 5*ITERATION_MULTIPLIER, Sha256Hash)
	case mysql.AuthTiDBSM3Password:
		return hashCrypt(pwd, salt, 5*ITERATION_MULTIPLIER, Sm3Hash)
	default:
		return ""
	}
}
