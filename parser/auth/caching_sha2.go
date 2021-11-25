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
)

const (
	MIXCHARS             = 32
	SALT_LENGTH          = 20
	ITERATION_MULTIPLIER = 1000
)

func b64From24bit(b []byte, n int) []byte {
	b64t := []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	w := (int64(b[0]) << 16) | (int64(b[1]) << 8) | int64(b[2])
	ret := make([]byte, 0, n)
	for n > 0 {
		n--
		ret = append(ret, b64t[w&0x3f])
		w >>= 6
	}

	return ret
}

func sha256crypt(plaintext string, salt []byte, iterations int) string {
	// Numbers in the comments refer to the description of the algorithm on https://www.akkadia.org/drepper/SHA-crypt.txt

	// 1, 2, 3
	tmpA := sha256.New()
	tmpA.Write([]byte(plaintext))
	tmpA.Write(salt)

	// 4, 5, 6, 7, 8
	tmpB := sha256.New()
	tmpB.Write([]byte(plaintext))
	tmpB.Write(salt)
	tmpB.Write([]byte(plaintext))
	sumB := tmpB.Sum(nil)

	// 9, 10
	var i int
	for i = len(plaintext); i > MIXCHARS; i -= MIXCHARS {
		tmpA.Write(sumB[:MIXCHARS])
	}
	tmpA.Write(sumB[:i])

	// 11
	for i = len(plaintext); i > 0; i >>= 1 {
		if i%2 == 0 {
			tmpA.Write([]byte(plaintext))
		} else {
			tmpA.Write(sumB)
		}
	}

	// 12
	sumA := tmpA.Sum(nil)

	// 13, 14, 15
	tmpDP := sha256.New()
	for range []byte(plaintext) {
		tmpDP.Write([]byte(plaintext))
	}
	sumDP := tmpDP.Sum(nil)

	// 16
	p := make([]byte, 0, sha256.Size)
	for i = len(plaintext); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			p = append(p, sumDP...)
		} else {
			p = append(p, sumDP[0:i]...)
		}
	}

	// 17, 18, 19
	tmpDS := sha256.New()
	for i = 0; i < 16+int(sumA[0]); i++ {
		tmpDS.Write(salt)
	}
	sumDS := tmpDS.Sum(nil)

	// 20
	s := []byte{}
	for i = len(salt); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			s = append(s, sumDS...)
		} else {
			s = append(s, sumDS[0:i]...)
		}
	}

	// 21
	tmpC := sha256.New()
	var sumC []byte
	for i = 0; i < iterations; i++ {
		tmpC.Reset()

		if i&1 != 0 {
			tmpC.Write(p)
		} else {
			tmpC.Write(sumA)
		}
		if i%3 != 0 {
			tmpC.Write(s)
		}
		if i%7 != 0 {
			tmpC.Write(p)
		}
		if i&1 != 0 {
			tmpC.Write(sumA)
		} else {
			tmpC.Write(p)
		}
		sumC = tmpC.Sum(nil)
		copy(sumA, tmpC.Sum(nil))
	}

	// 22
	buf := bytes.Buffer{}
	buf.Grow(100) // FIXME
	buf.Write([]byte{'$', 'A', '$'})
	rounds := fmt.Sprintf("%03d", iterations/ITERATION_MULTIPLIER)
	buf.Write([]byte(rounds))
	buf.Write([]byte{'$'})
	buf.Write(salt)

	buf.Write(b64From24bit([]byte{sumC[0], sumC[10], sumC[20]}, 4))
	buf.Write(b64From24bit([]byte{sumC[21], sumC[1], sumC[11]}, 4))
	buf.Write(b64From24bit([]byte{sumC[12], sumC[22], sumC[2]}, 4))
	buf.Write(b64From24bit([]byte{sumC[3], sumC[13], sumC[23]}, 4))
	buf.Write(b64From24bit([]byte{sumC[24], sumC[4], sumC[14]}, 4))
	buf.Write(b64From24bit([]byte{sumC[15], sumC[25], sumC[5]}, 4))
	buf.Write(b64From24bit([]byte{sumC[6], sumC[16], sumC[26]}, 4))
	buf.Write(b64From24bit([]byte{sumC[27], sumC[7], sumC[17]}, 4))
	buf.Write(b64From24bit([]byte{sumC[18], sumC[28], sumC[8]}, 4))
	buf.Write(b64From24bit([]byte{sumC[9], sumC[19], sumC[29]}, 4))
	buf.Write(b64From24bit([]byte{0, sumC[31], sumC[30]}, 3))

	return buf.String()
}

// Checks if a MySQL style caching_sha2 authentication string matches a password
func CheckShaPassword(pwhash []byte, password string) (bool, error) {
	pwhash_parts := bytes.Split(pwhash, []byte("$"))
	if len(pwhash_parts) != 4 {
		return false, errors.New("failed to decode hash parts")
	}

	hash_type := string(pwhash_parts[1])
	if hash_type != "A" {
		return false, errors.New("digest type is incompatible")
	}

	iterations, err := strconv.Atoi(string(pwhash_parts[2]))
	if err != nil {
		return false, errors.New("failed to decode iterations")
	}
	iterations = iterations * ITERATION_MULTIPLIER
	salt := pwhash_parts[3][:SALT_LENGTH]

	newHash := sha256crypt(password, salt, iterations)

	return bytes.Equal(pwhash, []byte(newHash)), nil
}

func NewSha2Password(pwd string) string {
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

	return sha256crypt(pwd, salt, 5*ITERATION_MULTIPLIER)
}
