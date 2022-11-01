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

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	jwkRepo "github.com/lestrrat-go/jwx/v2/jwk"
	jwsRepo "github.com/lestrrat-go/jwx/v2/jws"
	jwtRepo "github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/errors"
)

type jwksImpl struct {
	s        jwkRepo.Set
	m        sync.RWMutex
	filepath string
}

var jwks jwksImpl

func load() (err error) {
	jwks.m.Lock()
	defer jwks.m.Unlock()
	jwks.s, err = jwkRepo.ReadFile(jwks.filepath)
	return err
}

func verify(tokenBytes []byte) (payload []byte, err error) {
	jwks.m.RLock()
	defer jwks.m.RUnlock()
	return jwsRepo.Verify(tokenBytes, jwsRepo.WithKeySet(jwks.s))
}

// LoadJWKS4AuthToken reload the jwks every auth-token-refresh-interval.
func LoadJWKS4AuthToken(jwksPath string, interval time.Duration) error {
	jwks.filepath = jwksPath
	go func() {
		for range time.Tick(interval) {
			if err := load(); err != nil {
				fmt.Println("TODO: warn this error")
			}
		}
	}()
	return load()
}

// VerifyJWT verifies the signature in the jwt, and returns the claims.
func VerifyJWT(tokenString string, retryTime int) (map[string]interface{}, error) {
	var Err error
	for retryTime >= 0 {
		retryTime--
		parts := strings.Split(tokenString, ".")
		if len(parts) != 3 {
			Err = errors.AddStack(errors.New("Invalid JWT"))
			continue
		}

		// verify signature
		verifiedPayload, err := verify(([]byte)(tokenString))
		if err != nil {
			Err = errors.AddStack(err)
			if err = load(); err != nil {
				Err = errors.AddStack(err)
			}
			continue
		}

		jwt := jwtRepo.New()
		if err = jwt.(json.Unmarshaler).UnmarshalJSON(verifiedPayload); err != nil {
			Err = errors.AddStack(err)
			continue
		}
		claims, err := jwt.AsMap(context.Background())
		if err != nil {
			Err = errors.AddStack(err)
			continue
		}
		return claims, nil
	}
	Err = errors.AddStack(errors.New("Retry time has been spent out"))
	return nil, Err
}
