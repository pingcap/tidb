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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package privileges

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	jwkRepo "github.com/lestrrat-go/jwx/v2/jwk"
	jwsRepo "github.com/lestrrat-go/jwx/v2/jws"
	jwtRepo "github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
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
				logutil.BgLogger().Error("Fail to load JWKS", zap.String("path", jwksPath), zap.Duration("interval", interval))
			}
		}
	}()
	return load()
}

// verifyJWT verifies the signature in the jwt, and returns the claims.
func verifyJWT(tokenString string, retryTime int) (map[string]interface{}, error) {
	var (
		verifiedPayload []byte
		err             error
	)
	for retryTime >= 0 {
		retryTime--
		parts := strings.Split(tokenString, ".")
		if len(parts) != 3 {
			err = errors.New("Invalid JWT")
			continue
		}

		// verify signature
		verifiedPayload, err = verify(([]byte)(tokenString))
		if err != nil {
			if e := load(); e != nil {
				err = e
			}
			continue
		}

		jwt := jwtRepo.New()
		if err = jwt.(json.Unmarshaler).UnmarshalJSON(verifiedPayload); err != nil {
			continue
		}
		claims, err := jwt.AsMap(context.Background())
		if err != nil {
			continue
		}
		return claims, nil
	}
	err = errors.Annotate(err, "Retry time has been spent out")
	return nil, err
}
