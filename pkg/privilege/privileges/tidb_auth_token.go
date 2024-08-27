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
	"sync/atomic"
	"time"
	"unsafe"

	jwkRepo "github.com/lestrrat-go/jwx/v2/jwk"
	jwsRepo "github.com/lestrrat-go/jwx/v2/jws"
	jwtRepo "github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// JWKSImpl contains a JSON Web Key Set (JWKS), and a filepath that stores the JWKS
type JWKSImpl struct {
	set      unsafe.Pointer // *jwkRepo.Set
	filepath string
}

// GlobalJWKS is the global JWKS for tidb-server
var GlobalJWKS JWKSImpl

func (jwks *JWKSImpl) load() error {
	cur, err := jwkRepo.ReadFile(jwks.filepath)
	if err == nil {
		atomic.StorePointer(&jwks.set, unsafe.Pointer(&cur))
	}
	return err
}

func (jwks *JWKSImpl) verify(tokenBytes []byte) (payload []byte, err error) {
	s := (*jwkRepo.Set)(atomic.LoadPointer(&jwks.set))
	if s == nil {
		return nil, errors.New("No valid JWKS yet")
	}
	return jwsRepo.Verify(tokenBytes, jwsRepo.WithKeySet(*s))
}

// LoadJWKS4AuthToken reload the jwks every auth-token-refresh-interval.
func (jwks *JWKSImpl) LoadJWKS4AuthToken(ctx context.Context, wg *sync.WaitGroup, jwksPath string, interval time.Duration) error {
	jwks.filepath = jwksPath
	if ctx != nil && wg != nil {
		go func() {
			ticker := time.Tick(interval)
			wg.Add(1)
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case <-ticker:
				}
				if err := jwks.load(); err != nil {
					logutil.BgLogger().Error("Fail to load JWKS", zap.String("path", jwksPath), zap.Duration("interval", interval))
				}
			}
		}()
	}
	return jwks.load()
}

// checkSigWithRetry verifies the signature in the jwt, and returns the claims.
func (jwks *JWKSImpl) checkSigWithRetry(tokenString string, retryTime int) (map[string]any, error) {
	var (
		verifiedPayload []byte
		err             error
	)
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		err = errors.New("Invalid JWT")
		return nil, err
	}
	for retryTime >= 0 {
		retryTime--

		// verify signature
		verifiedPayload, err = jwks.verify(([]byte)(tokenString))
		if err != nil {
			if err1 := jwks.load(); err1 != nil {
				return nil, err1
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
