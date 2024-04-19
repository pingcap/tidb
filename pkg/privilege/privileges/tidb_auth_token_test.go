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
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	jwaRepo "github.com/lestrrat-go/jwx/v2/jwa"
	jwkRepo "github.com/lestrrat-go/jwx/v2/jwk"
	jwsRepo "github.com/lestrrat-go/jwx/v2/jws"
	jwtRepo "github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/lestrrat-go/jwx/v2/jwt/openid"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/stretchr/testify/require"
)

var (
	privateKeyStrings = []string{`-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAq8G5n9XBidxmBMVJKLOBsmdOHrCqGf17y9+VUXingwDUZxRp
2XbuLZLbJtLgcln1lC0L9BsogrWf7+pDhAzWovO6Ai4Aybu00tJ2u0g4j1aLiDds
y0gyvSb5FBoL08jFIH7t/JzMt4JpF487AjzvITwZZcnsrB9a9sdn2E5B/aZmpDGi
2+Isf5osnlw0zvveTwiMo9ba416VIzjntAVEvqMFHK7vyHqXbfqUPAyhjLO+iee9
9Tg5AlGfjo1s6FjeML4xX7sAMGEy8FVBWNfpRU7ryTWoSn2adzyA/FVmtBvJNQBC
MrrAhXDTMJ5FNi8zHhvzyBKHU0kBTS1UNUbP9wIDAQABAoIBAFF0sbz82imwje2L
RvP3lfXvClyBulpTHigFJEKcLw1xEkrEoqKQxcp1UFvsPKfexBn+9yFQ0/iRfIWC
m3x/vjdP0ZKBELybudkWGVsemDxadhgm+QC7f9y3I/+FjsBlAiA0MlfQYUJSpdaX
hgu8rEgdwYnFpunGgRRyY2xxSNirEAzA6aTa1PkNU6W7nF5trOUOfdUSNZuPsS4y
rQjZJZDxB4SW+biuTqNAOKPPnnFY3PdntQx9uhcSm+qiDP2yQXoXuDK/TAN4euOK
vR5POnnDNKhFizGnR8xjW8GSmfg9ILxw/BpNFoIkvZo5xLtt7lNM2VPJaLzXEse2
axOpKckCgYEA2g8GWQOmqH8M4LaOxZcy+4dvoOou4vv+V5Bn4TDtmRaQd40BqfOZ
jyi9sci7iGYVsHdSpLlLFcXedx97QKstJZZ8RKQZv/wBZ7JH6Hn80ipGnJ3a7S9+
JY99iVDF6hOroR2fbnrqa/Dx8pPdMy9ZOXZvh3Q527j8u4m9zXUXfVUCgYEAyaRG
dSEt/AJxoecZqa450H8rlOQVDC0DcQcxGlEP7L2wQRinnJkfZ6+r7jhfu4SikOZO
MdXDF/ILGxSXw6+0xHwq9XfSlNhgTTcBNZOYfchMi6mvUxe/r4TsMXEcbRPSsuWo
EZJ1oZLHxdw9B96R9blnxk54VvILG60rrwbaOBsCgYEAz8EQ4y4/Urn5ov9L96We
xVa8XCvCkDBWm0bSMhNTzE9bRQvrUejtnR/L297MDaB1ebO14YtIpm3nDsfHvk1Y
rj86FovinK+VBx8ss6nF3ta4f+9F7kUZgt+7U2DJr8Md+lsm0zP4tO7TFbMbRPEP
qVfV2tA5b8ZHxMXvOBkfUCECgYAZbFvx0rAgkRJQrnme2jex4QbWq/c3ZMmFS7nW
LphKahQ58OjZJrk98nlD/NmdI/j3OgJr6B7D+yGJVYxZAONSzrD/6A6l864YrjG5
1pUobsOv7EINwPXLJIA/L5q86f3rzmblaEjqiT4k5ULQpjBTAgBikWw80iGyaKAU
XlHPNwKBgQDC45gv8aRxJXwSjpCXHnnzoWAJHBOXIpTbQOVdGbuMRr5RAh4CVFsp
6rnNlannpnE8EMkLtAmPLNqmsP0XCRo2TpHU86PRO3OGH/3KEtU/X3ij9sts2OlM
03m9HNt6/h9glwk7NYwbGgOlKhRxr/DUTkumu0tdfYN+tLU83mBeNw==
-----END RSA PRIVATE KEY-----`, `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAywV8/DH1vLyuTOu9MBiAF2DLlZi0SOMEUznXVSRbt0+YVfsr
o67+66B7ATnB2a5BCyOGaFJ9aIwfTWILMTJo91hVk4gHdvsSYeiS3gnSQtKYEdAX
ZgL2apGP1s08XQfluTF57fxVn8RpKieox6Ea68JSGMuh0AEr2MuJzaTcxzQ5UpIi
K2vUuBXNMzZwbZKvssfsyoZ6zIEeco4BCGXXmJUyxFb6MLV8DWKwmUQjhV/EjDem
vE0vrUziY1afo2J9Ngk03mPHqprDZEa8u2wwtm2ghuCaislKh9X7vl31Yj5lcPCU
iacBupV6/bhMjPTAgIAOEcsLVZMK2P+snREDjwIDAQABAoIBACiu/93V8SWSNeeK
Mg5KSpjkt8dRo4cbnwlChQk10P9J/v/z5knVzpXPQfb76QHDLpuZ0dxj82eY9Mjg
Bdgk/u3aEMQQtVY9d/CQ16WRGEZ1xy2Cor25iEHQy59C337RD1LuPD3ZnBr5FA3z
hpoCic+G0EbRv6pcIbo/B21jRS7Rx+w13CNZQD1fL5vEc1CTR+WL/DeCTugGcj8i
wiaUb6eu2Z4YFoJqCWGhTfz1HL4i+y12HfAlezfYae9Lhm0r/mLMos6O7gHWqW24
EbmeQZy+TGjd7SBw1wsEv7ZO+MFsfvbBvZidmK/FcxUqiyfsvhsTuRbgv6+GiMep
rF+acgkCgYEA6C7dg6GtBydIGq1iE7ty2pUcW4YPL2BVjTK7Fntt1ToVzUKZAulG
Av0+kukeReDLGxrNMhHDzGuLboA2v/PNcMnoJWnzg2+tMByyLWEvIvp9fngbSwRr
JEdDbUDQZbpEkyEC8fDAO3l3EmoHaGBEshZ0tDl0fui36vM1w8lhZDUCgYEA39jW
bsHHny4QUwwsXu/dvg8meYP2rCjBxjM7PIz1FKut+oftUmYCVhRvhZl5ydpO9/2f
VQYqHnDMlmAzjCovKvjFFMXJl2QucUHR+S94sobmTj6tfY9VzAq8uZaMi9jq5uRL
WZvmTPtj3U7KequCqCN7w14o7JkFxOGquFy5eTMCgYANr42BD8uaK1eVsvif/yGS
/s0QHAPTIBOK4h2jAp2Dvwu/8JgCUuu8i17f2/vb1JdEPr0voVpwNzqdxdL0V5OZ
fV1Ar1EaQz/rIRXjlOHpZuh0xvGc52LFXan8y6A9DtCx93Ur+6vpFYzOOg+7uEj0
UlyIrwZN4LvOjo1xv/IMrQKBgQCfsFAhSUqAa1sn87o/q/zTlnlLHPI/lP/PxkKP
CrvYGDWQUaHjM3SdNgztETUJ5ByL27nr7O7lMnExIcYESx/FFx15mTQcNVLQZzVF
ADGpooTv8tTPiw6Y9lv2RclUBtZlCx4Z+hbMelaezZOy+WHHUzD6idTGHNA5yQeC
aFvEcwKBgQC+QzEkoG7IDqrFL62x+H607juYF4IY4kXo7zsrfY4uWffC7Mf5XaYs
qkX9+ouK/CROAKO+UdMEs8PWHF1CHmgV3t/EF2+xfkGvVr/RlgtMHgQe8lX9a+sK
1xpqDpqmXTST37cy+lQGPXmWrJsTulWQj0F1LV4i4qt7Ph4JK4kzvA==
-----END RSA PRIVATE KEY-----
`, `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAoaJsIxrUBKPW/dogPpUxxhiL8cpUt8uWlclOrmUSHFZzY50r
wsCt2ndnZRHE/HD+X7oCo28pdYTySZWnsiY/K2HeyYdsRzUH71Mx0Z+a1uBa0k6B
VHY8vrPObLCPFEmxnqml+Wj74zocsR23/puCz8Vgm+0VF49vu+ab90lc2iLJtElv
eRnLrSkaudCUndmn+aVftwnpDxJ4Z0rRlJkhyeZMN4+EMse5+0hAhg5UiPHE6pG8
RI3zYnp0EYKvN+M9/cdNntyuKCCCvOCi4b4d4wpGOrDuiA/moh2J9zwPBMiyvIFo
zUMmqAQV3zuUxx+jAAjrc9ReQLnoExhuhfrU5wIDAQABAoIBAC62PgI3MqbUosFi
VIdBnszdMzSBgNJNKAvJzc9grkc6RMa5GXiDLrtAXsU6yW8bSKhpnXGWIqkv7sWN
VpWJsB/dfQFI/eXmUZC8vl0SfzEyTY0R2xaJxSxn0nRe4jq+wXJVHP5jdMhKdxhI
um/+iWN6a10kuz+/2E65asGglhEEHxzm9ux9PGbhOR7NVAiReRfEKN0UgmD9jWHL
nR2uBsS3BsPBURKBERzYOqGmxMgOq9Y07Jf6d4Ln33SfkKsD/ibTPoUyTsvwG0g1
J7wVmqZRxG7GLGxLjjs+s16LWjRKUCbHOf7VIMKkYj2HBgzMZOG6/f579mejB//D
K5rSwuECgYEA0doT9iTVq9ZbpYKfLH5HzqoTuZeP5Q5acO2ZM2Bat6Xdk/ZgOgh4
Gvzgfi33kl03Pp2ZhQX9m1k1eicTcDPvNQZ3JeTI7bgO3ZQXt8PkCL/pPCZyfusB
C9sP4zhhmieLuX7SZmkWpJvy1XtjvJsyhnnZz2s51nvCKKAe6JVFatUCgYEAxS3f
yFOBzRAyuPWUF4pGTAVfysM47Zl0alDcZgM30ARhqhsfHOo26xeU6TEWucCh30fS
tehXlQDlygHN1+CxkqH6mv0Nlp1j/1YV9mZIEZ++jIggAgsit29YtoQMIe6/lv0+
+aivyNJrCtbgm9ZA4+OOie3Cvjf/6qnqnBSFpssCgYBWkfyCIpfzF68fDE/V7xJ4
czlH6vp1qAIvbBUzWKCT+lz6WT1BM5U4rPF/nD7xpnrP3fwjIGGK4LZq+gvO0d3w
pgYpH8S0LKYVSq6uJKXB5km1grbhHNmFpo1bUzsQeRfvIh5yGRA6QAthflGa0Pt6
9nGgW7+0d8GVONkHYe0NMQKBgAJU64uL6UIKif8D8G9i1Df77EkSi+7LXMQRFroi
GZvdIWaIkZKe9m1LRxiG2xTxQTjJuaUrDTYW36DG6q892fu47KS+j1WToOYZF4Nl
bD7BG9i/l1lO1mdC6tKltxsDnsJjVkZPh1yhmGB1cAyHuRa4zyu0YxQqx1z4C20z
FO2HAoGBALp9nGqbK6N96LYgef8GpP6o5pz3D1Jtj18iYyn3oz6z9t3dqNbpf2vh
cYnDqCQWSX5rfDRMbuhEJB+GvHYKVY/yVJ2ZWu1cKsB+2gzsITWewfxTS/ns+4Qk
RfViImdNIa19f7cmeC8RjhaSWBmb9JJk+p75e4XpgD1bG9U7DjiH
-----END RSA PRIVATE KEY-----
`}

	publicKeyStrings = []string{`-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq8G5n9XBidxmBMVJKLOB
smdOHrCqGf17y9+VUXingwDUZxRp2XbuLZLbJtLgcln1lC0L9BsogrWf7+pDhAzW
ovO6Ai4Aybu00tJ2u0g4j1aLiDdsy0gyvSb5FBoL08jFIH7t/JzMt4JpF487Ajzv
ITwZZcnsrB9a9sdn2E5B/aZmpDGi2+Isf5osnlw0zvveTwiMo9ba416VIzjntAVE
vqMFHK7vyHqXbfqUPAyhjLO+iee99Tg5AlGfjo1s6FjeML4xX7sAMGEy8FVBWNfp
RU7ryTWoSn2adzyA/FVmtBvJNQBCMrrAhXDTMJ5FNi8zHhvzyBKHU0kBTS1UNUbP
9wIDAQAB
-----END PUBLIC KEY-----`, `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAywV8/DH1vLyuTOu9MBiA
F2DLlZi0SOMEUznXVSRbt0+YVfsro67+66B7ATnB2a5BCyOGaFJ9aIwfTWILMTJo
91hVk4gHdvsSYeiS3gnSQtKYEdAXZgL2apGP1s08XQfluTF57fxVn8RpKieox6Ea
68JSGMuh0AEr2MuJzaTcxzQ5UpIiK2vUuBXNMzZwbZKvssfsyoZ6zIEeco4BCGXX
mJUyxFb6MLV8DWKwmUQjhV/EjDemvE0vrUziY1afo2J9Ngk03mPHqprDZEa8u2ww
tm2ghuCaislKh9X7vl31Yj5lcPCUiacBupV6/bhMjPTAgIAOEcsLVZMK2P+snRED
jwIDAQAB
-----END PUBLIC KEY-----`, `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoaJsIxrUBKPW/dogPpUx
xhiL8cpUt8uWlclOrmUSHFZzY50rwsCt2ndnZRHE/HD+X7oCo28pdYTySZWnsiY/
K2HeyYdsRzUH71Mx0Z+a1uBa0k6BVHY8vrPObLCPFEmxnqml+Wj74zocsR23/puC
z8Vgm+0VF49vu+ab90lc2iLJtElveRnLrSkaudCUndmn+aVftwnpDxJ4Z0rRlJkh
yeZMN4+EMse5+0hAhg5UiPHE6pG8RI3zYnp0EYKvN+M9/cdNntyuKCCCvOCi4b4d
4wpGOrDuiA/moh2J9zwPBMiyvIFozUMmqAQV3zuUxx+jAAjrc9ReQLnoExhuhfrU
5wIDAQAB
-----END PUBLIC KEY-----`}

	priKeys  []*rsa.PrivateKey
	pubKeys  []*rsa.PublicKey
	jwkArray []jwkRepo.Key
	path     [3]string // path[0] contains jwkArray[0], path[1] contains jwkArray[0:2], path[2] contains jwkArray[2]

	email1  = "user1@pingcap.com"
	email2  = "user2@pingcap.com"
	issuer1 = "issuer1"
	issuer2 = "issuer2"
)

type pair struct {
	name  string
	value any
}

func init() {
	for i := range publicKeyStrings {
		v, rest, err := jwkRepo.DecodePEM(([]byte)(privateKeyStrings[i]))
		if err != nil {
			log.Println(err.Error())
			log.Fatal("Error in decode private key")
		}
		if len(rest) > 0 {
			log.Fatal("Rest in decode private key")
		}
		priKey, ok := v.(*rsa.PrivateKey)
		if !ok {
			log.Fatal("Wrong type of private key")
		}
		priKeys = append(priKeys, priKey)
		v, rest, err = jwkRepo.DecodePEM(([]byte)(publicKeyStrings[i]))
		if err != nil {
			log.Println(err.Error())
			log.Fatal("Error in decode public key")
		} else if len(rest) > 0 {
			log.Fatal("Rest in decode public key")
		}
		pubKey, ok := v.(*rsa.PublicKey)
		if !ok {
			log.Fatal("Wrong type of public key")
		}
		pubKeys = append(pubKeys, pubKey)
		jwk, err := jwkRepo.FromRaw(pubKey)
		if err != nil {
			log.Fatal("Error when generate jwk")
		}
		keyAttributes := []pair{
			{jwkRepo.AlgorithmKey, jwaRepo.RS256},
			{jwkRepo.KeyIDKey, fmt.Sprintf("the-key-id-%d", i)},
			{jwkRepo.KeyUsageKey, "sig"},
		}
		for _, keyAttribute := range keyAttributes {
			if err = jwk.Set(keyAttribute.name, keyAttribute.value); err != nil {
				log.Println(err.Error())
				log.Fatalf("Error when set %s for key %d", keyAttribute.name, i)
			}
		}
		jwkArray = append(jwkArray, jwk)
	}

	for i := range path {
		path[i] = fmt.Sprintf("%s%cjwks%d.json", os.TempDir(), os.PathSeparator, i)
		file, err := os.Create(path[i])
		if err != nil {
			log.Fatal("Fail to create temp file")
		}
		jwks := jwkRepo.NewSet()
		var rawJSON []byte
		if i == 2 {
			jwks.AddKey(jwkArray[i])
		} else {
			for j := 0; j <= i; j++ {
				jwks.AddKey(jwkArray[j])
			}
		}
		if rawJSON, err = json.MarshalIndent(jwks, "", "  "); err != nil {
			log.Fatal("Error when marshaler json")
		}
		if n, err := file.Write(rawJSON); err != nil {
			log.Fatal("Error when writing json")
		} else if n != len(rawJSON) {
			log.Fatal("Lack byte when writing json")
		}
	}
}

func getSignedTokenString(priKey *rsa.PrivateKey, pairs map[string]any) (string, error) {
	jwt := jwtRepo.New()
	header := jwsRepo.NewHeaders()
	headerPairs := []pair{
		{jwsRepo.AlgorithmKey, jwaRepo.RS256},
		{jwsRepo.TypeKey, "JWT"},
	}
	for _, pair := range headerPairs {
		if err := header.Set(pair.name, pair.value); err != nil {
			log.Fatal("Error when set header")
		}
	}
	for k, v := range pairs {
		switch k {
		case jwsRepo.KeyIDKey:
			if err := header.Set(k, v); err != nil {
				log.Fatal("Error when set header")
			}
		case jwtRepo.SubjectKey, jwtRepo.IssuedAtKey, jwtRepo.ExpirationKey, jwtRepo.IssuerKey, openid.EmailKey:
			if err := jwt.Set(k, v); err != nil {
				log.Fatal("Error when set payload")
			}
		}
	}
	bytes, err := jwtRepo.Sign(jwt, jwtRepo.WithKey(jwaRepo.RS256, priKey, jwsRepo.WithProtectedHeaders(header)))
	if err != nil {
		return "", err
	}
	return string(hack.String(bytes)), nil
}

func TestAuthTokenClaims(t *testing.T) {
	var jwksImpl JWKSImpl
	now := time.Now()
	require.NoError(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, path[0], time.Hour), path[0])
	claims := map[string]any{
		jwsRepo.KeyIDKey:      "the-key-id-0",
		jwtRepo.SubjectKey:    email1,
		openid.EmailKey:       email1,
		jwtRepo.IssuedAtKey:   now.Unix(),
		jwtRepo.ExpirationKey: now.Add(100 * time.Hour).Unix(),
		jwtRepo.IssuerKey:     issuer1,
	}
	signedTokenString, err := getSignedTokenString(priKeys[0], claims)
	require.NoError(t, err)
	verifiedClaims, err := jwksImpl.checkSigWithRetry(signedTokenString, 0)
	require.NoError(t, err)
	for k, v := range claims {
		switch k {
		case jwtRepo.SubjectKey, openid.EmailKey, jwtRepo.IssuerKey:
			require.Equal(t, v, verifiedClaims[k])
		case jwtRepo.IssuedAtKey, jwtRepo.ExpirationKey:
			require.Equal(t, v, verifiedClaims[k].(time.Time).Unix())
		}
	}
	record := &UserRecord{
		baseRecord: baseRecord{
			User: email1,
		},
		AuthTokenIssuer: issuer1,
		UserAttributesInfo: UserAttributesInfo{
			MetadataInfo: MetadataInfo{
				Email: email1,
			},
		},
	}

	// Success
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.NoError(t, err)

	// test 'sub'
	verifiedClaims[jwtRepo.SubjectKey] = email2
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "Wrong 'sub'")
	delete(verifiedClaims, jwtRepo.SubjectKey)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "lack 'sub'")
	verifiedClaims[jwtRepo.SubjectKey] = email1

	// test 'email'
	verifiedClaims[openid.EmailKey] = email2
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "Wrong 'email'")
	delete(verifiedClaims, openid.EmailKey)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "lack 'email'")
	verifiedClaims[openid.EmailKey] = email1

	// test 'iat'
	delete(verifiedClaims, jwtRepo.IssuedAtKey)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "lack 'iat'")
	verifiedClaims[jwtRepo.IssuedAtKey] = "abc"
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "iat: abc is not a value of time.Time")
	time.Sleep(2 * time.Second)
	verifiedClaims[jwtRepo.IssuedAtKey] = now
	err = checkAuthTokenClaims(verifiedClaims, record, time.Second)
	require.ErrorContains(t, err, "the token has been out of its life time")
	verifiedClaims[jwtRepo.IssuedAtKey] = now.Add(time.Hour)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "the token is issued at a future time")
	verifiedClaims[jwtRepo.IssuedAtKey] = now

	// test 'exp'
	delete(verifiedClaims, jwtRepo.ExpirationKey)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "lack 'exp'")
	verifiedClaims[jwtRepo.ExpirationKey] = "abc"
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "exp: abc is not a value of time.Time")
	verifiedClaims[jwtRepo.ExpirationKey] = now
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "the token has been expired")
	verifiedClaims[jwtRepo.ExpirationKey] = now.Add(100 * time.Hour)

	// test token_issuer
	delete(verifiedClaims, jwtRepo.IssuerKey)
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "lack 'iss'")
	verifiedClaims[jwtRepo.IssuerKey] = issuer1
	record.AuthTokenIssuer = issuer2
	err = checkAuthTokenClaims(verifiedClaims, record, defaultTokenLife)
	require.ErrorContains(t, err, "Wrong 'iss")
}

func TestJWKSImpl(t *testing.T) {
	var jwksImpl JWKSImpl

	// Set wrong path of JWKS
	require.Error(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, "wrong-jwks-path", time.Hour))
	require.Error(t, jwksImpl.load())
	_, err := jwksImpl.checkSigWithRetry("invalid tokenString", 4)
	require.Error(t, err)
	_, err = jwksImpl.verify(([]byte)("invalid tokenString"))
	require.Error(t, err)

	require.NoError(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, path[0], time.Hour), path[0])
	now := time.Now()
	claims := map[string]any{
		jwsRepo.KeyIDKey:      "the-key-id-0",
		jwtRepo.SubjectKey:    email1,
		openid.EmailKey:       email1,
		jwtRepo.IssuedAtKey:   now.Unix(),
		jwtRepo.ExpirationKey: now.Add(100 * time.Hour).Unix(),
		jwtRepo.IssuerKey:     issuer1,
	}
	signedTokenString, err := getSignedTokenString(priKeys[0], claims)
	require.NoError(t, err)
	parts := strings.Split(signedTokenString, ".")

	// Wrong encoded JWT format
	_, err = jwksImpl.checkSigWithRetry(parts[0]+"."+parts[1], 0)
	require.ErrorContains(t, err, "Invalid JWT")
	_, err = jwksImpl.checkSigWithRetry(signedTokenString+"."+parts[1], 0)
	require.ErrorContains(t, err, "Invalid JWT")

	// Wrong signature
	_, err = jwksImpl.checkSigWithRetry(signedTokenString+"A", 0)
	require.ErrorContains(t, err, "could not verify message using any of the signatures or keys")

	// Wrong signature, and fail to reload JWKS
	jwksImpl.filepath = "wrong-path"
	_, err = jwksImpl.checkSigWithRetry(signedTokenString+"A", 0)
	require.ErrorContains(t, err, "open wrong-path: no such file or directory")
	jwksImpl.filepath = path[0]

	require.NoError(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, path[0], time.Hour), path[0])
	_, err = jwksImpl.checkSigWithRetry(signedTokenString, 0)
	require.NoError(t, err)

	// Wrong kid
	claims[jwsRepo.KeyIDKey] = "the-key-id-1"
	signedTokenString, err = getSignedTokenString(priKeys[0], claims)
	require.NoError(t, err)
	_, err = jwksImpl.checkSigWithRetry(signedTokenString, 0)
	require.Error(t, err)
	claims[jwsRepo.KeyIDKey] = "the-key-id-0"
	signedTokenString, err = getSignedTokenString(priKeys[0], claims)
	require.NoError(t, err)
	require.NoError(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, path[1], time.Hour), path[1])
	_, err = jwksImpl.checkSigWithRetry(signedTokenString, 0)
	require.NoError(t, err)
	require.NoError(t, jwksImpl.LoadJWKS4AuthToken(nil, nil, path[2], time.Hour), path[2])
	_, err = jwksImpl.checkSigWithRetry(signedTokenString, 0)
	require.Error(t, err)
}
