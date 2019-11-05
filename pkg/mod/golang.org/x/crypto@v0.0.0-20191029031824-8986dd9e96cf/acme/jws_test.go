// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package acme

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"testing"
)

// The following shell command alias is used in the comments
// throughout this file:
// alias b64raw="base64 -w0 | tr -d '=' | tr '/+' '_-'"

const (
	// Modulus in raw base64:
	// 4xgZ3eRPkwoRvy7qeRUbmMDe0V-xH9eWLdu0iheeLlrmD2mqWXfP9IeSKApbn34
	// g8TuAS9g5zhq8ELQ3kmjr-KV86GAMgI6VAcGlq3QrzpTCf_30Ab7-zawrfRaFON
	// a1HwEzPY1KHnGVkxJc85gNkwYI9SY2RHXtvln3zs5wITNrdosqEXeaIkVYBEhbh
	// Nu54pp3kxo6TuWLi9e6pXeWetEwmlBwtWZlPoib2j3TxLBksKZfoyFyek380mHg
	// JAumQ_I2fjj98_97mk3ihOY4AgVdCDj1z_GCoZkG5Rq7nbCGyosyKWyDX00Zs-n
	// NqVhoLeIvXC4nnWdJMZ6rogxyQQ
	testKeyPEM = `
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA4xgZ3eRPkwoRvy7qeRUbmMDe0V+xH9eWLdu0iheeLlrmD2mq
WXfP9IeSKApbn34g8TuAS9g5zhq8ELQ3kmjr+KV86GAMgI6VAcGlq3QrzpTCf/30
Ab7+zawrfRaFONa1HwEzPY1KHnGVkxJc85gNkwYI9SY2RHXtvln3zs5wITNrdosq
EXeaIkVYBEhbhNu54pp3kxo6TuWLi9e6pXeWetEwmlBwtWZlPoib2j3TxLBksKZf
oyFyek380mHgJAumQ/I2fjj98/97mk3ihOY4AgVdCDj1z/GCoZkG5Rq7nbCGyosy
KWyDX00Zs+nNqVhoLeIvXC4nnWdJMZ6rogxyQQIDAQABAoIBACIEZTOI1Kao9nmV
9IeIsuaR1Y61b9neOF/MLmIVIZu+AAJFCMB4Iw11FV6sFodwpEyeZhx2WkpWVN+H
r19eGiLX3zsL0DOdqBJoSIHDWCCMxgnYJ6nvS0nRxX3qVrBp8R2g12Ub+gNPbmFm
ecf/eeERIVxfifd9VsyRu34eDEvcmKFuLYbElFcPh62xE3x12UZvV/sN7gXbawpP
G+w255vbE5MoaKdnnO83cTFlcHvhn24M/78qP7Te5OAeelr1R89kYxQLpuGe4fbS
zc6E3ym5Td6urDetGGrSY1Eu10/8sMusX+KNWkm+RsBRbkyKq72ks/qKpOxOa+c6
9gm+Y8ECgYEA/iNUyg1ubRdH11p82l8KHtFC1DPE0V1gSZsX29TpM5jS4qv46K+s
8Ym1zmrORM8x+cynfPx1VQZQ34EYeCMIX212ryJ+zDATl4NE0I4muMvSiH9vx6Xc
7FmhNnaYzPsBL5Tm9nmtQuP09YEn8poiOJFiDs/4olnD5ogA5O4THGkCgYEA5MIL
qWYBUuqbEWLRtMruUtpASclrBqNNsJEsMGbeqBJmoMxdHeSZckbLOrqm7GlMyNRJ
Ne/5uWRGSzaMYuGmwsPpERzqEvYFnSrpjW5YtXZ+JtxFXNVfm9Z1gLLgvGpOUCIU
RbpoDckDe1vgUuk3y5+DjZihs+rqIJ45XzXTzBkCgYBWuf3segruJZy5rEKhTv+o
JqeUvRn0jNYYKFpLBeyTVBrbie6GkbUGNIWbrK05pC+c3K9nosvzuRUOQQL1tJbd
4gA3oiD9U4bMFNr+BRTHyZ7OQBcIXdz3t1qhuHVKtnngIAN1p25uPlbRFUNpshnt
jgeVoHlsBhApcs5DUc+pyQKBgDzeHPg/+g4z+nrPznjKnktRY1W+0El93kgi+J0Q
YiJacxBKEGTJ1MKBb8X6sDurcRDm22wMpGfd9I5Cv2v4GsUsF7HD/cx5xdih+G73
c4clNj/k0Ff5Nm1izPUno4C+0IOl7br39IPmfpSuR6wH/h6iHQDqIeybjxyKvT1G
N0rRAoGBAKGD+4ZI/E1MoJ5CXB8cDDMHagbE3cq/DtmYzE2v1DFpQYu5I4PCm5c7
EQeIP6dZtv8IMgtGIb91QX9pXvP0aznzQKwYIA8nZgoENCPfiMTPiEDT9e/0lObO
9XWsXpbSTsRPj0sv1rB+UzBJ0PgjK4q2zOF0sNo7b1+6nlM3BWPx
-----END RSA PRIVATE KEY-----
`

	// This thumbprint is for the testKey defined above.
	testKeyThumbprint = "6nicxzh6WETQlrvdchkz-U3e3DOQZ4heJKU63rfqMqQ"

	// openssl ecparam -name secp256k1 -genkey -noout
	testKeyECPEM = `
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIK07hGLr0RwyUdYJ8wbIiBS55CjnkMD23DWr+ccnypWLoAoGCCqGSM49
AwEHoUQDQgAE5lhEug5xK4xBDZ2nAbaxLtaLiv85bxJ7ePd1dkO23HThqIrvawF5
QAaS/RNouybCiRhRjI3EaxLkQwgrCw0gqQ==
-----END EC PRIVATE KEY-----
`
	// openssl ecparam -name secp384r1 -genkey -noout
	testKeyEC384PEM = `
-----BEGIN EC PRIVATE KEY-----
MIGkAgEBBDAQ4lNtXRORWr1bgKR1CGysr9AJ9SyEk4jiVnlUWWUChmSNL+i9SLSD
Oe/naPqXJ6CgBwYFK4EEACKhZANiAAQzKtj+Ms0vHoTX5dzv3/L5YMXOWuI5UKRj
JigpahYCqXD2BA1j0E/2xt5vlPf+gm0PL+UHSQsCokGnIGuaHCsJAp3ry0gHQEke
WYXapUUFdvaK1R2/2hn5O+eiQM8YzCg=
-----END EC PRIVATE KEY-----
`
	// openssl ecparam -name secp521r1 -genkey -noout
	testKeyEC512PEM = `
-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIBSNZKFcWzXzB/aJClAb305ibalKgtDA7+70eEkdPt28/3LZMM935Z
KqYHh/COcxuu3Kt8azRAUz3gyr4zZKhlKUSgBwYFK4EEACOhgYkDgYYABAHUNKbx
7JwC7H6pa2sV0tERWhHhB3JmW+OP6SUgMWryvIKajlx73eS24dy4QPGrWO9/ABsD
FqcRSkNVTXnIv6+0mAF25knqIBIg5Q8M9BnOu9GGAchcwt3O7RDHmqewnJJDrbjd
GGnm6rb+NnWR9DIopM0nKNkToWoF/hzopxu4Ae/GsQ==
-----END EC PRIVATE KEY-----
`
	// 1. openssl ec -in key.pem -noout -text
	// 2. remove first byte, 04 (the header); the rest is X and Y
	// 3. convert each with: echo <val> | xxd -r -p | b64raw
	testKeyECPubX    = "5lhEug5xK4xBDZ2nAbaxLtaLiv85bxJ7ePd1dkO23HQ"
	testKeyECPubY    = "4aiK72sBeUAGkv0TaLsmwokYUYyNxGsS5EMIKwsNIKk"
	testKeyEC384PubX = "MyrY_jLNLx6E1-Xc79_y-WDFzlriOVCkYyYoKWoWAqlw9gQNY9BP9sbeb5T3_oJt"
	testKeyEC384PubY = "Dy_lB0kLAqJBpyBrmhwrCQKd68tIB0BJHlmF2qVFBXb2itUdv9oZ-TvnokDPGMwo"
	testKeyEC512PubX = "AdQ0pvHsnALsfqlraxXS0RFaEeEHcmZb44_pJSAxavK8gpqOXHvd5Lbh3LhA8atY738AGwMWpxFKQ1VNeci_r7SY"
	testKeyEC512PubY = "AXbmSeogEiDlDwz0Gc670YYByFzC3c7tEMeap7CckkOtuN0Yaebqtv42dZH0MiikzSco2ROhagX-HOinG7gB78ax"

	// echo -n '{"crv":"P-256","kty":"EC","x":"<testKeyECPubX>","y":"<testKeyECPubY>"}' | \
	// openssl dgst -binary -sha256 | b64raw
	testKeyECThumbprint = "zedj-Bd1Zshp8KLePv2MB-lJ_Hagp7wAwdkA0NUTniU"
)

var (
	testKey      *rsa.PrivateKey
	testKeyEC    *ecdsa.PrivateKey
	testKeyEC384 *ecdsa.PrivateKey
	testKeyEC512 *ecdsa.PrivateKey
)

func init() {
	testKey = parseRSA(testKeyPEM, "testKeyPEM")
	testKeyEC = parseEC(testKeyECPEM, "testKeyECPEM")
	testKeyEC384 = parseEC(testKeyEC384PEM, "testKeyEC384PEM")
	testKeyEC512 = parseEC(testKeyEC512PEM, "testKeyEC512PEM")
}

func decodePEM(s, name string) []byte {
	d, _ := pem.Decode([]byte(s))
	if d == nil {
		panic("no block found in " + name)
	}
	return d.Bytes
}

func parseRSA(s, name string) *rsa.PrivateKey {
	b := decodePEM(s, name)
	k, err := x509.ParsePKCS1PrivateKey(b)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", name, err))
	}
	return k
}

func parseEC(s, name string) *ecdsa.PrivateKey {
	b := decodePEM(s, name)
	k, err := x509.ParseECPrivateKey(b)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", name, err))
	}
	return k
}

func TestJWSEncodeJSON(t *testing.T) {
	claims := struct{ Msg string }{"Hello JWS"}
	// JWS signed with testKey and "nonce" as the nonce value
	// JSON-serialized JWS fields are split for easier testing
	const (
		// {"alg":"RS256","jwk":{"e":"AQAB","kty":"RSA","n":"..."},"nonce":"nonce","url":"url"}
		protected = "eyJhbGciOiJSUzI1NiIsImp3ayI6eyJlIjoiQVFBQiIsImt0eSI6" +
			"IlJTQSIsIm4iOiI0eGdaM2VSUGt3b1J2eTdxZVJVYm1NRGUwVi14" +
			"SDllV0xkdTBpaGVlTGxybUQybXFXWGZQOUllU0tBcGJuMzRnOFR1" +
			"QVM5ZzV6aHE4RUxRM2ttanItS1Y4NkdBTWdJNlZBY0dscTNRcnpw" +
			"VENmXzMwQWI3LXphd3JmUmFGT05hMUh3RXpQWTFLSG5HVmt4SmM4" +
			"NWdOa3dZSTlTWTJSSFh0dmxuM3pzNXdJVE5yZG9zcUVYZWFJa1ZZ" +
			"QkVoYmhOdTU0cHAza3hvNlR1V0xpOWU2cFhlV2V0RXdtbEJ3dFda" +
			"bFBvaWIyajNUeExCa3NLWmZveUZ5ZWszODBtSGdKQXVtUV9JMmZq" +
			"ajk4Xzk3bWszaWhPWTRBZ1ZkQ0RqMXpfR0NvWmtHNVJxN25iQ0d5" +
			"b3N5S1d5RFgwMFpzLW5OcVZob0xlSXZYQzRubldkSk1aNnJvZ3h5" +
			"UVEifSwibm9uY2UiOiJub25jZSIsInVybCI6InVybCJ9"
		// {"Msg":"Hello JWS"}
		payload = "eyJNc2ciOiJIZWxsbyBKV1MifQ"
		// printf '<protected>.<payload>' | openssl dgst -binary -sha256 -sign testKey | b64raw
		signature = "YFyl_xz1E7TR-3E1bIuASTr424EgCvBHjt25WUFC2VaDjXYV0Rj_" +
			"Hd3dJ_2IRqBrXDZZ2n4ZeA_4mm3QFwmwyeDwe2sWElhb82lCZ8iX" +
			"uFnjeOmSOjx-nWwPa5ibCXzLq13zZ-OBV1Z4oN_TuailQeRoSfA3" +
			"nO8gG52mv1x2OMQ5MAFtt8jcngBLzts4AyhI6mBJ2w7Yaj3ZCriq" +
			"DWA3GLFvvHdW1Ba9Z01wtGT2CuZI7DUk_6Qj1b3BkBGcoKur5C9i" +
			"bUJtCkABwBMvBQNyD3MmXsrRFRTgvVlyU_yMaucYm7nmzEr_2PaQ" +
			"50rFt_9qOfJ4sfbLtG1Wwae57BQx1g"
	)

	b, err := jwsEncodeJSON(claims, testKey, noKeyID, "nonce", "url")
	if err != nil {
		t.Fatal(err)
	}
	var jws struct{ Protected, Payload, Signature string }
	if err := json.Unmarshal(b, &jws); err != nil {
		t.Fatal(err)
	}
	if jws.Protected != protected {
		t.Errorf("protected:\n%s\nwant:\n%s", jws.Protected, protected)
	}
	if jws.Payload != payload {
		t.Errorf("payload:\n%s\nwant:\n%s", jws.Payload, payload)
	}
	if jws.Signature != signature {
		t.Errorf("signature:\n%s\nwant:\n%s", jws.Signature, signature)
	}
}

func TestJWSEncodeKID(t *testing.T) {
	kid := keyID("https://example.org/account/1")
	claims := struct{ Msg string }{"Hello JWS"}
	// JWS signed with testKeyEC
	const (
		// {"alg":"ES256","kid":"https://example.org/account/1","nonce":"nonce","url":"url"}
		protected = "eyJhbGciOiJFUzI1NiIsImtpZCI6Imh0dHBzOi8vZXhhbXBsZS5" +
			"vcmcvYWNjb3VudC8xIiwibm9uY2UiOiJub25jZSIsInVybCI6InVybCJ9"
		// {"Msg":"Hello JWS"}
		payload = "eyJNc2ciOiJIZWxsbyBKV1MifQ"
	)

	b, err := jwsEncodeJSON(claims, testKeyEC, kid, "nonce", "url")
	if err != nil {
		t.Fatal(err)
	}
	var jws struct{ Protected, Payload, Signature string }
	if err := json.Unmarshal(b, &jws); err != nil {
		t.Fatal(err)
	}
	if jws.Protected != protected {
		t.Errorf("protected:\n%s\nwant:\n%s", jws.Protected, protected)
	}
	if jws.Payload != payload {
		t.Errorf("payload:\n%s\nwant:\n%s", jws.Payload, payload)
	}

	sig, err := base64.RawURLEncoding.DecodeString(jws.Signature)
	if err != nil {
		t.Fatalf("jws.Signature: %v", err)
	}
	r, s := big.NewInt(0), big.NewInt(0)
	r.SetBytes(sig[:len(sig)/2])
	s.SetBytes(sig[len(sig)/2:])
	h := sha256.Sum256([]byte(protected + "." + payload))
	if !ecdsa.Verify(testKeyEC.Public().(*ecdsa.PublicKey), h[:], r, s) {
		t.Error("invalid signature")
	}
}

func TestJWSEncodeJSONEC(t *testing.T) {
	tt := []struct {
		key      *ecdsa.PrivateKey
		x, y     string
		alg, crv string
	}{
		{testKeyEC, testKeyECPubX, testKeyECPubY, "ES256", "P-256"},
		{testKeyEC384, testKeyEC384PubX, testKeyEC384PubY, "ES384", "P-384"},
		{testKeyEC512, testKeyEC512PubX, testKeyEC512PubY, "ES512", "P-521"},
	}
	for i, test := range tt {
		claims := struct{ Msg string }{"Hello JWS"}
		b, err := jwsEncodeJSON(claims, test.key, noKeyID, "nonce", "url")
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		var jws struct{ Protected, Payload, Signature string }
		if err := json.Unmarshal(b, &jws); err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		b, err = base64.RawURLEncoding.DecodeString(jws.Protected)
		if err != nil {
			t.Errorf("%d: jws.Protected: %v", i, err)
		}
		var head struct {
			Alg   string
			Nonce string
			URL   string `json:"url"`
			KID   string `json:"kid"`
			JWK   struct {
				Crv string
				Kty string
				X   string
				Y   string
			} `json:"jwk"`
		}
		if err := json.Unmarshal(b, &head); err != nil {
			t.Errorf("%d: jws.Protected: %v", i, err)
		}
		if head.Alg != test.alg {
			t.Errorf("%d: head.Alg = %q; want %q", i, head.Alg, test.alg)
		}
		if head.Nonce != "nonce" {
			t.Errorf("%d: head.Nonce = %q; want nonce", i, head.Nonce)
		}
		if head.URL != "url" {
			t.Errorf("%d: head.URL = %q; want 'url'", i, head.URL)
		}
		if head.KID != "" {
			// We used noKeyID in jwsEncodeJSON: expect no kid value.
			t.Errorf("%d: head.KID = %q; want empty", i, head.KID)
		}
		if head.JWK.Crv != test.crv {
			t.Errorf("%d: head.JWK.Crv = %q; want %q", i, head.JWK.Crv, test.crv)
		}
		if head.JWK.Kty != "EC" {
			t.Errorf("%d: head.JWK.Kty = %q; want EC", i, head.JWK.Kty)
		}
		if head.JWK.X != test.x {
			t.Errorf("%d: head.JWK.X = %q; want %q", i, head.JWK.X, test.x)
		}
		if head.JWK.Y != test.y {
			t.Errorf("%d: head.JWK.Y = %q; want %q", i, head.JWK.Y, test.y)
		}
	}
}

type customTestSigner struct {
	sig []byte
	pub crypto.PublicKey
}

func (s *customTestSigner) Public() crypto.PublicKey { return s.pub }
func (s *customTestSigner) Sign(io.Reader, []byte, crypto.SignerOpts) ([]byte, error) {
	return s.sig, nil
}

func TestJWSEncodeJSONCustom(t *testing.T) {
	claims := struct{ Msg string }{"hello"}
	const (
		// printf '{"Msg":"hello"}' | b64raw
		payload = "eyJNc2ciOiJoZWxsbyJ9"
		// printf 'testsig' | b64raw
		testsig = "dGVzdHNpZw"

		// printf '{"alg":"ES256","jwk":{"crv":"P-256","kty":"EC","x":<testKeyECPubY>,"y":<testKeyECPubY>},"nonce":"nonce","url":"url"}' | b64raw
		es256phead = "eyJhbGciOiJFUzI1NiIsImp3ayI6eyJjcnYiOiJQLTI1NiIsImt0" +
			"eSI6IkVDIiwieCI6IjVsaEV1ZzV4SzR4QkRaMm5BYmF4THRhTGl2" +
			"ODVieEo3ZVBkMWRrTzIzSFEiLCJ5IjoiNGFpSzcyc0JlVUFHa3Yw" +
			"VGFMc213b2tZVVl5TnhHc1M1RU1JS3dzTklLayJ9LCJub25jZSI6" +
			"Im5vbmNlIiwidXJsIjoidXJsIn0"

		// {"alg":"RS256","jwk":{"e":"AQAB","kty":"RSA","n":"..."},"nonce":"nonce","url":"url"}
		rs256phead = "eyJhbGciOiJSUzI1NiIsImp3ayI6eyJlIjoiQVFBQiIsImt0eSI6" +
			"IlJTQSIsIm4iOiI0eGdaM2VSUGt3b1J2eTdxZVJVYm1NRGUwVi14" +
			"SDllV0xkdTBpaGVlTGxybUQybXFXWGZQOUllU0tBcGJuMzRnOFR1" +
			"QVM5ZzV6aHE4RUxRM2ttanItS1Y4NkdBTWdJNlZBY0dscTNRcnpw" +
			"VENmXzMwQWI3LXphd3JmUmFGT05hMUh3RXpQWTFLSG5HVmt4SmM4" +
			"NWdOa3dZSTlTWTJSSFh0dmxuM3pzNXdJVE5yZG9zcUVYZWFJa1ZZ" +
			"QkVoYmhOdTU0cHAza3hvNlR1V0xpOWU2cFhlV2V0RXdtbEJ3dFda" +
			"bFBvaWIyajNUeExCa3NLWmZveUZ5ZWszODBtSGdKQXVtUV9JMmZq" +
			"ajk4Xzk3bWszaWhPWTRBZ1ZkQ0RqMXpfR0NvWmtHNVJxN25iQ0d5" +
			"b3N5S1d5RFgwMFpzLW5OcVZob0xlSXZYQzRubldkSk1aNnJvZ3h5" +
			"UVEifSwibm9uY2UiOiJub25jZSIsInVybCI6InVybCJ9"
	)

	tt := []struct {
		alg, phead string
		pub        crypto.PublicKey
	}{
		{"ES256", es256phead, testKeyEC.Public()},
		{"RS256", rs256phead, testKey.Public()},
	}
	for _, tc := range tt {
		tc := tc
		t.Run(tc.alg, func(t *testing.T) {
			signer := &customTestSigner{
				sig: []byte("testsig"),
				pub: tc.pub,
			}
			b, err := jwsEncodeJSON(claims, signer, noKeyID, "nonce", "url")
			if err != nil {
				t.Fatal(err)
			}
			var j struct{ Protected, Payload, Signature string }
			if err := json.Unmarshal(b, &j); err != nil {
				t.Fatal(err)
			}
			if j.Protected != tc.phead {
				t.Errorf("j.Protected = %q\nwant %q", j.Protected, tc.phead)
			}
			if j.Payload != payload {
				t.Errorf("j.Payload = %q\nwant %q", j.Payload, payload)
			}
			if j.Signature != testsig {
				t.Errorf("j.Signature = %q\nwant %q", j.Signature, testsig)
			}
		})
	}
}

func TestJWKThumbprintRSA(t *testing.T) {
	// Key example from RFC 7638
	const base64N = "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAt" +
		"VT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn6" +
		"4tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FD" +
		"W2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n9" +
		"1CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINH" +
		"aQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
	const base64E = "AQAB"
	const expected = "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"

	b, err := base64.RawURLEncoding.DecodeString(base64N)
	if err != nil {
		t.Fatalf("Error parsing example key N: %v", err)
	}
	n := new(big.Int).SetBytes(b)

	b, err = base64.RawURLEncoding.DecodeString(base64E)
	if err != nil {
		t.Fatalf("Error parsing example key E: %v", err)
	}
	e := new(big.Int).SetBytes(b)

	pub := &rsa.PublicKey{N: n, E: int(e.Uint64())}
	th, err := JWKThumbprint(pub)
	if err != nil {
		t.Error(err)
	}
	if th != expected {
		t.Errorf("thumbprint = %q; want %q", th, expected)
	}
}

func TestJWKThumbprintEC(t *testing.T) {
	// Key example from RFC 7520
	// expected was computed with
	// printf '{"crv":"P-521","kty":"EC","x":"<base64X>","y":"<base64Y>"}' | \
	// openssl dgst -binary -sha256 | b64raw
	const (
		base64X = "AHKZLLOsCOzz5cY97ewNUajB957y-C-U88c3v13nmGZx6sYl_oJXu9A5RkT" +
			"KqjqvjyekWF-7ytDyRXYgCF5cj0Kt"
		base64Y = "AdymlHvOiLxXkEhayXQnNCvDX4h9htZaCJN34kfmC6pV5OhQHiraVySsUda" +
			"QkAgDPrwQrJmbnX9cwlGfP-HqHZR1"
		expected = "dHri3SADZkrush5HU_50AoRhcKFryN-PI6jPBtPL55M"
	)

	b, err := base64.RawURLEncoding.DecodeString(base64X)
	if err != nil {
		t.Fatalf("Error parsing example key X: %v", err)
	}
	x := new(big.Int).SetBytes(b)

	b, err = base64.RawURLEncoding.DecodeString(base64Y)
	if err != nil {
		t.Fatalf("Error parsing example key Y: %v", err)
	}
	y := new(big.Int).SetBytes(b)

	pub := &ecdsa.PublicKey{Curve: elliptic.P521(), X: x, Y: y}
	th, err := JWKThumbprint(pub)
	if err != nil {
		t.Error(err)
	}
	if th != expected {
		t.Errorf("thumbprint = %q; want %q", th, expected)
	}
}

func TestJWKThumbprintErrUnsupportedKey(t *testing.T) {
	_, err := JWKThumbprint(struct{}{})
	if err != ErrUnsupportedKey {
		t.Errorf("err = %q; want %q", err, ErrUnsupportedKey)
	}
}
