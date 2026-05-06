// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	if os.Getenv("FAKE_OAUTH_ALLOW_INSECURE") != "true" {
		log.Fatal("fake-oauth: this tool issues tokens without authentication and must NOT be used in production. " +
			"Set FAKE_OAUTH_ALLOW_INSECURE=true to explicitly enable it in local development environments only.")
	}
	log.Println("WARNING: fake-oauth is running. This service issues tokens without authentication. FOR LOCAL DEVELOPMENT USE ONLY.")
	http.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"access_token": "ok", "token_type":"service_account", "expires_in":3600}`))
	})
	certFile := os.Getenv("FAKE_OAUTH_TLS_CERT")
	keyFile := os.Getenv("FAKE_OAUTH_TLS_KEY")
	if certFile == "" || keyFile == "" {
		log.Fatal("fake-oauth: FAKE_OAUTH_TLS_CERT and FAKE_OAUTH_TLS_KEY must be set to provide TLS certificate and key files.")
	}
	_ = http.ListenAndServeTLS(":5000", certFile, keyFile, nil)
}
