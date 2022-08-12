#! /bin/bash
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script used for generating tls file for unit test
# execute: cd pkg/utils/tls_test && sh generate.sh && cd -

    cat - > "ipsan.cnf" <<EOF
[dn]
CN = localhost
[req]
distinguished_name = dn
[EXT]
subjectAltName = @alt_names
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth,serverAuth
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF


openssl ecparam -out "ca.key" -name prime256v1 -genkey
openssl req -new -batch -sha256 -subj '/CN=localhost' -key "ca.key" -out "ca.csr"
openssl x509 -req -sha256 -days 100000 -in "ca.csr" -signkey "ca.key" -out "ca.pem" 2> /dev/null

for role in server client1 client2; do
    openssl ecparam -out "$role.key" -name prime256v1 -genkey
    openssl req -new -batch -sha256 -subj "/CN=${role}" -key "$role.key" -out "$role.csr"
    openssl x509 -req -sha256 -days 100000 -extensions EXT -extfile "ipsan.cnf" -in "$role.csr" -CA "ca.pem" -CAkey "ca.key" -CAcreateserial -out "$role.pem" 2> /dev/null
done
