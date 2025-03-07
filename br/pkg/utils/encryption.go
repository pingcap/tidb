// Copyright 2024 PingCAP, Inc.
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

package utils

import (
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/util/encrypt"
)

func Decrypt(content []byte, cipher *backuppb.CipherInfo, iv []byte) ([]byte, error) {
	if len(content) == 0 || cipher == nil {
		return content, nil
	}

	switch cipher.CipherType {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return content, nil
	case encryptionpb.EncryptionMethod_AES128_CTR,
		encryptionpb.EncryptionMethod_AES192_CTR,
		encryptionpb.EncryptionMethod_AES256_CTR:
		return encrypt.AESDecryptWithCTR(content, cipher.CipherKey, iv)
	default:
		return content, errors.Annotatef(berrors.ErrInvalidArgument, "cipher type invalid %s", cipher.CipherType)
	}
}

func IsEffectiveEncryptionMethod(method encryptionpb.EncryptionMethod) bool {
	return method != encryptionpb.EncryptionMethod_UNKNOWN && method != encryptionpb.EncryptionMethod_PLAINTEXT
}
