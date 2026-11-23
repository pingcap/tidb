// Copyright 2026 PingCAP, Inc.
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

package common

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

func GetStorage(
	ctx context.Context,
	storageName string,
	backendOptions objstore.BackendOptions,
	noCreds bool,
	sendCreds bool,
) (*backuppb.StorageBackend, storeapi.Storage, error) {
	u, err := objstore.ParseBackend(storageName, &backendOptions)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	s, err := objstore.New(ctx, u, storageOpts(noCreds, sendCreds))
	if err != nil {
		return nil, nil, errors.Annotate(err, "create storage failed")
	}
	return u, s, nil
}

func storageOpts(noCreds bool, sendCreds bool) *storeapi.Options {
	return &storeapi.Options{
		NoCredentials:   noCreds,
		SendCredentials: sendCreds,
	}
}

func DecodeBackupMeta(metaData []byte, cipherInfo *backuppb.CipherInfo) (*backuppb.BackupMeta, error) {
	ci := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	if cipherInfo != nil {
		ci = *cipherInfo
	}

	var iv []byte
	if ci.CipherType != encryptionpb.EncryptionMethod_PLAINTEXT {
		iv = metaData[:metautil.CrypterIvLen]
	}
	decryptBackupMeta, err := utils.Decrypt(metaData[len(iv):], &ci, iv)
	if err != nil {
		return nil, errors.Annotate(err, "decrypt failed with wrong key")
	}

	backupMeta := &backuppb.BackupMeta{}
	if err = proto.Unmarshal(decryptBackupMeta, backupMeta); err != nil {
		return nil, errors.Annotate(err, "parse backupmeta failed because of wrong aes cipher")
	}
	return backupMeta, nil
}

func ReadBackupMetaFromStorage(
	ctx context.Context,
	fileName string,
	storage storeapi.Storage,
	cipherInfo *backuppb.CipherInfo,
) (*backuppb.BackupMeta, error) {
	metaData, err := storage.ReadFile(ctx, fileName)
	if err != nil {
		return nil, errors.Annotate(err, "load backupmeta failed")
	}
	return DecodeBackupMeta(metaData, cipherInfo)
}
