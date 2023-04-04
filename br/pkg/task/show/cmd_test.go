// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package show_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/task/show"
	"github.com/stretchr/testify/require"
)

func TestXxx(t *testing.T) {
	cfg := show.Config{
		Storage: "s3://breeze/tpcc-600",
		BackendCfg: storage.BackendOptions{
			S3: storage.S3BackendOptions{
				Endpoint:        "http://10.2.5.17:9000",
				AccessKey:       "minioadmin",
				SecretAccessKey: "minioadmin",
				ForcePathStyle:  true,
			},
		},
		Cipher: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}

	req := require.New(t)
	ctx := context.Background()
	exec, err := show.CreateExec(ctx, cfg)
	req.NoError(err)
	items, err := exec.Read(ctx)
	req.NoError(err)
	data, err := json.Marshal(items)
	req.NoError(err)
	fmt.Println(string(data))
	req.FailNow("meow? meow!")
}
