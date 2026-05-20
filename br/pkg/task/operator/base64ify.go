package operator

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

func Base64ify(ctx context.Context, cfg Base64ifyConfig) error {
	return runEncode(ctx, cfg)
}

func runEncode(ctx context.Context, cfg Base64ifyConfig) error {
	s, err := objstore.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}

	// objstore.New clears credentials when SendCredentials is false. Preserve
	// credentials that were explicitly provided in the storage config, while still
	// avoiding loading ambient credentials unless --load-creds is set.
	var s3AccessKey, s3SecretAccessKey, s3SessionToken, gcsCredentialsBlob string
	if !cfg.LoadCerd {
		if s3 := s.GetS3(); s3 != nil {
			s3AccessKey = s3.AccessKey
			s3SecretAccessKey = s3.SecretAccessKey
			s3SessionToken = s3.SessionToken
		}
		if gcs := s.GetGcs(); gcs != nil {
			gcsCredentialsBlob = gcs.CredentialsBlob
		}
	}

	store, err := objstore.New(ctx, s, &storeapi.Options{
		SendCredentials:          cfg.LoadCerd,
		CheckS3ObjectLockOptions: true,
	})
	if err != nil {
		return err
	}
	store.Close()

	if !cfg.LoadCerd {
		if s3 := s.GetS3(); s3 != nil {
			s3.AccessKey = s3AccessKey
			s3.SecretAccessKey = s3SecretAccessKey
			s3.SessionToken = s3SessionToken
		}
		if gcs := s.GetGcs(); gcs != nil {
			gcs.CredentialsBlob = gcsCredentialsBlob
		}
	}

	if cfg.LoadCerd {
		fmt.Fprintln(os.Stderr, color.HiRedString("Credientials are encoded to the base64 string. DON'T share this with untrusted people!"))
	}

	sBytes, err := s.Marshal()
	if err != nil {
		return err
	}
	fmt.Println(base64.StdEncoding.EncodeToString(sBytes))
	return nil
}
