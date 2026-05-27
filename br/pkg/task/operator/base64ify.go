package operator

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func Base64ify(ctx context.Context, cfg Base64ifyConfig) error {
	return runEncode(ctx, cfg)
}

func runEncode(ctx context.Context, cfg Base64ifyConfig) error {
	s, err := storage.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}

	store, err := objstore.New(ctx, s, &storeapi.Options{
		SendCredentials:          cfg.LoadCerd,
		CheckS3ObjectLockOptions: true,
	})
	if err != nil {
		return err
	}
	store.Close()

	if cfg.LoadCerd {
<<<<<<< HEAD
		_, err := storage.New(ctx, s, &storage.ExternalStorageOptions{
			SendCredentials: true,
		})
		if err != nil {
			return err
		}
=======
>>>>>>> beb12a7923d (br: encode S3 object lock status in base64ify output (#68552))
		fmt.Fprintln(os.Stderr, color.HiRedString("Credientials are encoded to the base64 string. DON'T share this with untrusted people!"))
	}

	sBytes, err := s.Marshal()
	if err != nil {
		return err
	}
	fmt.Println(base64.StdEncoding.EncodeToString(sBytes))
	return nil
}
