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
	return runEncode(ctx, cfg) // Assuming runEncode will be similarly modified to accept Base64ifyConfig
}

func runEncode(ctx context.Context, cfg Base64ifyConfig) error {
	s, err := objstore.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	if cfg.LoadCerd {
		_, err := objstore.New(ctx, s, &storeapi.Options{
			SendCredentials: true,
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, color.HiRedString("Credientials are encoded to the base64 string. DON'T share this with untrusted people!"))
	}

	sBytes, err := s.Marshal()
	if err != nil {
		return err
	}
	fmt.Println(base64.StdEncoding.EncodeToString(sBytes))
	return nil
}
