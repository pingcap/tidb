package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
)

func Base64ify(ctx context.Context, cfg Base64ifyConfig) error {
	if cfg.Revert {
		return runRevert(ctx, cfg)
	}
	return runEncode(ctx, cfg) // Assuming runEncode will be similarly modified to accept Base64ifyConfig
}

func runRevert(ctx context.Context, cfg Base64ifyConfig) error {
	b64 := cfg.StorageURI
	data, err := base64.RawStdEncoding.DecodeString(b64)
	if err != nil {
		return errors.Trace(err)
	}

	backend := backup.StorageBackend{}
	if err := backend.Unmarshal(data); err != nil {
		return errors.Trace(err)
	}

	s, err := storage.Create(ctx, &backend, false)
	if err != nil {
		return errors.Trace(err)
	}
	type Output struct {
		Uri string `json:"uri"`
		backup.StorageBackend
	}
	if err := backend.Unmarshal(data); err != nil {
		return errors.Trace(err)
	}

	out := Output{
		Uri:            s.URI(),
		StorageBackend: backend,
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func runEncode(ctx context.Context, cfg Base64ifyConfig) error {
	s, err := storage.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	if cfg.LoadCerd {
		_, err := storage.New(ctx, s, &storage.ExternalStorageOptions{
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
