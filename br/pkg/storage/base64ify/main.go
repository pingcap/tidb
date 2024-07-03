package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/spf13/pflag"
)

func main() {
	flags := new(pflag.FlagSet)
	flags.StringP("storage", "s", "", "the external storage input.")
	flags.Bool("load-creds", false, "whether loading the credientials from current environment and marshal them to the base64 string. [!]")
	storage.DefineFlags(flags)

	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "failed: %s\n", err)
		os.Exit(1)
	}
	if err := run(flags); err != nil {
		panic(err)
	}
}

func run(flags *pflag.FlagSet) error {
	opts := &storage.BackendOptions{}
	if err := opts.ParseFromFlags(flags); err != nil {
		return err
	}
	url, err := flags.GetString("storage")
	if err != nil {
		return err
	}
	loadCred, err := flags.GetBool("load-creds")
	if err != nil {
		return err
	}

	s, err := storage.ParseBackend(url, opts)
	if err != nil {
		return err
	}
	if loadCred {
		_, err := storage.New(context.Background(), s, &storage.ExternalStorageOptions{
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
