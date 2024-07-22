package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
)

// statusOK make a string like <green>●</green> <bold>{message}</bold>
func statusOK(message string) string {
	return color.GreenString("●") + color.New(color.Bold).Sprintf(" %s", message)
}

func RunListMigrations(ctx context.Context, cfg ListMigrationConfig) error {
	backend, err := storage.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	st, err := storage.Create(ctx, backend, false)
	if err != nil {
		return err
	}
	ext := stream.MigerationExtension(st)
	migs, err := ext.Load(ctx)
	if err != nil {
		return err
	}
	if cfg.JSONOutput {
		if err := json.NewEncoder(os.Stdout).Encode(migs); err != nil {
			return err
		}
	} else {
		console := glue.ConsoleOperations{ConsoleGlue: glue.StdIOGlue{}}
		console.Println(statusOK(fmt.Sprintf("Total %d Migrations.", len(migs.Layers)+1)))
		console.Printf(">   BASE   <\n")
		tbl := console.CreateTable()
		stream.AddMigrationToTable(migs.Base, tbl)
		tbl.Print()
		for _, t := range migs.Layers {
			console.Printf("> %08d <\n", t.SeqNum)
			tbl := console.CreateTable()
			stream.AddMigrationToTable(&t.Content, tbl)
			tbl.Print()
		}
	}
	return nil
}
