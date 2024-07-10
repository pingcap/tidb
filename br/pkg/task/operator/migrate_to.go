package operator

import (
	"context"
	"os"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
)

func (cfg *MigrateToConfig) getTargetVersion(migs stream.Migrations) (int, bool) {
	if cfg.Recent {
		if len(migs.Layers) == 0 {
			return 0, false
		}
		return migs.Layers[0].ID, true
	} else if cfg.Base {
		return 0, true
	} else {
		return cfg.MigrateTo, true
	}
}

func RunMigrateTo(ctx context.Context, cfg MigrateToConfig) error {
	if err := cfg.Verify(); err != nil {
		return err
	}

	backend, err := storage.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	st, err := storage.Create(context.Background(), backend, false)
	if err != nil {
		return err
	}

	est := stream.MigerationExtension(st)
	migs, err := est.Load(ctx)
	if err != nil {
		return err
	}
	console := glue.ConsoleOperations{ConsoleGlue: glue.StdIOGlue{}}
	targetVersion, ok := cfg.getTargetVersion(migs)
	if !ok {
		console.Printf("No recent migration found. Skipping.")
	}

	if !cfg.Yes {
		targetMig := est.MergeTo(migs, targetVersion)
		estBase, effects := est.EstimateEffectFor(ctx, targetMig)
		tbl := console.CreateTable()
		stream.AddMigrationToTable(estBase.NewBase, tbl)
		console.Println("The new BASE migration will be like: ")
		tbl.Print()
		file, err := os.CreateTemp(os.TempDir(), "tidb_br_migrate_to_*.json")
		if err != nil {
			return errors.Trace(err)
		}
		if err := storage.JSONEffects(effects, file); err != nil {
			return errors.Trace(err)
		}
		console.Printf("%s effects will happen in the external storage, you may check them in %s\n",
			color.HiRedString("%d", len(effects)),
			color.New(color.Bold).Sprint( file.Name()))
		if len(estBase.Warnings) > 0 {
			console.Printf("The following errors happened during estimating: ")
			for _, w := range estBase.Warnings {
				console.Printf("- %s\n", color.HiRedString(w.Error()))
			}
		}
		if !console.PromptBool("continue?") {
			console.Println("aborted.")
			return nil
		}
	}

	result := est.MergeAndMigrateTo(ctx, targetVersion)
	if len(result.Warnings) > 0 {
		console.Printf("The following errors happened, you may re-execute to retry: ")
		for _, w := range result.Warnings {
			console.Printf("- %s\n", color.HiRedString(w.Error()))
		}
	}

	return nil
}
