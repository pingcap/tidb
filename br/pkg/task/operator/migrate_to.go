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

type migrateToCtx struct {
	cfg     MigrateToConfig
	console glue.ConsoleOperations
	est     stream.MigrationExt
}

func (cx migrateToCtx) printErr(errs []error, msg string) {
	if len(errs) > 0 {
		cx.console.Println(msg)
		for _, w := range errs {
			cx.console.Printf("- %s\n", color.HiRedString(w.Error()))
		}
	}
}

func (cx migrateToCtx) estlimateByLog(migs stream.Migrations, targetVersion int) error {
	targetMig := cx.est.MergeTo(migs, targetVersion)
	tbl := cx.console.CreateTable()
	stream.AddMigrationToTable(targetMig, tbl)
	cx.console.Println("The migration going to be executed will be like: ")
	tbl.Print()

	if !cx.console.PromptBool("continue?") {
		return errors.Annotatef(context.Canceled, "the user aborted the operation")
	}
	return nil

}

func (cx migrateToCtx) dryRun(ctx context.Context, migs stream.Migrations, targetVersion int) error {
	est := cx.est
	console := cx.console
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
		color.New(color.Bold).Sprint(file.Name()))
	cx.printErr(estBase.Warnings, "The following errors happened during estimating: ")

	return nil
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

	cx := migrateToCtx{
		cfg:     cfg,
		console: console,
		est:     est,
	}

	targetVersion, ok := cfg.getTargetVersion(migs)
	if !ok {
		console.Printf("No recent migration found. Skipping.")
	}

	if cfg.DryRun {
		return cx.dryRun(ctx, migs, targetVersion)
	}
	if !cfg.Yes {
		err := cx.estlimateByLog(migs, targetVersion)
		if err != nil {
			return err
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
