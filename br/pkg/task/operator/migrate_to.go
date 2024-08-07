package operator

import (
	"context"

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
		return migs.Layers[0].SeqNum, true
	}
	if cfg.Base {
		return 0, true
	}
	return cfg.MigrateTo, true
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
	targetMig := migs.MergeTo(targetVersion)
	tbl := cx.console.CreateTable()
	stream.AddMigrationToTable(targetMig, tbl)
	cx.console.Println("The migration going to be executed will be like: ")
	tbl.Print()

	if !cx.console.PromptBool("continue?") {
		return errors.Annotatef(context.Canceled, "the user aborted the operation")
	}
	return nil
}

func (cx migrateToCtx) dryRun(ctx context.Context, targetVersion int) error {
	var (
		est     = cx.est
		console = cx.console
		estBase stream.MergeAndMigratedTo
		effects []storage.Effect
	)
	effects = est.DryRun(func(me stream.MigrationExt) {
		estBase = me.MergeAndMigrateTo(ctx, targetVersion)
	})

	tbl := console.CreateTable()
	stream.AddMigrationToTable(estBase.NewBase, tbl)
	console.Println("The new BASE migration will be like: ")
	tbl.Print()
	file, err := storage.SaveJSONEffectsToTmp(effects)
	if err != nil {
		return errors.Trace(err)
	}
	console.Printf("%s effects will happen in the external storage, you may check them in %s\n",
		color.HiRedString("%d", len(effects)),
		color.New(color.Bold).Sprint(file))
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

	console := glue.ConsoleOperations{ConsoleGlue: glue.StdIOGlue{}}

	est := stream.MigerationExtension(st)
	est.Hooks = stream.NewProgressBarHooks(console)
	migs, err := est.Load(ctx)
	if err != nil {
		return err
	}

	cx := migrateToCtx{
		cfg:     cfg,
		console: console,
		est:     est,
	}

	targetVersion, ok := cfg.getTargetVersion(migs)
	if !ok {
		console.Printf("No recent migration found. Skipping.")
	}

	if !cfg.Yes {
		err := cx.estlimateByLog(migs, targetVersion)
		if err != nil {
			return err
		}
	}

	if cfg.DryRun {
		// Note: this shouldn't be committed even user requires,
		// as once we encounter error during committing,
		// we won't record the failure to the new BASE migration.
		// Then those files failed to be deleted will leak.
		return cx.dryRun(ctx, targetVersion)
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
