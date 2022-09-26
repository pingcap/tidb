// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	flagDatabase = "database"
	flagTable    = "table"
	flagPort     = "port"
	flagWorker   = "worker"
)

var rootCmd *cobra.Command

func main() {
	rootCmd = &cobra.Command{}
	rootCmd.Flags().StringP(flagDatabase, "B", "s3", "Database to import")
	rootCmd.Flags().StringP(flagTable, "T", "t", "Table to import")
	rootCmd.Flags().IntP(flagPort, "P", 4000, "TCP/IP port to connect to")
	rootCmd.Flags().IntP(flagWorker, "w", 16, "Workers to import synchronously")

	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		database, err := cmd.Flags().GetString(flagDatabase)
		if err != nil {
			return errors.Trace(err)
		}
		table, err := cmd.Flags().GetString(flagTable)
		if err != nil {
			return errors.Trace(err)
		}
		port, err := cmd.Flags().GetInt(flagPort)
		if err != nil {
			return errors.Trace(err)
		}
		worker, err := cmd.Flags().GetInt(flagWorker)
		if err != nil {
			return errors.Trace(err)
		}

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4", "root", "", "127.0.0.1", port, database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return errors.Trace(err)
		}

		tableTemp := `CREATE TABLE IF NOT EXISTS %s (
	   a VARCHAR(11)
)`
		_, err = db.Exec(fmt.Sprintf(tableTemp, table))
		if err != nil {
			return errors.Trace(err)
		}

		query := fmt.Sprintf("insert into %s values('aaaaaaaaaa')", table) // nolint:gosec
		for i := 1; i < 10000; i++ {
			query += ",('aaaaaaaaaa')"
		}
		ch := make(chan struct{}, worker)
		for i := 0; i < worker; i++ {
			ch <- struct{}{}
		}
		var eg *errgroup.Group
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		eg, ctx = errgroup.WithContext(ctx)
		for i := 0; i < 500; i++ {
			if ctx.Err() != nil {
				break
			}
			<-ch
			eg.Go(func() error {
				_, err := db.ExecContext(ctx, query)
				if err != nil {
					cancel()
					return errors.Trace(err)
				}
				ch <- struct{}{}
				return nil
			})
		}
		return eg.Wait()
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("fail to import data, err: %v", err)
		os.Exit(2)
	}
}
