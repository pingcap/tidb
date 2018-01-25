// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}

func doInsert(table *table, db *sql.DB, count int) {
	sqls, err := genRowDatas(table, count)
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}

	for _, sql := range sqls {
		_, err = txn.Exec(sql)
		if err != nil {
			log.Fatalf(errors.ErrorStack(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}
}

func doJob(table *table, db *sql.DB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	count := 0
	for range jobChan {
		count++
		if count == batch {
			doInsert(table, db, count)
			count = 0
		}
	}

	if count > 0 {
		doInsert(table, db, count)
		count = 0
	}

	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}

	close(doneChan)

	now := time.Now()
	seconds := now.Unix() - start.Unix()

	tps := int64(-1)
	if seconds > 0 {
		tps = int64(jobCount) / seconds
	}

	fmt.Printf("[importer]total %d cases, cost %d seconds, tps %d, start %s, now %s\n", jobCount, seconds, tps, start, now)
}

func doProcess(table *table, dbs []*sql.DB, jobCount int, workerCount int, batch int) {
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	start := time.Now()
	go addJobs(jobCount, jobChan)

	for i := 0; i < workerCount; i++ {
		go doJob(table, dbs[i], batch, jobChan, doneChan)
	}

	doWait(doneChan, start, jobCount, workerCount)
}
