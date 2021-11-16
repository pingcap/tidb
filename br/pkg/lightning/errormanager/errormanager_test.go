// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errormanager

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

var _ = Suite(errorManagerSuite{})

func TestErrorManager(t *testing.T) {
	TestingT(t)
}

type errorManagerSuite struct{}

func (e errorManagerSuite) TestInit(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	cfg := config.NewConfig()
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	cfg.App.MaxError.Type.Store(10)
	cfg.App.TaskInfoSchemaName = "lightning_errors"

	em := New(db, cfg)
	c.Assert(em.dupResolution, Equals, cfg.TikvImporter.DuplicateResolution)
	c.Assert(em.remainingError.Type.Load(), Equals, cfg.App.MaxError.Type.Load())
	c.Assert(em.remainingError.Conflict.Load(), Equals, cfg.App.MaxError.Conflict.Load())

	em.remainingError.Type.Store(0)
	em.dupResolution = config.DupeResAlgNone
	ctx := context.Background()
	err = em.Init(ctx)
	c.Assert(err, IsNil)

	em.dupResolution = config.DupeResAlgRecord
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`;").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
		WillReturnResult(sqlmock.NewResult(2, 1))
	err = em.Init(ctx)
	c.Assert(err, IsNil)

	em.dupResolution = config.DupeResAlgNone
	em.remainingError.Type.Store(1)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`;").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.type_error_v1.*").
		WillReturnResult(sqlmock.NewResult(4, 1))
	err = em.Init(ctx)
	c.Assert(err, IsNil)

	em.dupResolution = config.DupeResAlgRecord
	em.remainingError.Type.Store(1)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS `lightning_errors`.*").
		WillReturnResult(sqlmock.NewResult(5, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.type_error_v1.*").
		WillReturnResult(sqlmock.NewResult(6, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `lightning_errors`\\.conflict_error_v1.*").
		WillReturnResult(sqlmock.NewResult(7, 1))
	err = em.Init(ctx)
	c.Assert(err, IsNil)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
