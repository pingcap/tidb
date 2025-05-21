// Copyright 2024 PingCAP, Inc.
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

package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// deprecated parameter
type Granularity string

const (
	FineGrained   Granularity = "fine-grained"
	CoarseGrained Granularity = "coarse-grained"
)

const logRestoreTableIDMarkerFilePrefix = "v1/log_restore_tables"

type LogRestoreTableIDsMarkerFile struct {
	// RestoreCommitTs records the timestamp after PITR restore done. Only the later PITR restore from the log backup of the cluster,
	// whose BackupTS is not less than it, can ignore the restore table IDs blocklist recorded in the file.
	RestoreCommitTs uint64 `protobuf:"varint,1,opt,name=restore_commit_ts,proto3"`
	// SnapshotBackupTs records the BackupTS of the PITR restore. Any PITR restore from the log backup of the cluster, whose restoredTS
	// is less than it, can ignore the restore table IDs blocklist recorded in the file.
	SnapshotBackupTs uint64 `protobuf:"varint,2,opt,name=snapshot_backup_ts,proto3"`
	// TableIDs records the table IDs blocklist of the cluster running the log backup task.
	TableIds []int64 `protobuf:"varint,3,rep,packed,name=table_ids,proto3"`
	// Checksum records the checksum of other fields.
	Checksum []byte `protobuf:"bytes,4,opt,name=checksum,proto3"`
}

func (m *LogRestoreTableIDsMarkerFile) Reset()         { *m = LogRestoreTableIDsMarkerFile{} }
func (m *LogRestoreTableIDsMarkerFile) String() string { return proto.CompactTextString(m) }
func (m *LogRestoreTableIDsMarkerFile) ProtoMessage()  {}

func (l *LogRestoreTableIDsMarkerFile) filename() string {
	return fmt.Sprintf("%s/R%016X_S%016X.meta", logRestoreTableIDMarkerFilePrefix, l.RestoreCommitTs, l.SnapshotBackupTs)
}

func parseLogRestoreTableIDsMarkerFileName(filename string) (restoreCommitTs, snapshotBackupTs uint64, parsed bool) {
	filename = path.Base(filename)
	if !strings.HasSuffix(filename, ".meta") {
		return 0, 0, false
	}
	if filename[0] != 'R' {
		return 0, 0, false
	}
	ts, err := strconv.ParseUint(filename[1:17], 16, 64)
	if err != nil {
		log.Warn("failed to parse log restore table IDs marker file name", zap.String("filename", filename), zap.Error(err))
		return 0, 0, false
	}
	restoreCommitTs = ts
	if filename[17] != '_' || filename[18] != 'S' {
		return 0, 0, false
	}
	ts, err = strconv.ParseUint(filename[19:35], 16, 64)
	if err != nil {
		log.Warn("failed to parse log restore table IDs marker file name", zap.String("filename", filename), zap.Error(err))
		return 0, 0, false
	}
	snapshotBackupTs = ts
	return restoreCommitTs, snapshotBackupTs, true
}

func (markerFile *LogRestoreTableIDsMarkerFile) checksumLogRestoreTableIDsMarkerFile() []byte {
	hasher := sha256.New()
	hasher.Write(binary.LittleEndian.AppendUint64(nil, markerFile.RestoreCommitTs))
	hasher.Write(binary.LittleEndian.AppendUint64(nil, markerFile.SnapshotBackupTs))
	for _, tableId := range markerFile.TableIds {
		hasher.Write(binary.LittleEndian.AppendUint64(nil, uint64(tableId)))
	}
	return hasher.Sum(nil)
}

func (markerFile *LogRestoreTableIDsMarkerFile) setChecksumLogRestoreTableIDsMarkerFile() {
	markerFile.Checksum = markerFile.checksumLogRestoreTableIDsMarkerFile()
}

// MarshalLogRestoreTableIDsMarkerFile generates an markerfile and marshals it. It returns its filename and the marshaled data.
func MarshalLogRestoreTableIDsMarkerFile(restoreCommitTs, snapshotBackupTs uint64, tableIds []int64) (string, []byte, error) {
	markerFile := &LogRestoreTableIDsMarkerFile{
		RestoreCommitTs:  restoreCommitTs,
		SnapshotBackupTs: snapshotBackupTs,
		TableIds:         tableIds,
	}
	markerFile.setChecksumLogRestoreTableIDsMarkerFile()
	filename := markerFile.filename()
	data, err := proto.Marshal(markerFile)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return filename, data, nil
}

// unmarshalLogRestoreTableIDsMarkerFile unmarshals the given markerfile.
func unmarshalLogRestoreTableIDsMarkerFile(data []byte) (restoreCommitTs, snapshotBackupTs uint64, tableIds []int64, err error) {
	markerFile := &LogRestoreTableIDsMarkerFile{}
	if err = proto.Unmarshal(data, markerFile); err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	if !bytes.Equal(markerFile.checksumLogRestoreTableIDsMarkerFile(), markerFile.Checksum) {
		return 0, 0, nil, errors.Errorf(
			"checksum mismatch (calculated checksum is %s but the recorded checksum is %s), the log restore table IDs marker file may be corrupted.",
			base64.StdEncoding.EncodeToString(markerFile.checksumLogRestoreTableIDsMarkerFile()),
			base64.StdEncoding.EncodeToString(markerFile.Checksum),
		)
	}
	return markerFile.RestoreCommitTs, markerFile.SnapshotBackupTs, markerFile.TableIds, nil
}

func fastWalkLogRestoreTableIDsMarkerFile(
	ctx context.Context,
	s storage.ExternalStorage,
	filterOutFn func(restoreCommitTs, snapshotBackupTs uint64) bool,
	executionFn func(ctx context.Context, filename string, restoreCommitTs uint64, tableIds []int64) error,
) error {
	filenames := make([]string, 0)
	if err := s.WalkDir(ctx, &storage.WalkOption{SubDir: logRestoreTableIDMarkerFilePrefix}, func(path string, _ int64) error {
		restoreCommitTs, snapshotBackupTs, parsed := parseLogRestoreTableIDsMarkerFileName(path)
		if parsed {
			if filterOutFn(restoreCommitTs, snapshotBackupTs) {
				return nil
			}
		}
		filenames = append(filenames, path)
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	workerpool := util.NewWorkerPool(8, "walk dir log restore table IDs marker files")
	eg, ectx := errgroup.WithContext(ctx)
	for _, filename := range filenames {
		if ectx.Err() != nil {
			break
		}
		workerpool.ApplyOnErrorGroup(eg, func() error {
			data, err := s.ReadFile(ectx, filename)
			if err != nil {
				return errors.Trace(err)
			}
			restoreCommitTs, snapshotBackupTs, tableIds, err := unmarshalLogRestoreTableIDsMarkerFile(data)
			if err != nil {
				return errors.Trace(err)
			}
			if filterOutFn(restoreCommitTs, snapshotBackupTs) {
				return nil
			}
			err = executionFn(ectx, filename, restoreCommitTs, tableIds)
			return errors.Trace(err)
		})
	}
	return errors.Trace(eg.Wait())
}

// CheckTableTrackerContainsTableIDsFromMarkerFiles checks whether pitr id tracker contains the filtered table IDs from marker file.
func CheckTableTrackerContainsTableIDsFromMarkerFiles(
	ctx context.Context,
	s storage.ExternalStorage,
	tracker *utils.PiTRIdTracker,
	startTs, restoredTs uint64,
) error {
	err := fastWalkLogRestoreTableIDsMarkerFile(ctx, s, func(restoreCommitTs, snapshotBackupTs uint64) bool {
		return startTs >= restoreCommitTs || restoredTs <= snapshotBackupTs
	}, func(_ context.Context, _ string, restoreCommitTs uint64, tableIds []int64) error {
		for _, tableId := range tableIds {
			if tracker.ContainsTableId(tableId) || tracker.ContainsPartitionId(tableId) {
				return errors.Errorf(
					"cannot restore the table(Id=%d) because it is restored(at %d) before snapshot backup(at %d). "+
						"Please respecify the filter that does not contain the table or replace with a newer snapshot backup.",
					tableId, restoreCommitTs, startTs)
			}
		}
		return nil
	})
	return errors.Trace(err)
}

// TruncateLogRestoreTableIDsMarkerFiles truncates the marker files whose restore commit ts is not larger than truncate until ts.
func TruncateLogRestoreTableIDsMarkerFiles(
	ctx context.Context,
	s storage.ExternalStorage,
	untilTs uint64,
) error {
	err := fastWalkLogRestoreTableIDsMarkerFile(ctx, s, func(restoreCommitTs, snapshotBackupTs uint64) bool {
		return untilTs < restoreCommitTs
	}, func(ctx context.Context, filename string, _ uint64, _ []int64) error {
		return s.DeleteFile(ctx, filename)
	})
	return errors.Trace(err)
}

type UniqueTableName struct {
	DB    string
	Table string
}

func TransferBoolToValue(enable bool) string {
	if enable {
		return "ON"
	}
	return "OFF"
}

// GetTableSchema returns the schema of a table from TiDB.
func GetTableSchema(
	dom *domain.Domain,
	dbName ast.CIStr,
	tableName ast.CIStr,
) (*model.TableInfo, error) {
	info := dom.InfoSchema()
	table, err := info.TableByName(context.Background(), dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

const maxUserTablesNum = 10

// AssertUserDBsEmpty check whether user dbs exist in the cluster
func AssertUserDBsEmpty(dom *domain.Domain) error {
	databases := dom.InfoSchema().AllSchemas()
	m := meta.NewReader(dom.Store().GetSnapshot(kv.MaxVersion))
	userTables := make([]string, 0, maxUserTablesNum+1)
	appendTables := func(dbName, tableName string) bool {
		if len(userTables) >= maxUserTablesNum {
			userTables = append(userTables, "...")
			return true
		}
		userTables = append(userTables, fmt.Sprintf("%s.%s", dbName, tableName))
		return false
	}
LISTDBS:
	for _, db := range databases {
		dbName := db.Name.L
		if tidbutil.IsMemOrSysDB(dbName) {
			continue
		}
		tables, err := m.ListSimpleTables(db.ID)
		if err != nil {
			return errors.Annotatef(err, "failed to iterator tables of database[id=%d]", db.ID)
		}
		if len(tables) == 0 {
			// tidb create test db on fresh cluster
			// if it's empty we don't take it as user db
			if dbName != "test" {
				if appendTables(db.Name.O, "") {
					break LISTDBS
				}
			}
			continue
		}
		for _, table := range tables {
			if appendTables(db.Name.O, table.Name.O) {
				break LISTDBS
			}
		}
	}
	if len(userTables) > 0 {
		return errors.Annotate(berrors.ErrRestoreNotFreshCluster,
			"user db/tables: "+strings.Join(userTables, ", "))
	}
	return nil
}

// GetTS gets a new timestamp from PD.
func GetTS(ctx context.Context, pdClient pd.Client) (uint64, error) {
	p, l, err := pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// GetTSWithRetry gets a new timestamp with retry from PD.
func GetTSWithRetry(ctx context.Context, pdClient pd.Client) (uint64, error) {
	var (
		startTS  uint64
		getTSErr error
		retry    uint
	)

	err := utils.WithRetry(ctx, func() error {
		startTS, getTSErr = GetTS(ctx, pdClient)
		failpoint.Inject("get-ts-error", func(val failpoint.Value) {
			if val.(bool) && retry < 3 {
				getTSErr = errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
			}
		})

		retry++
		if getTSErr != nil {
			log.Warn("failed to get TS, retry it", zap.Uint("retry time", retry), logutil.ShortError(getTSErr))
		}
		return getTSErr
	}, utils.NewAggressivePDBackoffStrategy())

	if err != nil {
		log.Error("failed to get TS", zap.Error(err))
	}
	return startTS, errors.Trace(err)
}
