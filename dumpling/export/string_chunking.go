package export

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

// concurrentDumpStringFields handles composite key chunking with multiple columns
func (d *Dumper) concurrentDumpStringFields(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task, fields []string, orderByClause string, estimatedCount uint64) error {
	db, tbl := meta.DatabaseName(), meta.TableName()

	// Calculate total count and chunk parameters
	totalCount := int64(estimatedCount)
	chunkSize := int64(d.conf.Rows)
	if totalCount <= chunkSize {
		tctx.L().Info("table too small for chunking, using sequential dump",
			zap.String("database", db), zap.String("table", tbl))
		return d.dumpWholeTableDirectly(tctx, meta, taskChan, "", orderByClause, 0, 1)
	}

	// Calculate number of chunks
	numChunks := (totalCount + chunkSize - 1) / chunkSize
	selectField, selectLen := meta.SelectedField(), meta.SelectedLen()

	tctx.L().Info("starting streaming string-based chunking",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Strings("fields", fields),
		zap.Int64("totalChunks", numChunks),
		zap.Int64("chunkSize", chunkSize))

	// Stream chunk generation and task creation
	return d.streamStringChunks(tctx, conn, meta, taskChan, fields, orderByClause, chunkSize, numChunks, selectField, selectLen)
}

// concurrentDumpStringField handles single column chunking (backward compatibility)
func (d *Dumper) concurrentDumpStringField(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task, field, orderByClause string, estimatedCount uint64) error {
	return d.concurrentDumpStringFields(tctx, conn, meta, taskChan, []string{field}, orderByClause, estimatedCount)
}

// streamStringChunks generates boundaries incrementally and sends tasks with buffering to handle last chunk detection
func (d *Dumper) streamStringChunks(tctx *tcontext.Context, conn *BaseConn, meta TableMeta, taskChan chan<- Task, fields []string, orderByClause string, chunkSize, numChunks int64, selectField string, selectLen int) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()

	// For boundary sampling, we need to select all columns used in ORDER BY
	// Extract column names from ORDER BY clause and select them for boundary sampling
	// This ensures we get complete composite key values for proper WHERE clause generation

	// Parse ORDER BY clause to extract all column names
	// orderByClause format: "ORDER BY `item_id`,`photo_index`"
	orderByColumns := extractOrderByColumns(orderByClause)

	// Build SELECT columns for boundary sampling (all ORDER BY columns)
	selectCols := strings.Join(orderByColumns, ", ")

	tctx.L().Debug("boundary sampling setup",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Strings("chunkingFields", fields),
		zap.Strings("orderByColumns", orderByColumns),
		zap.String("dataOrderBy", orderByClause))

	// Initialize chunk tracking
	chunkStats := newTableChunkStat()
	d.chunkedTables.Store(meta.ChunkKey(), chunkStats)

	// Buffering approach: Buffer one chunk to determine if it's the last one
	type bufferedChunk struct {
		task       *TaskTableData
		chunkIndex int
	}
	var buffer *bufferedChunk

	// Helper function to send buffered chunk
	sendBufferedChunk := func(isLast bool) error {
		if buffer != nil {
			// Update the task with proper chunk info
			if isLast {
				buffer.task.TotalChunks = buffer.chunkIndex + 1 // Now we know the total
			}

			ctxDone := d.sendTaskToChan(tctx, buffer.task, taskChan)
			if ctxDone {
				return tctx.Err()
			}
			buffer = nil
		}
		return nil
	}

	// True streaming approach: Buffer chunks to handle last chunk detection
	var totalChunks int64

	defer func() {
		// Send any remaining buffered chunk as the last chunk
		if buffer != nil {
			_ = sendBufferedChunk(true) // Mark as last chunk
		}

		chunkStats.finalized.Store(true)

		// Update the totalChunks at the end of streaming to enable proper progress tracking
		// In streaming mode, we don't know total chunks upfront, so we update it after completion
		tctx.L().Debug("updating total chunks for streaming table",
			zap.String("database", db),
			zap.String("table", tbl),
			zap.Int64("totalChunks", totalChunks))

		if chunkStats.finished.Load() == chunkStats.sent.Load() {
			IncCounter(d.metrics.finishedTablesCounter)
			d.chunkedTables.Delete(meta.ChunkKey())
		}
	}()

	// Streaming boundary sampling and task creation
	tctx.L().Info("starting streaming boundary sampling and task creation",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Int64("estimatedChunks", numChunks))

	var previousBoundary []string
	// Continue boundary sampling until end of data (ignore numChunks estimate for streaming)
	for i := int64(1); ; i++ {
		// Check if we've hit the safety limit
		if i >= maxChunkLimit {
			tctx.L().Warn("hit max chunk limit during boundary sampling",
				zap.String("database", db),
				zap.String("table", tbl),
				zap.Int64("chunkIndex", i),
				zap.Int64("maxChunkLimit", maxChunkLimit))
			break
		}

		// Sample boundary for chunk i
		var sampleQuery string

		// Use cursor-based boundary sampling for optimal performance
		if len(previousBoundary) == 0 {
			// First boundary: OFFSET is acceptable for the first boundary
			offset := chunkSize
			sampleQuery = fmt.Sprintf(
				"SELECT %s FROM `%s`.`%s` %s LIMIT 1 OFFSET %d",
				selectCols,
				escapeString(db),
				escapeString(tbl),
				orderByClause,
				offset)
		} else {
			// Subsequent boundaries: use cursor-based pagination for performance
			// Skip chunkSize rows from previous boundary, then take the first row
			whereClause := buildCursorWhereClause(orderByColumns, previousBoundary)
			fullWhere := buildWhereCondition(conf, whereClause)
			sampleQuery = fmt.Sprintf(
				"SELECT %s FROM `%s`.`%s` %s %s LIMIT 1 OFFSET %d",
				selectCols,
				escapeString(db),
				escapeString(tbl),
				fullWhere,
				orderByClause,
				chunkSize) // Skip chunkSize more rows from cursor position
		}

		tctx.L().Debug("sampling boundary",
			zap.String("query", sampleQuery),
			zap.Int64("chunkIndex", i),
			zap.Bool("usingCursor", len(previousBoundary) > 0))

		// Execute boundary sampling query
		var currentBoundary []string
		err := conn.QuerySQL(tctx, func(rows *sql.Rows) error {
			// We're selecting all ORDER BY columns, not just chunking fields
			values := make([]any, len(orderByColumns))
			scanArgs := make([]any, len(orderByColumns))
			for j := range values {
				scanArgs[j] = &values[j]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}

			currentBoundary = make([]string, len(orderByColumns))
			for j, val := range values {
				if val == nil {
					currentBoundary[j] = ""
				} else {
					// Convert SQL driver value to string properly
					switch v := val.(type) {
					case string:
						currentBoundary[j] = v
					case []byte:
						currentBoundary[j] = string(v)
					case int64:
						currentBoundary[j] = strconv.FormatInt(v, 10)
					case int32:
						currentBoundary[j] = strconv.FormatInt(int64(v), 10)
					case int:
						currentBoundary[j] = strconv.Itoa(v)
					case float64:
						currentBoundary[j] = strconv.FormatFloat(v, 'f', -1, 64)
					case float32:
						currentBoundary[j] = strconv.FormatFloat(float64(v), 'f', -1, 32)
					default:
						currentBoundary[j] = fmt.Sprintf("%v", v)
					}
				}
			}
			return nil
		}, func() {}, sampleQuery)

		if err != nil {
			tctx.L().Warn("failed to sample boundary, stopping boundary collection",
				zap.String("database", db),
				zap.String("table", tbl),
				zap.Int64("chunkIndex", i),
				zap.Error(err))
			break
		}

		if len(currentBoundary) == 0 {
			tctx.L().Info("boundary sampling returned no results - reached end of data",
				zap.String("database", db),
				zap.String("table", tbl),
				zap.Int64("chunkIndex", i))
			break
		}

		tctx.L().Debug("sampled boundary successfully",
			zap.String("database", db),
			zap.String("table", tbl),
			zap.Int64("boundaryIndex", i),
			zap.Strings("boundary", currentBoundary))

		// Create task for chunk using previousBoundary -> currentBoundary (with buffering)
		var newTask *TaskTableData

		if len(previousBoundary) == 0 {
			// First chunk: everything up to first boundary
			whereClause := buildUpperBoundWhereClause(orderByColumns, currentBoundary)
			fullWhere := buildWhereCondition(conf, whereClause)
			query := buildSelectQuery(db, tbl, selectField, "", fullWhere, orderByClause)
			newTask = d.newTaskTableData(meta, newTableData(query, selectLen, false), int(totalChunks), -1)
		} else {
			// Intermediate chunk: between previousBoundary and currentBoundary
			whereClause := buildBoundedWhereClause(orderByColumns, previousBoundary, currentBoundary)
			fullWhere := buildWhereCondition(conf, whereClause)
			query := buildSelectQuery(db, tbl, selectField, "", fullWhere, orderByClause)
			newTask = d.newTaskTableData(meta, newTableData(query, selectLen, false), int(totalChunks), -1)
		}

		// Send previous buffered chunk (now we know it's not the last)
		if err := sendBufferedChunk(false); err != nil {
			return err
		}

		// Buffer the new task
		buffer = &bufferedChunk{
			task:       newTask,
			chunkIndex: int(totalChunks),
		}
		totalChunks++

		previousBoundary = currentBoundary // Update for next iteration
	}

	// After the loop, check if there's any remaining data to dump.
	// This happens if the last boundary sampling query returned no results,
	// meaning we've reached the end of the table, but there might be a partial chunk left.
	if len(previousBoundary) > 0 && totalChunks > 0 {
		tctx.L().Info("dumping remaining data after last sampled boundary",
			zap.String("database", db),
			zap.String("table", tbl),
			zap.Strings("lastBoundary", previousBoundary))

		// Send previous buffered chunk (now we know it's not the last)
		if err := sendBufferedChunk(false); err != nil {
			return err
		}

		// Create and buffer the final chunk
		whereClause := buildLowerBoundWhereClause(orderByColumns, previousBoundary)
		fullWhere := buildWhereCondition(conf, whereClause)
		query := buildSelectQuery(db, tbl, selectField, "", fullWhere, orderByClause)
		finalTask := d.newTaskTableData(meta, newTableData(query, selectLen, false), int(totalChunks), -1)

		buffer = &bufferedChunk{
			task:       finalTask,
			chunkIndex: int(totalChunks),
		}
		totalChunks++
		// The defer function will send this final chunk marked as last
	} else if totalChunks == 0 {
		// This block handles the case where no boundaries were found at all (e.g., very small table)
		// and no previousBoundary was ever set.
		tctx.L().Info("no boundaries found, dumping entire table as a single chunk",
			zap.String("database", db),
			zap.String("table", tbl))

		var firstWhereClause string
		if conf.Where != "" {
			firstWhereClause = conf.Where
		}
		query := buildSelectQuery(db, tbl, selectField, "", firstWhereClause, orderByClause)
		task := d.newTaskTableData(meta, newTableData(query, selectLen, false), 0, 1)

		// Single chunk case - send immediately as we know it's the only one
		ctxDone := d.sendTaskToChan(tctx, task, taskChan)
		if ctxDone {
			return tctx.Err()
		}
		totalChunks++
	}

	tctx.L().Info("completed streaming chunking",
		zap.String("database", db),
		zap.String("table", tbl),
		zap.Int64("totalChunks", totalChunks))

	return nil
}
