package export

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

// getStringOrNumericIndexColumns picks up indices for chunking, including string columns
// follows same priority: primary key > unique key with smallest count > key with max cardinality
// Returns all columns of the selected index for proper composite key handling
func getStringOrNumericIndexColumns(tctx *tcontext.Context, db *BaseConn, meta TableMeta) ([]string, []bool, error) {
	database, table := meta.DatabaseName(), meta.TableName()
	colName2Type := string2Map(meta.ColumnNames(), meta.ColumnTypes())
	keyQuery := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", escapeString(database), escapeString(table))
	results, err := db.QuerySQLWithColumns(tctx, []string{"NON_UNIQUE", "SEQ_IN_INDEX", "KEY_NAME", "COLUMN_NAME", "CARDINALITY"}, keyQuery)
	if err != nil {
		return nil, nil, err
	}

	type keyInfo struct {
		columns   []string
		isStrings []bool
		count     uint64
	}
	var (
		uniqueKeyMap   = map[string]*keyInfo{} // unique key name -> key info
		bestKey        *keyInfo
		maxCardinality int64 = -1
	)

	// Group columns by key name and sequence
	keyColumns := make(map[string]map[uint64]string) // keyName -> seqIndex -> columnName
	for _, oneRow := range results {
		_, seqInIndex, keyName, colName, _ := oneRow[0], oneRow[1], oneRow[2], oneRow[3], oneRow[4]
		seqInIndexInt, err := strconv.ParseUint(seqInIndex, 10, 64)
		if err != nil {
			continue
		}

		if keyColumns[keyName] == nil {
			keyColumns[keyName] = make(map[uint64]string)
		}
		keyColumns[keyName][seqInIndexInt] = colName
	}

	// Process each key to build complete column lists
	for _, oneRow := range results {
		nonUnique, seqInIndex, keyName, _, cardinality := oneRow[0], oneRow[1], oneRow[2], oneRow[3], oneRow[4]

		// Only process the first column of each key to avoid duplicates
		if seqInIndex != "1" {
			continue
		}

		// Build complete column list for this key
		var columns []string
		var isStrings []bool
		keyColMap := keyColumns[keyName]

		// Add columns in sequence order
		for seq := uint64(1); seq <= uint64(len(keyColMap)); seq++ {
			if colName, exists := keyColMap[seq]; exists {
				colType := colName2Type[colName]
				_, isNumeric := dataTypeInt[colType]
				_, isString := dataTypeString[colType]

				// Accept both numeric and string columns for chunking
				if !isNumeric && !isString {
					// If any column in the key is not numeric/string, skip this key
					columns = nil
					isStrings = nil
					break
				}
				columns = append(columns, colName)
				isStrings = append(isStrings, isString)
			}
		}

		if len(columns) == 0 {
			continue
		}

		keyInfoObj := &keyInfo{
			columns:   columns,
			isStrings: isStrings,
			count:     uint64(len(columns)),
		}

		switch {
		case keyName == "PRIMARY":
			return columns, isStrings, nil
		case nonUnique == "0":
			uniqueKeyMap[keyName] = keyInfoObj
		case len(uniqueKeyMap) == 0:
			cardinalityInt, err := strconv.ParseInt(cardinality, 10, 64)
			if err == nil && cardinalityInt > maxCardinality {
				bestKey = keyInfoObj
				maxCardinality = cardinalityInt
			}
		}
	}

	if len(uniqueKeyMap) > 0 {
		var minCols uint64 = math.MaxUint64
		for _, keyInfo := range uniqueKeyMap {
			if keyInfo.count < minCols {
				bestKey = keyInfo
				minCols = keyInfo.count
			}
		}
	}

	if bestKey != nil {
		return bestKey.columns, bestKey.isStrings, nil
	}
	return nil, nil, nil
}

// pickupPossibleFieldsForStringChunking returns all columns of the selected index for composite key chunking
// escapeSQLString properly escapes a string for use in internal SQL boundary condition queries.
// This function is used only for generating WHERE clauses in chunking and pagination queries,
// and does NOT affect the dumpling data output format. Data output escaping is controlled
// separately by the global --escape-backslash flag in the WriteToBuffer methods.
func escapeSQLString(s string) string {
	var buf bytes.Buffer
	buf.WriteByte('\'')
	// Use the existing escapeSQL function with escapeBackslash=true for proper SQL escaping
	escapeSQL([]byte(s), &buf, true)
	buf.WriteByte('\'')
	return buf.String()
}

// Helper functions for streaming boundary generation

// buildCursorWhereClause builds a WHERE clause for cursor-based pagination using OR conditions
// For composite keys: WHERE col1 > val1 OR (col1 = val1 AND col2 > val2) OR (col1 = val1 AND col2 = val2 AND col3 > val3) ...
func buildCursorWhereClause(columnNames []string, boundary []string) string {
	if len(boundary) == 0 || len(columnNames) == 0 {
		return ""
	}

	quotedCols := make([]string, len(columnNames))
	escapedBoundary := make([]string, len(columnNames))

	for i, col := range columnNames {
		// Check if column is already quoted with backticks
		if strings.HasPrefix(col, "`") && strings.HasSuffix(col, "`") {
			quotedCols[i] = col // Already quoted, use as-is
		} else {
			quotedCols[i] = fmt.Sprintf("`%s`", escapeString(col)) // Quote and escape
		}
	}

	for i, val := range boundary {
		if val != "" {
			escapedBoundary[i] = escapeSQLString(val)
		} else {
			escapedBoundary[i] = "''"
		}
	}

	conditions := make([]string, 0, len(quotedCols))

	// Generate OR conditions for cursor-based pagination
	// col1 > val1 OR (col1 = val1 AND col2 > val2) OR (col1 = val1 AND col2 = val2 AND col3 > val3) ...
	for i := range quotedCols {
		var condition strings.Builder

		// Add equality conditions for previous columns
		if i > 0 {
			condition.WriteString("(")
			for j := range i {
				if j > 0 {
					condition.WriteString(" AND ")
				}
				fmt.Fprintf(&condition, "%s = %s", quotedCols[j], escapedBoundary[j])
			}
			condition.WriteString(" AND ")
		}

		// Add greater than condition for current column
		if i == len(quotedCols)-1 {
			fmt.Fprintf(&condition, "%s >= %s", quotedCols[i], escapedBoundary[i])
		} else {
			fmt.Fprintf(&condition, "%s > %s", quotedCols[i], escapedBoundary[i])
		}

		if i > 0 {
			condition.WriteString(")")
		}

		conditions = append(conditions, condition.String())
	}

	return strings.Join(conditions, " OR ")
}

// buildUpperBoundWhereClause builds a WHERE clause with only an upper bound using OR conditions
// For composite keys: WHERE col1 < val1 OR (col1 = val1 AND col2 < val2) OR (col1 = val1 AND col2 = val2 AND col3 < val3) ...
func buildUpperBoundWhereClause(columnNames []string, upperBoundary []string) string {
	if len(upperBoundary) == 0 || len(columnNames) == 0 {
		return ""
	}

	quotedCols := make([]string, len(columnNames))
	escapedBoundary := make([]string, len(columnNames))

	for i, col := range columnNames {
		// Check if column is already quoted with backticks
		if strings.HasPrefix(col, "`") && strings.HasSuffix(col, "`") {
			quotedCols[i] = col // Already quoted, use as-is
		} else {
			quotedCols[i] = fmt.Sprintf("`%s`", escapeString(col)) // Quote and escape
		}
	}

	for i, val := range upperBoundary {
		if val != "" {
			escapedBoundary[i] = escapeSQLString(val)
		} else {
			escapedBoundary[i] = "''"
		}
	}

	conditions := make([]string, 0, len(quotedCols))

	// Generate OR conditions for upper bound
	// col1 < val1 OR (col1 = val1 AND col2 < val2) OR (col1 = val1 AND col2 = val2 AND col3 < val3) ...
	for i := range quotedCols {
		var condition strings.Builder

		// Add equality conditions for previous columns
		if i > 0 {
			condition.WriteString("(")
			for j := range i {
				if j > 0 {
					condition.WriteString(" AND ")
				}
				fmt.Fprintf(&condition, "%s = %s", quotedCols[j], escapedBoundary[j])
			}
			condition.WriteString(" AND ")
		}

		// Add less than condition for current column
		fmt.Fprintf(&condition, "%s < %s", quotedCols[i], escapedBoundary[i])

		if i > 0 {
			condition.WriteString(")")
		}

		conditions = append(conditions, condition.String())
	}

	return strings.Join(conditions, " OR ")
}

// buildLowerBoundWhereClause builds a WHERE clause with only a lower bound using OR conditions
// For composite keys: WHERE col1 > val1 OR (col1 = val1 AND col2 > val2) OR (col1 = val1 AND col2 = val2 AND col3 > val3) ...
func buildLowerBoundWhereClause(columnNames []string, lowerBoundary []string) string {
	// This is the same as buildCursorWhereClause for > condition
	return buildCursorWhereClause(columnNames, lowerBoundary)
}

// buildBoundedWhereClause builds a WHERE clause with both bounds using OR conditions
// For composite keys: WHERE (lower_conditions) AND (upper_conditions)
func buildBoundedWhereClause(columnNames []string, lowerBoundary, upperBoundary []string) string {
	if len(lowerBoundary) == 0 || len(upperBoundary) == 0 || len(columnNames) == 0 {
		return ""
	}

	lowerClause := buildCursorWhereClause(columnNames, lowerBoundary)
	upperClause := buildUpperBoundWhereClause(columnNames, upperBoundary)

	if lowerClause == "" || upperClause == "" {
		return ""
	}

	return fmt.Sprintf("(%s) AND (%s)", lowerClause, upperClause)
}
