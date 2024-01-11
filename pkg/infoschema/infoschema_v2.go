



type sortedTableItem struct {
	tableID int64
	schemaTS uint64
	dbID int64
}

type sortedTable []sortedTableItem

type infoschemaData struct {
	// For the TableByID API
	tables [512]sortedTable   // {id, schemaTS, dbID} sorted
	cache *ristretto.Cache    // {id, schemaTS} => table.Table

	// For the TableByName API
	tableMap *btree.BTreeG[byNameItem]  // {name, schemaTS} => id
}

type byNameItem struct {
	dbName string
	tableName string
	schemaTS

	tableID int64
}

type infoschema struct {
	ts uint64
	*infoschemaData
}
