package catalog

/*
//CreateTableCheck 用来检查table，并返回所有的应该建的索引
func CreateTableCheck(statement types.CreateTableStatement) (error, []IndexCatalog) {
	//检查表名是否已经存在
	// if _, ok := TableName2CatalogMap[statement.TableName]; ok {
	// 	return errors.New("Table " + statement.TableName + " already exists"), nil
	// }

	newCatalog := CreateTableStatement2TableCatalog(&statement)
	err, indexs := createTableInitAndCheck(newCatalog)
	if err != nil {
		return err, nil
	}
	if newCatalog != nil {
		TableName2CatalogMap[statement.TableName] = newCatalog

	} else {
		return errors.New("fail to conver type, internal errors"), nil
	}

	//_= AddTableToCatalog(UsingDatabase.DatabaseId)
	return FlushDatabaseMeta(UsingDatabase.DatabaseId), indexs
}
*/
