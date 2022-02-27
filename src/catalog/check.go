package catalog

import (
	"SQL-On-LevelDB/src/interpreter/types"
)

//CreateTableCheck 用来检查table，并返回所有的应该建的索引
func CreateTableInitAndCheck(statement types.CreateTableStatement) (*TableCatalog, error) {
	//检查表名是否已经存在
	// if _, ok := TableName2CatalogMap[statement.TableName]; ok {
	// 	return errors.New("Table " + statement.TableName + " already exists"), nil
	// }
	/*
		type CreateTableStatement struct {
			TableName   string
			ColumnsMap  map[string]Column
			PrimaryKeys []Key
			Cluster     Cluster
		}
		type TableCatalog struct {
			TableName    string
			ColumnsMap   map[string]Column
			PrimaryKeys  []Key
			Cluster      Cluster
			Indexs       []IndexCatalog
			RecordNo     int //RecordNo means the now record number
			RecordTotal  int //RecordTotal means the total number
			RecordLength int //RecordLength means a record length contains 3 parts, a vaild part , null bitmap, and record . use byte as the unit
		}

	*/
	newCatalog := CreateTableStatement2TableCatalog(&statement)
	err := createTableCheck(newCatalog)
	if err != nil {
		return newCatalog, err
	}
	return newCatalog, nil
}
