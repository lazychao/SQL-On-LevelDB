package check

import (
	"SQL-On-LevelDB/src/catalog"
	"errors"
)

func createTableCheck(statement *catalog.TableCatalog) error {

	recordlength := 0
	columnNum := 0
	bytesPos := make([]int, len(statement.ColumnsMap)+1)

	for _, item := range statement.ColumnsMap { //check the type and length
		if item.Type.TypeTag > catalog.Timestamp || item.Type.TypeTag < catalog.Bool {
			return errors.New("column " + item.Name + " has a illegal type")
		}
		if item.Type.TypeTag == catalog.Bytes && item.Type.Length > 255 {
			return errors.New("column " + item.Name + " has a length > 255, please set the length between 0~255")
		}
		switch item.Type.TypeTag {
		case catalog.Bool:
			recordlength += 1
			bytesPos[item.ColumnPos] = 1
		case catalog.Int64:
			recordlength += 8
			bytesPos[item.ColumnPos] = 8
		case catalog.Float64:
			recordlength += 8
			bytesPos[item.ColumnPos] = 8
		case catalog.String, catalog.Bytes:
			recordlength += item.Type.Length //string is not like thess, but nowsday we don't use string type
			bytesPos[item.ColumnPos] = item.Type.Length
		case catalog.Date:
			recordlength += 5 //I don't know how length
			bytesPos[item.ColumnPos] = 5
		case catalog.Timestamp:
			recordlength += 8 //I don't know
			bytesPos[item.ColumnPos] = 8
		case catalog.Null:
			recordlength += 8 //it can't be null at create time
			bytesPos[item.ColumnPos] = 8
		case catalog.Alien:
			recordlength += 0 // I don't know
			bytesPos[item.ColumnPos] = 0
		}
		columnNum += 1
	}
	//还要更新columnMaps里的startbytesPos
	toolBytes := (columnNum)/8 + 1
	recordlength += toolBytes //bit map and a vaild part!!

	for i := 0; i < len(statement.ColumnsMap); i++ {
		tmpNum := bytesPos[i]
		bytesPos[i] = toolBytes
		toolBytes += tmpNum
	}
	//奇怪的算法，先从1-n-1累加，然后将第0位置为初始值
	for k, v := range statement.ColumnsMap {
		v.StartBytesPos = bytesPos[v.ColumnPos]
		statement.ColumnsMap[k] = v
	}

	statement.RecordLength = recordlength
	if len(statement.PrimaryKeys) > 0 {
		keyname := statement.PrimaryKeys[0].Name
		if item, ok := statement.ColumnsMap[keyname]; !ok {
			return errors.New("primary key error, don't have a column name " + item.Name)
		} else {
			//把主键设定成了unique
			item.Unique = true
			item.NotNull = true
			statement.ColumnsMap[keyname] = item
		}
	}

	/*
		type IndexCatalog struct {
			IndexName string
			Unique    bool
			Keys      []Key
		}
	*/
	//要为每一个unique的键都增加索引

	for _, item := range statement.ColumnsMap {
		if item.Unique {
			statement.Indexs = append(statement.Indexs, catalog.IndexCatalog{
				IndexName: item.Name,
				Unique:    true,
				Keys: []catalog.Key{
					{
						Name:     item.Name,
						KeyOrder: catalog.Asc,
					},
				},
			})
		}
	}
	//为主键添加索引
	return nil
}
