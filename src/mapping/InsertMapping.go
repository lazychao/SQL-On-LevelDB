package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/value"
	"SQL-On-LevelDB/src/utils"
	"bytes"
	"errors"

	"github.com/tinylib/msgp/msgp"
)

//Insert前需要获得table信息
func InsertGetTableCatalog(tableName string) (*catalog.TableCatalog, error) {
	value := GetOne([]byte("m_" + tableName))
	if value == nil {
		err := errors.New("insert table error:this table doesnot exist")

		return nil, err
	}
	b := bytes.NewBuffer(value)
	var inst catalog.TableCatalog
	_ = msgp.Decode(b, &inst)
	return &inst, nil
}
func InsertRecord(table *catalog.TableCatalog, colPos []int, startBytePos []int, values []value.Value, uniquescolumns []catalog.UniquesColumn) error {
	//TODO 检查unique的数据项是否重复，实际上是执行select(走索引)，如果select出来的rownum不为0就是重复了
	// for _, item := range uniquescolumns {

	// }
	data := make([]byte, table.RecordLength)
	nullmapBytes := data[0 : len(table.ColumnsMap)/8+1]
	nullmap := utils.BytesToBools(nullmapBytes)
	nullmap[0] = true //作用暂定
	for _, columnIndex := range colPos {
		nullmap[columnIndex+1] = true
	}
	nullmapBytes = utils.BoolsToBytes(nullmap)
	copy(data[:], nullmapBytes)
	for index := range colPos {
		tmp, err := values[index].Convert2Bytes()
		if err != nil {
			return err
		}
		copy(data[startBytePos[index]:], tmp)
	}
	//把data存到db里
	//先构造key
	s := "r_" + table.TableName + "_"
	r_key := []byte(s)
	r_key = append(r_key, utils.IntToBytes(table.RecordNo)...)

	operation := db.DbOperation{DbOperationType: db.Put, Key: r_key, Value: data}
	operationChannel <- operation
	<-resultChannel

	//还要更新unique索引
	/*
		type UniquesColumn struct {
			ColumnName string
			Value      value.Value
		}
	*/
	for _, item := range uniquescolumns {
		s = "i_" + table.TableName + "_" + item.ColumnName + "_"
		i_key := []byte(s)
		value, _ := item.Value.Convert2BytesComparable()
		i_key = append(i_key, value...)
		i_key = append(i_key, utils.IntToBytes(table.RecordNo)...)
		operation := db.DbOperation{DbOperationType: db.Put, Key: i_key}
		operationChannel <- operation
		<-resultChannel
	}
	//更新非unique索引
	//TODO 更新check go。检测是否是index里的，把原来的判断逻辑也直接改成判断是否是index里，而不是unique

	//还要更新回去元数据
	table.RecordTotal++
	table.RecordNo++
	err := UpdateTable(table)
	if err != nil {
		return err
	}

	return nil
}
