package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"bytes"
	"errors"

	"github.com/tinylib/msgp/msgp"
)

func DeleteGetTableCatalog(tableName string) (*catalog.TableCatalog, error) {
	value := GetOne([]byte("m_" + tableName))
	if value == nil {
		err := errors.New("delete table error:this table doesnot exist")
		return nil, err
	}
	b := bytes.NewBuffer(value)
	var inst catalog.TableCatalog
	_ = msgp.Decode(b, &inst)
	return &inst, nil
}
func DeleteRecord(table *catalog.TableCatalog, where *types.Where) (error, int) {
	colPos := getColPos(table, where)
	//构造前缀
	//fmt.Println(table.RecordNo)
	prefix := "r_" + table.TableName + "_"
	operation := db.DbOperation{DbOperationType: db.GetBatchKeyValueWithPrefix, Key: []byte(prefix)}
	operationChannel <- operation
	//得到结果
	result := <-resultChannel
	var deletedKey [][]byte
	rownum := 0
	for i, valuebytes := range result.Value {
		decodedRow := decode([]byte(valuebytes), table) //将行从字节解码回value
		//where筛选
		if flag, err := checkRow(decodedRow, where, colPos); err != nil || flag == false {
			if err != nil {
				return err, 0
			}
			continue
		}
		rownum++
		deletedKey = append(deletedKey, result.Key[i])

	}

	if rownum == 0 {
		return nil, 0
	}
	//执行删除,批量执行，减小通信开销
	operation = db.DbOperation{DbOperationType: db.DeleteBatch, KeyBatch: deletedKey}
	operationChannel <- operation
	//得到结果
	result = <-resultChannel
	//更新元数据
	table.RecordTotal = table.RecordTotal - rownum
	UpdateTable(table)
	return result.Err, rownum
}
