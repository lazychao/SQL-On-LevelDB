package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"SQL-On-LevelDB/src/interpreter/value"
	"SQL-On-LevelDB/src/utils"
	"bytes"
	"errors"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

func SelectGetTableCatalog(tableName string) (*catalog.TableCatalog, error) {
	value := GetOne([]byte("m_" + tableName))
	if value == nil {
		err := errors.New("select table error:this table doesnot exist")
		return nil, err
	}
	b := bytes.NewBuffer(value)
	var inst catalog.TableCatalog
	_ = msgp.Decode(b, &inst)
	return &inst, nil
}
func SelectRecord(table *catalog.TableCatalog, columns []string, where *types.Where) (error, []value.Row) {
	ret := []value.Row{}
	colPos := getColPos(table, where)
	//构造前缀
	fmt.Println(table.RecordNo)
	prefix := "r_" + table.TableName + "_"
	operation := db.DbOperation{DbOperationType: db.GetBatchWithPrefix, Key: []byte(prefix), Value: []byte("")}
	operationChannel <- operation
	//得到结果
	result := <-resultChannel
	for _, rowbytes := range result.Result {
		//fmt.Println(rowbytes)
		decodedRow := decode([]byte(rowbytes), table) //将行从字节解码回value

		//where筛选
		if flag, err := checkRow(decodedRow, where, colPos); err != nil || flag == false {
			if err != nil {
				return err, nil
			}
			continue
		}
		//field 筛选
		tmp, _ := columnFilter(table, decodedRow, columns)
		ret = append(ret, tmp)
	}

	return nil, ret
}
func columnFilter(table *catalog.TableCatalog, record value.Row, columns []string) (value.Row, error) {
	if len(columns) == 0 { //如果select* 则使用全部的即可
		return record, nil
	}
	var ret value.Row

	for _, column := range columns {
		ret.Values = append(ret.Values, record.Values[table.ColumnsMap[column].ColumnPos])
	}

	return ret, nil
}
func checkRow(record value.Row, where *types.Where, colpos []int) (bool, error) {
	if len(colpos) == 0 {
		return true, nil
	}
	val := make([]value.Value, 0, len(colpos))
	for i := 0; i < len(colpos); i++ {
		val = append(val, record.Values[colpos[i]])
	}
	return where.Expr.Evaluate(val)

}
func decode(bytes []byte, table *catalog.TableCatalog) value.Row {

	nullmap := utils.BytesToBools(bytes[0 : len(table.ColumnsMap)/8+1])
	if nullmap[0] == false {
		return value.Row{}
	}
	record := value.Row{Values: make([]value.Value, len(table.ColumnsMap))}
	for _, column := range table.ColumnsMap {
		//这里不用range给的索引是因为，map的迭代是无固定顺序的，这个索引是不可以用的
		startpos := column.StartBytesPos
		length := column.Type.Length
		tag := column.Type.TypeTag

		if !nullmap[column.ColumnPos+1] {
			tag = catalog.Null
		}
		record.Values[column.ColumnPos], _ = value.Byte2Value(bytes[startpos:], tag, length)
		//fmt.Print(record.Values[column.ColumnPos].String() + " ")
	}
	//fmt.Println(" ")
	return record
}

//获取   where -> 每列所在的位置切片
func getColPos(table *catalog.TableCatalog, where *types.Where) (colPos []int) {
	if where == nil {
		colPos = make([]int, 0, 0)
	} else {
		cols := where.Expr.GetTargetCols()
		colPos = make([]int, 0, len(cols))
		for _, item := range cols {
			colPos = append(colPos, table.ColumnsMap[item].ColumnPos)
		}
	}
	return
}
