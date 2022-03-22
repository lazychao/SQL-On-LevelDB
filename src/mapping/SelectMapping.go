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
func SelectRecordWithIndex(table *catalog.TableCatalog, columns []string, where *types.Where, indexcolumn string) (error, []value.Row) {
	ret := []value.Row{}
	//构造区间
	//只会得到一个区间，因为只有AND
	f, rangee := where.Expr.GetRange(indexcolumn)
	if !f {
		fmt.Println("cannot select with index")
		err, rows := SelectRecord(table, columns, where)
		return err, rows
	}

	//根据区间，构造index key range
	//3种情况，不可能存在两边都无尽
	//1.负无穷，右有尽 2.左有尽，正无穷 3.左有尽，右有尽
	//根据levelDB底层比较机制，负无穷用 i_tablename_index_表示， 正无穷i_tablename_index`表示
	//因为_的byte是95，依据的是ascii码，96对应的符号是`
	if rangee == nil {
		//区间为空,直接返回空结果
		return nil, ret
	}
	// fmt.Println(rangee.Begin.Val.String())
	// fmt.Println(rangee.Begin.Include)
	// fmt.Println(rangee.End.Val.String())
	// fmt.Println(rangee.End.Include)

	//由于levelDb的range是左闭右开的，所以还要检查一下结果，看要不要剔除左边，或者添加右边
	removeleftcheck := false //check是否要剔除第一个元素，如果rangee为左开，则要剔除
	addrightcheck := false   //如果rangee为右闭，则要添加
	Basekey := "i_" + table.TableName + "_" + indexcolumn + "_"
	Basekeybytes := []byte(Basekey)
	var startkeybytes []byte
	var endkeybytes []byte
	if _, ok := rangee.Begin.Val.(value.Null); !ok {
		value, _ := rangee.Begin.Val.Convert2BytesComparable()

		startkeybytes = append(Basekeybytes, value...)
		//fmt.Println(startkeybytes)
		//[105 95 116 101 115 116 51 95 97 95 128 0 0 0 0 0 0 6]
		if !rangee.Begin.Include {
			//左开
			removeleftcheck = true
		}
	} else {
		startkeybytes = Basekeybytes
	}

	if _, ok := rangee.End.Val.(value.Null); !ok {
		value, _ := rangee.End.Val.Convert2BytesComparable()
		endkeybytes = append(Basekeybytes, value...)
		if rangee.End.Include {
			//右闭
			addrightcheck = true
		}
	} else {
		endkeybytes = []byte("i_" + table.TableName + "_" + indexcolumn + "`")
	}

	operation := db.DbOperation{DbOperationType: db.GetIndexWithRange,
		IndexBeginKey: startkeybytes, IndexEndKey: endkeybytes,
		Removeleftcheck: removeleftcheck, Addrightcheck: addrightcheck}
	operationChannel <- operation
	//得到结果
	result := <-resultChannel
	if result.Cnt == 0 {
		return nil, ret
	}
	rowids := decodeIndex(result.Key, table, indexcolumn)
	rowkeybytes := encodeKey(rowids, table)
	operation = db.DbOperation{DbOperationType: db.GetBatchValue, KeyBatch: rowkeybytes}
	operationChannel <- operation
	//得到结果
	result = <-resultChannel
	//得到要进行where比较的列
	colPos := getColPos(table, where)
	for _, rowbytes := range result.Value {
		fmt.Println(rowbytes)
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
func SelectRecord(table *catalog.TableCatalog, columns []string, where *types.Where) (error, []value.Row) {
	ret := []value.Row{}
	colPos := getColPos(table, where)
	//构造前缀
	//fmt.Println(table.RecordNo)
	prefix := "r_" + table.TableName + "_"
	operation := db.DbOperation{DbOperationType: db.GetBatchValueWithPrefix, Key: []byte(prefix)}
	operationChannel <- operation
	//得到结果
	result := <-resultChannel
	for _, rowbytes := range result.Value {
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
func decodeIndex(dbResult []db.DbResult, table *catalog.TableCatalog, indexcolumn string) []int {
	var rowIds []int
	Basekeybytes := []byte("i_" + table.TableName + "_" + indexcolumn + "_")

	for _, item := range dbResult {
		indexKey := []byte(item)
		rowidbytes := indexKey[len(Basekeybytes)+table.ColumnsMap[indexcolumn].Type.Length:]
		rowIds = append(rowIds, utils.BytesToInt(rowidbytes))
	}
	return rowIds
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
func encodeKey(rowids []int, table *catalog.TableCatalog) [][]byte {
	base := "r_" + table.TableName + "_"
	basebytes := []byte(base)
	var rowkeybytes [][]byte
	for _, rowid := range rowids {
		rowkeybytes = append(rowkeybytes, append(basebytes, utils.IntToBytes(rowid)...))
	}
	return rowkeybytes
}
