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
	"sort"

	"github.com/tinylib/msgp/msgp"
)

func SelectGetTableCatalog(tableName string) (*catalog.TableCatalog, error) {
	value := GetOne([]byte("m_" + tableName))
	//fmt.Println("SelectGetTableCatalog")
	if value == nil {
		err := errors.New("select table error:this table doesnot exist")
		return nil, err
	}
	b := bytes.NewBuffer(value)
	var inst catalog.TableCatalog
	_ = msgp.Decode(b, &inst)
	return &inst, nil
}
func SelectRecordWithIndex(table *catalog.TableCatalog, statement *types.SelectStatement, indexcolumn string, rowChannel chan<- value.Row) {

	//构造区间
	//只会得到一个区间，因为只有AND
	f, rangee := statement.Where.Expr.GetRange(indexcolumn)
	if !f {
		fmt.Println("cannot select with index")
		SelectRecord(table, statement, rowChannel)
	}

	//根据区间，构造index key range
	//3种情况，不可能存在两边都无尽
	//1.负无穷，右有尽 2.左有尽，正无穷 3.左有尽，右有尽
	//根据levelDB底层比较机制，负无穷用 i_tablename_index_表示， 正无穷i_tablename_index`表示
	//因为_的byte是95，依据的是ascii码，96对应的符号是`
	if rangee == nil {
		//区间为空,直接返回空结果
		close(rowChannel)
		return
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

	operation := db.DbOperation{
		IndexBeginKey: startkeybytes, IndexEndKey: endkeybytes,
		Removeleftcheck: removeleftcheck, Addrightcheck: addrightcheck}
	indexKeyChannel := make(chan db.IndexKeyBatch, 1)
	db.DB.GetIndexWithRange(&operation, indexKeyChannel)
	//得到结果
	result := <-indexKeyChannel
	if len(result) == 0 {
		close(rowChannel)
		return
	}
	rowids := decodeIndex(result, table, indexcolumn)
	rowkeybytes := encodeKey(rowids, table)

	operation = db.DbOperation{KeyBatch: rowkeybytes}
	//得到要进行where比较的列
	resultChannel := make(chan db.KV, 10)
	go db.DB.GetBatchValue(&operation, resultChannel)
	//得到结果
	decodedRowChannel := make(chan value.Row, 10)

	//译码
	go decodeOp(resultChannel, table, decodedRowChannel) //将行从字节解码回value
	filterRowChannel := make(chan value.Row, 10)
	//where筛选
	go filterOp(decodedRowChannel, filterRowChannel, statement.Where, table)
	//field 筛选
	projectedRowChannel := make(chan value.Row, 10)
	go projectOp(filterRowChannel, projectedRowChannel, table, statement.Fields.ColumnNames)
	//orderby 排序
	orderByRowChannel := make(chan value.Row, 10)
	go orderbyOp(table, statement.OrderBy, projectedRowChannel, orderByRowChannel)
	//limit
	go limitOp(statement.Limit, orderByRowChannel, rowChannel)
	//fmt.Println("syn")

}
func SelectRecord(table *catalog.TableCatalog, statement *types.SelectStatement, rowChannel chan<- value.Row) {

	//构造前缀
	//fmt.Println(table.RecordNo)
	prefix := "r_" + table.TableName + "_"
	operation := db.DbOperation{Key: []byte(prefix)}
	resultChannel := make(chan db.KV, 10)
	go db.DB.GetBatchValueWithPrefix(&operation, resultChannel)
	//得到结果
	decodedRowChannel := make(chan value.Row, 10)

	//译码
	go decodeOp(resultChannel, table, decodedRowChannel) //将行从字节解码回value
	filterRowChannel := make(chan value.Row, 10)
	//where筛选
	go filterOp(decodedRowChannel, filterRowChannel, statement.Where, table)
	//field 筛选
	projectedRowChannel := make(chan value.Row, 10)
	go projectOp(filterRowChannel, projectedRowChannel, table, statement.Fields.ColumnNames)
	//orderby 排序
	orderByRowChannel := make(chan value.Row, 10)
	go orderbyOp(table, statement.OrderBy, projectedRowChannel, orderByRowChannel)
	//limit
	go limitOp(statement.Limit, orderByRowChannel, rowChannel)
	//fmt.Println("syn")

}
func limitOp(limit types.Limit, orderByRowChannel <-chan value.Row, resultChannel chan<- value.Row) {

	defer close(resultChannel)
	if limit.Rowcount != 0 {
		//unsafe ,need to check if the range out of index
		i := 0
		for row := range orderByRowChannel {
			if i >= limit.Offset && i < limit.Offset+limit.Rowcount {
				resultChannel <- row

			} else if i >= limit.Offset+limit.Rowcount {
				break
			}
			i++
		}
	} else {
		for row := range orderByRowChannel {
			resultChannel <- row
		}
	}

}
func orderbyOp(table *catalog.TableCatalog, orderby types.Order, projectedRowChannel <-chan value.Row, orderByRowChannel chan<- value.Row) {
	defer close(orderByRowChannel)
	var ret []value.Row
	if orderby.Col != "" {
		//order slow the throughput heavily
		for row := range projectedRowChannel {
			ret = append(ret, row)
		}
		pos := table.ColumnsMap[orderby.Col].ColumnPos
		//这里是阻塞的，无法流水化
		if orderby.Direction == types.Asc {
			sort.SliceStable(ret, func(i, j int) bool {
				f, _ := ret[i].Values[pos].Compare(ret[j].Values[pos], value.Less)
				return f
			})
		} else {
			sort.SliceStable(ret, func(i, j int) bool {
				f, _ := ret[i].Values[pos].Compare(ret[j].Values[pos], value.Great)
				return f
			})
		}
		for _, row := range ret {
			orderByRowChannel <- row
		}
	} else {
		for row := range projectedRowChannel {
			orderByRowChannel <- row
		}
	}
}
func projectOp(filterRowChannel <-chan value.Row, projectedRowChannel chan<- value.Row, table *catalog.TableCatalog, columns []string) {
	defer close(projectedRowChannel)
	//f := 1
	for record := range filterRowChannel {
		//beginTime := time.Now()

		if len(columns) == 0 { //如果select* 则使用全部的即可
			projectedRowChannel <- record
			// if f == 4 {
			// 	durationTime := time.Since(beginTime)
			// 	fmt.Println("project Finish operation at: ", durationTime)
			// }
			//f++

			continue
		}
		var ret value.Row

		for _, column := range columns {
			ret.Values = append(ret.Values, record.Values[table.ColumnsMap[column].ColumnPos])
		}
		projectedRowChannel <- ret
	}
}
func filterOp(decodedRowChannel <-chan value.Row, filterRowChannel chan<- value.Row, where *types.Where, table *catalog.TableCatalog) {
	defer close(filterRowChannel)
	// defer func() {
	// 	fmt.Print("filter: ")
	// 	fmt.Println(time.Now().UnixNano())
	// }()

	if where != nil {
		//fmt.Println("where")
		colPos := getColPos(table, where)
		//f := 1
		for decodedRow := range decodedRowChannel {
			//beginTime := time.Now()

			if flag, _ := checkRow(decodedRow, where, colPos); flag == false {
				continue
			}
			//fmt.Println(decodedRow.Values[0].String())
			filterRowChannel <- decodedRow
			// if f == 4 {
			// 	durationTime := time.Since(beginTime)
			// 	fmt.Println("filter Finish operation at: ", durationTime)
			// }
			//f++

		}
	} else {
		for decodedRow := range decodedRowChannel {
			filterRowChannel <- decodedRow
		}
	}

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
func decodeIndex(dbResult db.IndexKeyBatch, table *catalog.TableCatalog, indexcolumn string) []int {
	var rowIds []int
	Basekeybytes := []byte("i_" + table.TableName + "_" + indexcolumn + "_")

	for _, indexKey := range dbResult {
		rowidbytes := indexKey[len(Basekeybytes)+table.ColumnsMap[indexcolumn].Type.Length:]
		rowIds = append(rowIds, utils.BytesToInt(rowidbytes))
	}
	return rowIds
}
func decodeOp(resultChannel <-chan db.KV, table *catalog.TableCatalog, decodedRowChannel chan<- value.Row) {
	defer close(decodedRowChannel)
	// defer func() {
	// 	fmt.Print("decode: ")
	// 	fmt.Println(time.Now().UnixNano())
	// }()
	//f := 1
	for kv := range resultChannel {
		//fmt.Println(kv.Value)

		bytes := kv.Value
		nullmap := utils.BytesToBools(bytes[0 : len(table.ColumnsMap)/8+1])
		if !nullmap[0] {
			continue
		}
		record := value.Row{Values: make([]value.Value, len(table.ColumnsMap))}
		//beginTime := time.Now()
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
		// if f == 4 {
		// 	durationTime := time.Since(beginTime)
		// 	fmt.Println("decode Finish operation at: ", durationTime)
		// }
		//fmt.Println(record.Values[0].String())
		decodedRowChannel <- record

		//f++

	}
}

//获取   where -> 每列所在的位置切片
func getColPos(table *catalog.TableCatalog, where *types.Where) (colPos []int) {
	cols := where.Expr.GetTargetCols()
	colPos = make([]int, 0, len(cols))
	for _, item := range cols {
		colPos = append(colPos, table.ColumnsMap[item].ColumnPos)
	}
	return colPos
}
func encodeKey(rowids []int, table *catalog.TableCatalog) [][]byte {
	base := "r_" + table.TableName + "_"
	basebytes := []byte(base)
	var rowkeybytes [][]byte
	for _, rowid := range rowids {
		fmt.Println(rowid)
		rowkeybytes = append(rowkeybytes, append(basebytes, utils.IntToBytes(rowid)...))
	}
	return rowkeybytes
}
