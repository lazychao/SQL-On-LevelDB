package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"SQL-On-LevelDB/src/interpreter/value"
	"SQL-On-LevelDB/src/utils"
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
func DeleteRecord(table *catalog.TableCatalog, where *types.Where) int {

	//构造前缀
	//fmt.Println(table.RecordNo)
	prefix := "r_" + table.TableName + "_"
	operation := db.DbOperation{Key: []byte(prefix)}
	kvChannel := make(chan db.KV, 10)
	db.DB.GetBatchKeyValueWithPrefix(&operation, kvChannel)

	decodedRowChannel := make(chan []byte, 10)
	//译码
	go deleteFilterOp(kvChannel, decodedRowChannel, where, table) //将行从字节解码回value
	deleteNumChannel := make(chan int, 1)
	//执行删除,批量执行，减小通信开销
	operation = db.DbOperation{KeyChannel: decodedRowChannel}
	db.DB.DeleteBatch(&operation, deleteNumChannel)
	//得到结果
	rownum := <-deleteNumChannel
	//更新元数据
	table.RecordTotal = table.RecordTotal - rownum
	UpdateTable(table)
	return rownum
}

func DeleteRecordWithIndex(table *catalog.TableCatalog, where *types.Where, indexcolumn string) int {

	//构造区间
	//只会得到一个区间，因为只有AND
	f, rangee := where.Expr.GetRange(indexcolumn)
	if !f {

		return DeleteRecord(table, where)
	}

	//根据区间，构造index key range
	//3种情况，不可能存在两边都无尽
	//1.负无穷，右有尽 2.左有尽，正无穷 3.左有尽，右有尽
	//根据levelDB底层比较机制，负无穷用 i_tablename_index_表示， 正无穷i_tablename_index`表示
	//因为_的byte是95，依据的是ascii码，96对应的符号是`
	if rangee == nil {
		//区间为空,直接返回空结果
		//fmt.Println("sds")
		return 0
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
		//fmt.Println("sd")
		return 0
	}
	//fmt.Println("sad")
	rowids := decodeIndex(result, table, indexcolumn)
	rowkeybytes := encodeKey(rowids, table)

	operation2 := db.DbOperation{KeyBatch: rowkeybytes}
	//得到要进行where比较的列
	resultChannel := make(chan db.KV, 10)
	go db.DB.GetBatchKeyValue(&operation2, resultChannel)
	//得到结果
	decodedRowChannel := make(chan []byte, 10)
	go deleteFilterOp(resultChannel, decodedRowChannel, where, table)
	deleteNumChannel := make(chan int, 1)
	//执行删除,批量执行，减小通信开销
	operation = db.DbOperation{KeyChannel: decodedRowChannel}
	db.DB.DeleteBatch(&operation, deleteNumChannel)
	//得到结果
	rownum := <-deleteNumChannel
	//更新元数据
	table.RecordTotal = table.RecordTotal - rownum
	UpdateTable(table)
	return rownum
}
func deleteFilterOp(kvChannel <-chan db.KV, filterRowChannel chan<- []byte, where *types.Where, table *catalog.TableCatalog) {
	defer close(filterRowChannel)
	colPos := getColPos(table, where)
	for kv := range kvChannel {
		//fmt.Println(kv.Key)

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

		if where != nil {
			if flag, _ := checkRow(record, where, colPos); flag == false {
				continue
			}
			filterRowChannel <- kv.Key
			//todo
		} else {
			filterRowChannel <- kv.Key
		}

	}

}
