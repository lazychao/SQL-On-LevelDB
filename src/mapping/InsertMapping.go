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
	for _, item := range uniquescolumns {
		where := types.Where{Expr: &types.ComparisonExprLSRV{Left: item.ColumnName, Operator: value.Equal, Right: item.Value}} //构造where表达式
		statement := types.SelectStatement{Where: &where}
		rowChannel := make(chan value.Row, 1)
		go InsertSelectRecordWithIndex(table, &statement, item.ColumnName, rowChannel)
		//err, rows := SelectRecordWithIndex(table, make([]string, 0), &where, item.ColumnName, types.Order{})
		f := false
		for range rowChannel {
			f = true
			break
		}
		if f {
			return errors.New(item.ColumnName + " uniuqe conflict")
		}
	}
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

	operation := db.DbOperation{Key: r_key, Value: data}
	resultChannel := make(chan db.KV, 1)
	db.DB.Put(&operation, resultChannel)
	<-resultChannel

	//还要更新unique索引
	for _, item := range uniquescolumns {
		s = "i_" + table.TableName + "_" + item.ColumnName + "_"
		i_key := []byte(s)
		value, _ := item.Value.Convert2BytesComparable()
		i_key = append(i_key, value...)
		i_key = append(i_key, utils.IntToBytes(table.RecordNo)...)
		operation := db.DbOperation{Key: i_key}
		resultchannel := make(chan db.KV, 1)
		db.DB.Put(&operation, resultchannel)
		<-resultchannel
	}
	//更新非unique索引
	//TODO 更新check go。检测是否是index里的，把原来的判断逻辑也直接改成判断是否是index里，而不是unique
	//TODO uniquecolumn改成indexcolumn ，然后再检查unique冲突的时候遍历indexcolumn，同时要检查是否是unique
	//还要更新回去元数据
	table.RecordTotal++
	table.RecordNo++
	err := UpdateTable(table)
	if err != nil {
		return err
	}

	return nil
}
func InsertSelectRecordWithIndex(table *catalog.TableCatalog, statement *types.SelectStatement, indexcolumn string, rowChannel chan<- value.Row) {

	//构造区间
	//只会得到一个区间，因为只有AND
	f, rangee := statement.Where.Expr.GetRange(indexcolumn)
	if !f {
		InsertSelectRecord(table, statement, rowChannel)
	}

	if rangee == nil {
		//区间为空,直接返回空结果
		close(rowChannel)
		return
	}
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
	if len(result) != 0 {
		rowChannel <- value.Row{}
	}
	close(rowChannel)
}
func InsertSelectRecord(table *catalog.TableCatalog, statement *types.SelectStatement, rowChannel chan<- value.Row) {

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

	//where筛选
	go filterOp(decodedRowChannel, rowChannel, statement.Where, table)

}
