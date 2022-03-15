package db

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DbOperationTag = int

const (
	Put DbOperationTag = iota
	GetOne
	GetBatch
	Delete
	GetBatchWithPrefix
)

type DbResult []byte
type DbResultBatch struct {
	//二维字节数组
	Result []DbResult
	//结果数量
	Cnt int
	Err error
}
type DbOperation struct {
	DbOperationType DbOperationTag
	Key             []byte
	Value           []byte
}

func RunDb(operationChannel <-chan DbOperation, resultChannel chan<- DbResultBatch) {
	db, _ := leveldb.OpenFile("data/testdb", nil) //打开一个数据库,不存在就自动创建
	//这是相对路径
	defer db.Close()

	for operation := range operationChannel {
		var result DbResultBatch
		switch operation.DbOperationType {
		case Put:
			db.Put(operation.Key, operation.Value, nil)
			data, err := db.Get(operation.Key, nil) //data是字节切片
			result.Result = append(result.Result, DbResult(data))
			//fmt.Println(string(data))
			result.Cnt = 1
			result.Err = err
			resultChannel <- result
		case GetOne:
			data, err := db.Get(operation.Key, nil)
			if len(data) == 0 {
				result.Cnt = 0
				//fmt.Println(string(data))
				result.Err = err
				resultChannel <- result
			} else {
				result.Cnt = 1
				result.Err = err
				result.Result = append(result.Result, DbResult(data))
				resultChannel <- result
			}
		case GetBatchWithPrefix:
			iter := db.NewIterator(util.BytesPrefix(operation.Key), nil)
			for iter.Next() {
				//分配一块空间，如果直接用iter的话，会出错，最后只拿到同一个
				bytes := make([]byte, len(iter.Value()))
				copy(bytes, iter.Value())
				result.Result = append(result.Result, DbResult(bytes))
				//fmt.Println(iter.Value())
				result.Cnt++
			}
			iter.Release()
			// fmt.Println(result.Result[0])
			// fmt.Println(result.Result[1])
			resultChannel <- result
		}

	}
}
