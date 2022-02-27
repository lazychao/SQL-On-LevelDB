package db

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type DbOperationTag = int

const (
	Put DbOperationTag = iota
	GetOne
	GetBatch
	Delete
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

		}

	}
}
