package db

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

var finishChannel chan<- error

type DbOperationTag = int

const (
	Put DbOperationTag = iota
	Get
	Delete
)

type DbOperation struct {
	DbOperationType DbOperationTag
	Key             string
	Value           string
}

func SetFinishChannel(channel chan<- error) {
	finishChannel = channel
}
func RunDb(operationChannel <-chan DbOperation) {
	db, _ := leveldb.OpenFile("data/testdb", nil) //打开一个数据库,不存在就自动创建
	//这是相对路径
	defer db.Close()

	for operation := range operationChannel {

		switch operation.DbOperationType {
		case Put:
			db.Put([]byte(operation.Key), []byte(operation.Value), nil)
			data, err := db.Get([]byte(operation.Key), nil) //data是字节切片
			fmt.Println(string(data))
			finishChannel <- err
		}

	}
}
