package db

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DbOperationTag = int

const (
	Put DbOperationTag = iota
	GetOne
	GetBatchValue
	DeleteBatch
	GetBatchValueWithPrefix
	GetBatchKeyValueWithPrefix
	GetIndexWithRange
)

type DbResult []byte
type DbResultBatch struct {
	//二维字节数组
	Value []DbResult
	Key   []DbResult
	//结果数量
	Cnt int
	Err error
}

type DbOperation struct {
	DbOperationType DbOperationTag
	Key             []byte
	KeyBatch        [][]byte
	Value           []byte
	IndexBeginKey   []byte
	IndexEndKey     []byte
	Removeleftcheck bool
	Addrightcheck   bool
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
			//data, err := db.Get(operation.Key, nil) //data是字节切片
			//result.Value = append(result.Value, DbResult(data))
			//fmt.Println(string(data))
			//result.Cnt = 1
			//result.Err = err
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
				result.Value = append(result.Value, DbResult(data))
				resultChannel <- result
			}

		case GetBatchValue:

			for _, key := range operation.KeyBatch {
				data, _ := db.Get(key, nil)
				result.Value = append(result.Value, DbResult(data))
				result.Cnt++
			}
			resultChannel <- result

		case GetBatchValueWithPrefix:
			iter := db.NewIterator(util.BytesPrefix(operation.Key), nil)
			for iter.Next() {
				//分配一块空间，如果直接用iter的话，会出错，最后只拿到同一个
				bytes := make([]byte, len(iter.Value()))
				copy(bytes, iter.Value())
				result.Value = append(result.Value, DbResult(bytes))
				//fmt.Println(iter.Value())
				result.Cnt++
			}
			iter.Release()
			// fmt.Println(result.Value[0])
			// fmt.Println(result.Value[1])
			resultChannel <- result

		case GetBatchKeyValueWithPrefix:
			iter := db.NewIterator(util.BytesPrefix(operation.Key), nil)
			for iter.Next() {
				//分配一块空间，如果直接用iter的话，会出错，最后只拿到同一个
				valuebytes := make([]byte, len(iter.Value()))
				copy(valuebytes, iter.Value())
				result.Value = append(result.Value, DbResult(valuebytes))

				keybytes := make([]byte, len(iter.Key()))
				copy(keybytes, iter.Key())
				result.Key = append(result.Key, DbResult(keybytes))
				//fmt.Println(iter.Value())
				result.Cnt++
			}
			iter.Release()
			// fmt.Println(result.Value[0])
			// fmt.Println(result.Value[1])
			resultChannel <- result

		case DeleteBatch:
			batch := new(leveldb.Batch)
			for _, key := range operation.KeyBatch {
				batch.Delete(key)
			}
			err := db.Write(batch, nil)
			result.Err = err
			resultChannel <- result
		case GetIndexWithRange:
			//根据range把index数据取出，只要key
			iter := db.NewIterator(&util.Range{Start: operation.IndexBeginKey, Limit: operation.IndexEndKey}, nil)
			for iter.Next() {
				keybytes := make([]byte, len(iter.Key()))
				copy(keybytes, iter.Key())
				result.Key = append(result.Key, DbResult(keybytes))
				//fmt.Println(iter.Value())
				result.Cnt++
			}
			iter.Release()
			if operation.Removeleftcheck {
				for {
					//这句话报错越界了，没道理啊  where a=6
					if bytes.HasPrefix([]byte(result.Key[0]), operation.IndexBeginKey) {
						//要去掉第一个
						//可能有多个的啊，考虑非unique index ,可能把所有都去掉了
						//TODO 测试这里
						result.Key = result.Key[1:]
						result.Cnt--
						if len(result.Key) == 0 {
							break
						}
					} else {
						break
					}
				}

			}
			if operation.Addrightcheck {
				//要尝试添加
				//可能多个 根据前缀添加所有
				iter := db.NewIterator(util.BytesPrefix(operation.IndexEndKey), nil)
				for iter.Next() {
					//分配一块空间，如果直接用iter的话，会出错，最后只拿到同一个
					keybytes := make([]byte, len(iter.Key()))
					copy(keybytes, iter.Key())
					result.Key = append(result.Key, DbResult(keybytes))
					result.Cnt++
					//fmt.Print(keybytes)
				}
				iter.Release()
			}

			resultChannel <- result
		}

	}
}
