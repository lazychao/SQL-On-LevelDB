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

type KV struct {
	Key   []byte
	Value []byte
}
type IndexKeyBatch [][]byte
type DbOperation struct {
	Key             []byte
	KeyBatch        [][]byte
	Value           []byte
	IndexBeginKey   []byte
	IndexEndKey     []byte
	Removeleftcheck bool
	Addrightcheck   bool
	KeyChannel      chan []byte
}

//就用单例模式吧
type DBInstance struct {
	db *leveldb.DB
}

var DB DBInstance

func Init() {
	db, _ := leveldb.OpenFile("data/testdb", nil)
	DB.db = db
}
func Close() {
	DB.db.Close()
}
func (dbinstance DBInstance) Put(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	dbinstance.db.Put(operation.Key, operation.Value, nil)
	resultChannel <- KV{}
}
func (dbinstance DBInstance) GetOne(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	data, _ := dbinstance.db.Get(operation.Key, nil)
	if len(data) == 0 {
		resultChannel <- KV{}
	} else {
		resultChannel <- KV{Value: data}
	}
}
func (dbinstance DBInstance) GetBatchValue(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	for _, key := range operation.KeyBatch {
		//每读出一个就返回一个KV
		data, _ := dbinstance.db.Get(key, nil)
		//fmt.Println(data)
		resultChannel <- KV{Value: data}
	}

}
func (dbinstance DBInstance) GetBatchKeyValue(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	for _, key := range operation.KeyBatch {
		//每读出一个就返回一个KV
		data, _ := dbinstance.db.Get(key, nil)
		//fmt.Println(key)
		keybytes := make([]byte, len(key))
		copy(keybytes, key)
		resultChannel <- KV{Key: keybytes, Value: data}
	}

}
func (dbinstance DBInstance) GetBatchValueWithPrefix(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	// defer func() {
	// 	fmt.Print("db: ")
	// 	fmt.Println(time.Now().UnixNano())
	// }()

	//f := 1
	iter := dbinstance.db.NewIterator(util.BytesPrefix(operation.Key), nil)
	for iter.Next() {
		//beginTime := time.Now()
		bytes := make([]byte, len(iter.Value()))
		copy(bytes, iter.Value())
		resultChannel <- KV{Value: bytes}
		// if f == 4 {
		// 	durationTime := time.Since(beginTime)
		// 	fmt.Println("db Finish operation at: ", durationTime)
		// }
		// f++

	}
	iter.Release()
}
func (dbinstance DBInstance) GetBatchKeyValueWithPrefix(operation *DbOperation, resultChannel chan<- KV) {
	defer close(resultChannel)
	iter := dbinstance.db.NewIterator(util.BytesPrefix(operation.Key), nil)
	for iter.Next() {
		keybytes := make([]byte, len(iter.Key()))
		copy(keybytes, iter.Key())
		valuebytes := make([]byte, len(iter.Value()))
		copy(valuebytes, iter.Value())
		resultChannel <- KV{Key: keybytes, Value: valuebytes}
	}
	iter.Release()
}
func (dbinstance DBInstance) DeleteBatch(operation *DbOperation, synchannel chan<- int) {
	defer close(synchannel)
	cnt := 0
	batch := new(leveldb.Batch)
	for key := range operation.KeyChannel {
		batch.Delete(key)
		cnt++
	}
	dbinstance.db.Write(batch, nil)
	synchannel <- cnt
}
func (dbinstance DBInstance) GetIndexWithRange(operation *DbOperation, resultChannel chan<- IndexKeyBatch) {
	//根据range把index数据取出，只要key
	var result IndexKeyBatch
	iter := dbinstance.db.NewIterator(&util.Range{Start: operation.IndexBeginKey, Limit: operation.IndexEndKey}, nil)
	for iter.Next() {
		keybytes := make([]byte, len(iter.Key()))
		copy(keybytes, iter.Key())
		result = append(result, keybytes)
	}
	iter.Release()
	if operation.Removeleftcheck {
		for {
			//这句话报错越界了，没道理啊  where a=6
			if bytes.HasPrefix(result[0], operation.IndexBeginKey) {
				//要去掉第一个
				//可能有多个的啊，考虑非unique index ,可能把所有都去掉了
				//TODO 测试这里
				result = result[1:]
				if len(result) == 0 {
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
		iter := dbinstance.db.NewIterator(util.BytesPrefix(operation.IndexEndKey), nil)
		for iter.Next() {
			//分配一块空间，如果直接用iter的话，会出错，最后只拿到同一个
			keybytes := make([]byte, len(iter.Key()))
			copy(keybytes, iter.Key())
			result = append(result, keybytes)
			//fmt.Print(keybytes)
		}
		iter.Release()
	}
	resultChannel <- result
}
