// The returned DB instance is safe for concurrent use. Which mean that all
// DB's methods may be called concurrently from multiple goroutine.
package main

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	db, _ := leveldb.OpenFile("data/testdb", nil) //打开一个数据库,不存在就自动创建
	//这是相对路径
	defer db.Close()
	db.Put([]byte("peop"), []byte("value1"), nil)
	db.Put([]byte("people2"), []byte("value2"), nil)
	db.Put([]byte("people3"), []byte("value3"), nil)
	db.Put([]byte("people4"), []byte("value4"), nil)
	//根据前缀查找
	// iter := db.NewIterator(util.BytesPrefix([]byte("people")), nil)
	// for iter.Next() {
	// 	fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	// }
	// iter.Release()
	//确定集合范围进行查找，顺序为字典序
	//左闭右开，不包含limit
	iter := db.NewIterator(&util.Range{Start: []byte("peo"), Limit: []byte("people4")}, nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	//data, _ := db.Get([]byte("key"), nil) //data是字节切片
	//fmt.Print(data)
}
