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
	fmt.Printf("%T", db)
	db.Delete([]byte("t_i_13"), nil)
	db.Put([]byte("t_i_1"), []byte("value1"), nil)
	db.Put([]byte("t_i_2"), []byte("value2"), nil)
	db.Put([]byte("t_i_9"), []byte("value3"), nil)
	db.Put([]byte("t_i_5"), []byte("value4"), nil)
	//遍历数据库
	// i := 0
	// iter := db.NewIterator(nil, nil)
	// for iter.Next() {
	// 	i++
	// 	fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	// }
	// iter.Release()
	// fmt.Print(i)

	//根据前缀查找
	prefix := "i_test3_"
	iter := db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		//fmt.Printf("[ %s ]:%s\n", iter.Key(), iter.Value())
		fmt.Println(iter.Key())
	}
	iter.Release()

	// 确定集合范围进行查找，顺序为字典序
	// 左闭右开，不包含limit
	// iter := db.NewIterator(&util.Range{Start: []byte("t_i_"), Limit: []byte("t_i_6")}, nil)
	// for iter.Next() {
	// 	fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	// }
	// iter.Release()

}
