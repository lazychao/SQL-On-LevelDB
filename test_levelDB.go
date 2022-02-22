// The returned DB instance is safe for concurrent use. Which mean that all
// DB's methods may be called concurrently from multiple goroutine.
package main

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	db, _ := leveldb.OpenFile("data/testdb", nil) //打开一个数据库,不存在就自动创建
	//这是相对路径
	defer db.Close()
	db.Put([]byte("key"), []byte("value"), nil)
	data, _ := db.Get([]byte("key"), nil) //data是字节切片
	fmt.Print(data)
}
