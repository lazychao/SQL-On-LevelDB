package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"bytes"
	"errors"

	"github.com/tinylib/msgp/msgp"
)

//var result db.DbResultBatch

/*
type Column struct {
	Name          string
	Type          ColumnType
	Unique        bool
	NotNull       bool
	ColumnPos     int //the created position when table is created, this Value is fixed
	StartBytesPos int //the start postion in record bytes array
}

type ColumnType struct {
	TypeTag ScalarColumnTypeTag
	Length  int
	IsArray bool
}
type Key struct {
	Name     string
	KeyOrder KeyOrder
}

// Cluster is a Spanner table cluster.
type Cluster struct {
	TableName string
	OnDelete  OnDelete
}
type TableCatalog struct {
	TableName    string
	ColumnsMap   map[string]Column
	PrimaryKeys  []Key
	Cluster      Cluster
	Indexs       []IndexCatalog
	RecordNo     int //RecordNo means the now record number
	RecordTotal  int //RecordTotal means the total number
	RecordLength int //RecordLength means a record length contains 3 parts, a vaild part , null bitmap, and record . use byte as the unit
}

*/

func GetOne(key []byte) []byte {
	operation := db.DbOperation{Key: key}
	resultchannel := make(chan db.KV, 1) //不能写成无缓冲！！！
	//fmt.Println("getone")
	db.DB.GetOne(&operation, resultchannel)

	result := <-resultchannel
	//fmt.Println(string(result.Value[0]))
	if len(result.Value) == 0 {
		return nil
	} else {
		return result.Value
	}

}

func CreateTable(tablecatalog *catalog.TableCatalog) error {

	m_key := "m_" + tablecatalog.TableName
	//先检查要新建的表的表名是否重复了
	value := GetOne([]byte(m_key))
	if value != nil {
		err := errors.New("create table error:this table already exists")

		return err
	}
	var buf bytes.Buffer
	_ = msgp.Encode(&buf, tablecatalog)

	// db.Put([]byte(m_key), []byte(m_value), nil)
	// data, _ := db.Get([]byte(m_key), nil) //data是字节切片
	//fmt.Println(string(data))
	operation := db.DbOperation{Key: []byte(m_key), Value: buf.Bytes()}
	resultchannel := make(chan db.KV, 1)
	db.DB.Put(&operation, resultchannel)
	<-resultchannel
	// //fmt.Println(string(result.Value[0]))
	// b := bytes.NewBuffer(result.Value[0])
	// var inst catalog.TableCatalog
	// _ = msgp.Decode(b, &inst)
	// fmt.Println(string(inst.TableName))
	// for k := range inst.ColumnsMap {
	// 	fmt.Println(k)
	// }

	return nil
}

//not alter table operation.
func UpdateTable(tablecatalog *catalog.TableCatalog) error {

	m_key := "m_" + tablecatalog.TableName

	var buf bytes.Buffer
	_ = msgp.Encode(&buf, tablecatalog)
	operation := db.DbOperation{Key: []byte(m_key), Value: buf.Bytes()}
	resultchannel := make(chan db.KV, 1)
	db.DB.Put(&operation, resultchannel)
	<-resultchannel
	//fmt.Println(string(result.Value[0]))
	// b := bytes.NewBuffer(result.Value[0])
	// var inst catalog.TableCatalog
	// _ = msgp.Decode(b, &inst)
	// fmt.Println(string(inst.TableName))
	// for k := range inst.ColumnsMap {
	// 	fmt.Println(k)
	// }
	// if result.Err != nil {
	// 	return errors.New("update table error!\n")
	// }

	return nil
}
