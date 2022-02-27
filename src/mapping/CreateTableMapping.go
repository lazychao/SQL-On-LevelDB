package mapping

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"bytes"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

var operationChannel chan<- db.DbOperation
var resultChannel <-chan db.DbResultBatch
var finishChannel chan<- error

//var result db.DbResultBatch

func SetFinishChannel(channel chan<- error) {
	finishChannel = channel
}

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
func SetDbChannel(channel1 chan<- db.DbOperation, channel2 <-chan db.DbResultBatch) {
	operationChannel = channel1
	resultChannel = channel2
}

func CreateTable(tablecatalog *catalog.TableCatalog) error {

	m_key := "m_" + tablecatalog.TableName
	var buf bytes.Buffer
	_ = msgp.Encode(&buf, tablecatalog)

	// db.Put([]byte(m_key), []byte(m_value), nil)
	// data, _ := db.Get([]byte(m_key), nil) //data是字节切片
	//fmt.Println(string(data))
	operation := db.DbOperation{DbOperationType: db.Put, Key: []byte(m_key), Value: buf.Bytes()}
	operationChannel <- operation
	result := <-resultChannel
	//fmt.Println(string(result.Result[0]))
	b := bytes.NewBuffer(result.Result[0])
	var inst catalog.TableCatalog
	_ = msgp.Decode(b, &inst)
	fmt.Println(string(inst.TableName))
	for k := range inst.ColumnsMap {
		fmt.Println(k)
	}
	finishChannel <- result.Err

	return nil
}
