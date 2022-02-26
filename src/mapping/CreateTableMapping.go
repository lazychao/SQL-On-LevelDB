package mapping

import (
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"fmt"
)

var operationChannel chan<- db.DbOperation
var resultChannel <-chan db.DbResultBatch
var finishChannel chan<- error

//var result db.DbResultBatch

func SetFinishChannel(channel chan<- error) {
	finishChannel = channel
}

/*
type CreateTableStatement struct {
	TableName   string
	ColumnsMap  map[string]Column
	PrimaryKeys []Key
	Cluster     Cluster
}
*/
/*
type Column struct {
	Name      string
	Type      ColumnType
	Unique    bool
	NotNull   bool
	ColumnPos int //the created position when table is created, this value is fixed
}
type ColumnType struct {
	TypeTag ScalarColumnTypeTag
	Length  int
	IsArray bool
}
type ScalarColumnTypeTag = int

const (
	Bool ScalarColumnTypeTag = iota
	Int64
	Float64
	String
	Bytes
	Date
	Timestamp
)
*/
func SetDbChannel(channel1 chan<- db.DbOperation, channel2 <-chan db.DbResultBatch) {
	operationChannel = channel1
	resultChannel = channel2
}

func CreateTable(statement types.CreateTableStatement) error {

	m_key := "m_" + statement.TableName
	m_value := ""
	for k := range statement.ColumnsMap {
		m_value += k
		m_value += "_"
	}
	// db.Put([]byte(m_key), []byte(m_value), nil)
	// data, _ := db.Get([]byte(m_key), nil) //data是字节切片
	//fmt.Println(string(data))
	operation := db.DbOperation{DbOperationType: db.Put, Key: m_key, Value: m_value}
	operationChannel <- operation
	result := <-resultChannel
	fmt.Println(string(result.Result[0]))
	finishChannel <- result.Err

	return nil
}
