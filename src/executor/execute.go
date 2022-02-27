package executor

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"SQL-On-LevelDB/src/mapping"
	"SQL-On-LevelDB/src/utils/Error"
	"fmt"
)

//HandleOneParse 用来处理parse处理完的DStatement类型  dataChannel是接收Statement的通道,整个mysql运行过程中不会关闭，但是quit后就会关闭
//stopChannel 用来发送同步信号，每次处理完一个后就发送一个信号用来同步两协程，主协程需要接收到stopChannel的发送后才能继续下一条指令，当dataChannel
//关闭后，stopChannel才会关闭
func Execute(dataChannel <-chan types.DStatements, finishChannel chan<- error, operationChannel chan<- db.DbOperation, resultChannel <-chan db.DbResultBatch) {
	var err Error.Error
	mapping.SetDbChannel(operationChannel, resultChannel)
	mapping.SetFinishChannel(finishChannel)
	for statement := range dataChannel {
		//fmt.Println(statement)
		switch statement.GetOperationType() {

		case types.CreateTable:
			err = CreateTableAPI(statement.(types.CreateTableStatement))
			if !err.Status {
				fmt.Println(err.ErrorHint)
			} else {
				fmt.Printf("create table succes.\n")
			}

			//fmt.Println(err)

		}

	}

}

//CreateTableAPI CM进行检查，index检查 语法检查  之后调用RM中的CreateTable创建表， 之后使用RM中的CreateIndex建索引
func CreateTableAPI(statement types.CreateTableStatement) Error.Error {

	//先检查表
	catalog, err := catalog.CreateTableInitAndCheck(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}
	err = mapping.CreateTable(catalog)
	if err != nil {
		return Error.CreateFailError(err)
	}

	return Error.CreateSuccessError()
}
