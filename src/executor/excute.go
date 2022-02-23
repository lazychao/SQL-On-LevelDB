package excutor

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/mapping"
	"SQL-On-LevelDB/src/types"
	"SQL-On-LevelDB/src/utils/Error"
	"fmt"
)

//HandleOneParse 用来处理parse处理完的DStatement类型  dataChannel是接收Statement的通道,整个mysql运行过程中不会关闭，但是quit后就会关闭
//stopChannel 用来发送同步信号，每次处理完一个后就发送一个信号用来同步两协程，主协程需要接收到stopChannel的发送后才能继续下一条指令，当dataChannel
//关闭后，stopChannel才会关闭
func Excute(dataChannel <-chan types.DStatements, stopChannel chan<- Error.Error) {
	var err Error.Error
	for statement := range dataChannel {
		//fmt.Println(statement)
		switch statement.GetOperationType() {

		case types.CreateTable:
			err = CreateTableAPI(statement.(types.CreateTableStatement))
			if err.Status != true {
				fmt.Println(err.ErrorHint)
			} else {
				fmt.Printf("create table succes.\n")
			}

			//fmt.Println(err)
			stopChannel <- err
		}
		close(stopChannel)
	}
}

//CreateTableAPI CM进行检查，index检查 语法检查  之后调用RM中的CreateTable创建表， 之后使用RM中的CreateIndex建索引
func CreateTableAPI(statement types.CreateTableStatement) Error.Error {
	//先检查要新建的表是否合法
	err := catalog.CreateTableCheck(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}
	err = mapping.CreateTable(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}

	return Error.CreateSuccessError()
}
