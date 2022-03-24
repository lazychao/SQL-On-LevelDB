package main

import (
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/executor"
	"SQL-On-LevelDB/src/interpreter/parser"
	"SQL-On-LevelDB/src/interpreter/types"
	"fmt"
	"os"
	"time"
)

func main() {
	//parse协程 有缓冲信道
	StatementChannel := make(chan types.DStatements, 500)
	FinishChannel := make(chan error, 500)
	OperationChannel := make(chan db.DbOperation, 500) //用于传输数据库操作
	DbResultChannel := make(chan db.DbResultBatch, 500)

	reader, _ := os.Open("/home/lazychao/SQL-On-LevelDB/test/test1000.txt")
	//instruction0 1 都读了
	defer reader.Close()

	go executor.Execute(StatementChannel, FinishChannel, OperationChannel, DbResultChannel) //begin the runtime for exec
	//另外开一个线程消耗finishChannel
	go db.RunDb(OperationChannel, DbResultChannel) //重新开一个db
	go func() {
		for range FinishChannel {
			//TODO 更加优雅的处理方式
		}
	}()
	beginTime := time.Now()
	//Parser可以直接传一个reader进去
	parser.Parse(reader, StatementChannel) //开始解析
	//是在是不知道 parse执行结束后，statemChanel有没有读完
	durationTime := time.Since(beginTime)
	fmt.Println("Finish operation at: ", durationTime)
	time.Sleep(time.Duration(2) * time.Second)
	//要全部执行完了才应该可以close
	close(StatementChannel) //关闭StatementChannel，进而关闭FinishChannel
	close(FinishChannel)
	close(OperationChannel)
	close(DbResultChannel)
	//fmt.Println(<-StatementChannel)

}
