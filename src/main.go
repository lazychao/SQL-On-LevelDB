package main

import (
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/executor"
	"SQL-On-LevelDB/src/interpreter/parser"
	"SQL-On-LevelDB/src/interpreter/types"
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/peterh/liner"
)

const firstPrompt = "sql->"
const secondPrompt = "      ->"

// func InitDB() error {
// 	err := CatalogManager.LoadDbMeta()
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
// func expandPath(path string) (string, error) {
// 	if strings.HasPrefix(path, "~/") {
// 		parts := strings.SplitN(path, "/", 2)
// 		home, err := os.UserHomeDir()
// 		if err != nil {
// 			return "", err
// 		}
// 		return filepath.Join(home, parts[1]), nil
// 	}
// 	return path, nil
// }
func loadHistoryCommand() (*os.File, error) {
	var file *os.File
	wd, _ := os.Getwd() //获取当前路径
	path := filepath.Join(wd, ".sql_history")

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			return nil, err
		}
	} else {
		file, err = os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
	}
	return file, err

}

/*
	const (
	CreateDatabase OperationType = iota
	UseDatabase
	CreateTable
	CreateIndex
	DropTable
	DropIndex
	DropDatabase
	Insert
	Update
	Delete
	Select         10
	ExecFile
)
*/
// func HandleOneParse(input chan types.DStatements, output chan error) {
// 	for item := range input {
// 		//for range 会一直等待 直到通道被关闭
// 		fmt.Println(item.GetOperationType())
// 		//DO something you want
// 		// if item.GetOperationType() == 10 {
// 		// 	item.Show()
// 		// }

// 		output <- nil // put the error return
// 	}
// 	close(output)
// }

func main() {
	ll := liner.NewLiner()

	defer ll.Close()

	ll.SetCtrlCAborts(true)
	file, err := loadHistoryCommand()
	if err != nil {
		panic(err)
	}

	s := bufio.NewScanner(file)
	for s.Scan() {
		//fmt.Println(s.Text())
		//把历史命令文件里的命令都读出来放到命令行app历史里
		ll.AppendHistory(s.Text())
	}

	StatementChannel := make(chan types.DStatements, 500) //用于传输操作指令通道
	FinishChannel := make(chan error, 500)                //用于api执行完成反馈通道
	OperationChannel := make(chan db.DbOperation, 500)    //用于传输数据库操作
	DbResultChannel := make(chan db.DbResultBatch, 500)

	defer func() {
		_, err := ll.WriteHistory(file)
		if err != nil {
			panic(err)
		}
		_ = file.Close()
		close(StatementChannel)
		close(FinishChannel)
		close(OperationChannel)
		fmt.Println("bye")
	}()
	//用于传输数据库结果
	go executor.Execute(StatementChannel, FinishChannel, OperationChannel, DbResultChannel) //begin the runtime for exec
	go db.RunDb(OperationChannel, DbResultChannel)
	var beginSQLParse = false
	var sqlText = make([]byte, 0, 100)
	for { //each sql
	LOOP:
		beginSQLParse = false
		sqlText = sqlText[:0]
		var input string
		var err error
		for { //each line
			if beginSQLParse {
				input, err = ll.Prompt(secondPrompt)
			} else {
				input, err = ll.Prompt(firstPrompt)
			}
			if err != nil {
				if err == liner.ErrPromptAborted {
					goto LOOP
				}
			}
			trimInput := strings.TrimSpace(input) //get the input without front and backend space
			if len(trimInput) != 0 {
				ll.AppendHistory(input)
				//检测是否是要退f出
				if !beginSQLParse && (trimInput == "exit" || strings.HasPrefix(trimInput, "exit;")) {
					//main函数退出
					return
				}
				//要用字节切片来append，不能用字符串，因为字符串不可修改
				sqlText = append(sqlText, append([]byte{' '}, []byte(trimInput)[0:]...)...)
				if !beginSQLParse {
					beginSQLParse = true
				}
				if strings.Contains(trimInput, ";") {
					break
				}
			}
		}
		beginTime := time.Now()
		//执行解析，解析结果放在StatementChannel
		err = parser.Parse(strings.NewReader(string(sqlText)), StatementChannel)
		// err = parser.Parse(strings.NewReader(string(sqlText)), StatementChannel)
		// //fmt.Println(string(sqlText))
		if err != nil {
			fmt.Println(err)
			continue
		}
		<-FinishChannel //等待指令执行完成
		durationTime := time.Since(beginTime)
		fmt.Println("Finish operation at: ", durationTime)
	}

}
