package executor

import (
	"SQL-On-LevelDB/src/check"
	"SQL-On-LevelDB/src/db"
	"SQL-On-LevelDB/src/interpreter/types"
	"SQL-On-LevelDB/src/interpreter/value"
	"SQL-On-LevelDB/src/mapping"
	"SQL-On-LevelDB/src/utils/Error"
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

//HandleOneParse 用来处理parse处理完的DStatement类型  dataChannel是接收Statement的通道,整个mysql运行过程中不会关闭，但是quit后就会关闭
//stopChannel 用来发送同步信号，每次处理完一个后就发送一个信号用来同步两协程，主协程需要接收到stopChannel的发送后才能继续下一条指令，当dataChannel
//关闭后，stopChannel才会关闭
func Execute(dataChannel <-chan types.DStatements, finishChannel chan<- error, operationChannel chan<- db.DbOperation, resultChannel <-chan db.DbResultBatch) {
	var err Error.Error
	mapping.SetDbChannel(operationChannel, resultChannel)
	//关闭datachannel才能退出这个函数
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

		case types.Insert:
			err = InsertTableAPI(statement.(types.InsertStament))
			if !err.Status {
				fmt.Println(err.ErrorHint)
			} else {
				//fmt.Printf("insert succes.%v\n", time.Now().UnixNano())
				fmt.Println("insert succes.")
			}

		case types.Select:
			err = SelectAPI(statement.(types.SelectStatement))
			if !err.Status {
				fmt.Println(err.ErrorHint)
			}

			// case types.Delete:
			// 	err = DeleteTableAPI(statement.(types.DeleteStament))
			// 	if !err.Status {
			// 		fmt.Println(err.ErrorHint)
			// 	} else {
			// 		fmt.Printf("delete succes.\n")
			// 	}
			// 	finishChannel<-nil
		case types.Delete:
			err = DeleteAPI(statement.(types.DeleteStatement))
			if !err.Status {
				fmt.Println(err.ErrorHint)
			} else {
				fmt.Printf("delete success, %d rows are deleted.\n", err.Rows)
			}
		}
		finishChannel <- nil
	}

}

func InsertTableAPI(statement types.InsertStament) Error.Error {
	//先检查表是否存在，并获取catalog

	tablecatalog, colPos, startBytePos, uniquescolumns, err := check.InsertCheck(statement)

	if err != nil {
		return Error.CreateFailError(err)
	}
	err = mapping.InsertRecord(tablecatalog, colPos, startBytePos, statement.Values, uniquescolumns)
	if err != nil {
		return Error.CreateFailError(err)
	}
	return Error.CreateSuccessError()
}

//CreateTableAPI CM进行检查，index检查 语法检查  之后调用RM中的CreateTable创建表， 之后使用RM中的CreateIndex建索引
func CreateTableAPI(statement types.CreateTableStatement) Error.Error {

	//先检查表
	tablecatalog, err := check.CreateTableInitAndCheck(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}
	err = mapping.CreateTable(tablecatalog)
	if err != nil {
		return Error.CreateFailError(err)
	}

	return Error.CreateSuccessError()
}

//delete from table where
func DeleteAPI(statement types.DeleteStatement) Error.Error {
	//check
	err, _, table := check.DeleteCheck(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}
	//执行delete
	err, rowNum := mapping.DeleteRecord(table, statement.Where)
	if err != nil {
		return Error.CreateFailError(err)
	}
	return Error.CreateRowsError(rowNum)
}

//SELECT sel_field_list FROM table_name_list where_opt limit_opt
func SelectAPI(statement types.SelectStatement) Error.Error {
	//先检查有无语法错误
	//indexcolumn是要走的索引列
	err, indexcolumn, table := check.SelectCheck(statement)
	//err, _, table := check.SelectCheck(statement)
	if err != nil {
		return Error.CreateFailError(err)
	}
	var rows []value.Row
	//indexcolumn是要走的索引列
	if indexcolumn != "" {
		err, rows = mapping.SelectRecordWithIndex(table, statement.Fields.ColumnNames, statement.Where, indexcolumn, statement.OrderBy)
	} else {
		err, rows = mapping.SelectRecord(table, statement.Fields.ColumnNames, statement.Where, statement.OrderBy)
	}

	if err != nil {
		return Error.CreateFailError(err)
	}

	//limit
	if statement.Limit.Rowcount != 0 {
		//unsafe ,need to check if the range out of index
		if statement.Limit.Offset < len(rows) && statement.Limit.Rowcount <= len(rows) {
			rows = rows[statement.Limit.Offset : statement.Limit.Offset+statement.Limit.Rowcount]
		}
	}
	if statement.Fields.SelectAll {
		selectcolumn := make([]string, len(table.ColumnsMap))
		for name, column := range table.ColumnsMap {
			//是无序的啊啊啊啊啊
			selectcolumn[column.ColumnPos] = name
		}
		PrintTable(statement.TableNames[0], selectcolumn, rows) //very dirty  but I have no other choose
	} else {
		PrintTable(statement.TableNames[0], statement.Fields.ColumnNames, rows)
	}
	return Error.CreateSuccessError()
}

func PrintTable(tableName string, columnName []string, records []value.Row) error {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	totalHeader := make([]interface{}, 0, len(columnName)+1)
	//totalHeader = append(totalHeader, tableName)
	for _, item := range columnName {
		totalHeader = append(totalHeader, item)
	}
	t.SetStyle(table.StyleColoredBright)
	t.Style().Format.Header = text.FormatDefault
	t.AppendHeader(totalHeader)
	columnNum := len(columnName)

	Rows := make([]table.Row, 0, len(records)+1)

	for _, item := range records {
		newRow := make([]interface{}, 0, columnNum+1)
		//	newRow = append(newRow, strconv.Itoa(i+1))
		for _, col := range item.Values {
			newRow = append(newRow, col.String())
			// fmt.Print(col.String() + " ")
		}
		Rows = append(Rows, newRow)
	}
	t.AppendRows(Rows)
	t.AppendFooter(table.Row{"Total", len(records)})
	t.Render()
	return nil
}
