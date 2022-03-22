package check

import (
	"SQL-On-LevelDB/src/catalog"
	"SQL-On-LevelDB/src/interpreter/types"
	"SQL-On-LevelDB/src/interpreter/value"
	"SQL-On-LevelDB/src/mapping"
	"errors"
	"fmt"
)

//CreateTableCheck 用来检查table，并返回所有的应该建的索引
func CreateTableInitAndCheck(statement types.CreateTableStatement) (*catalog.TableCatalog, error) {
	//检查表名是否已经存在
	// if _, ok := TableName2CatalogMap[statement.TableName]; ok {
	// 	return errors.New("Table " + statement.TableName + " already exists"), nil
	// }
	/*
		type CreateTableStatement struct {
			TableName   string
			ColumnsMap  map[string]Column
			PrimaryKeys []Key
			Cluster     Cluster
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
	newCatalog := catalog.CreateTableStatement2TableCatalog(&statement)
	err := createTableCheck(newCatalog)
	if err != nil {
		return newCatalog, err
	}
	return newCatalog, nil
}
func InsertCheck(statement types.InsertStament) (*catalog.TableCatalog, []int, []int, []catalog.UniquesColumn, error) {
	var ok bool
	table, err := mapping.InsertGetTableCatalog(statement.TableName)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var columnPositions []int
	var startBytePos []int
	var uniquecolumns []catalog.UniquesColumn
	if len(statement.ColumnNames) == 0 {
		//所有数据项都要有
		if len(statement.Values) != len(table.ColumnsMap) {
			return nil, nil, nil, nil, errors.New("the input data is not enough")
		}
		valueNumber := len(statement.Values)
		columnPositions = make([]int, valueNumber)
		startBytePos = make([]int, valueNumber)
		for _, column := range table.ColumnsMap {
			//一一比较数据类型
			pos := column.ColumnPos
			if !(pos < valueNumber && column.Type.TypeTag == statement.Values[pos].Convert2IntType()) {
				if item, ok := statement.Values[pos].(value.Int); ok && column.Type.TypeTag == types.Float64 {
					//进行隐式转化，int转为float
					statement.Values[pos] = value.Float{Val: float64(item.Val)}
				} else {
					return nil, nil, nil, nil, errors.New(fmt.Sprintf("column %s need a type %s,but your input value is %s", column.Name, catalog.ColumnType2StringName(column.Type.TypeTag), statement.Values[pos].String()))
				}
			}
			//记录startBytePos
			startBytePos[column.ColumnPos] = column.StartBytesPos
			//搜索unique列表
			if column.Unique {
				//是unique的话 要检查是否重复，如果有索引
				uniquecolumns = append(uniquecolumns, catalog.UniquesColumn{ColumnName: column.Name, Value: statement.Values[pos]})
			}

		}
		for i := 0; i < len(statement.Values); i++ {
			//append 0,1,2,3...
			columnPositions[i] = i
		}
	} else {
		//insert into table (a,b,c) values()
		columnPositions = make([]int, 0)
		startBytePos = make([]int, 0)
		for index, colName := range statement.ColumnNames {
			var col catalog.Column
			if col, ok = table.ColumnsMap[colName]; !ok {
				return nil, nil, nil, nil, errors.New("don't have a column named " + colName + " ,please check your table")
			}
			if col.Type.TypeTag != statement.Values[index].Convert2IntType() {
				if item, ok := statement.Values[index].(value.Int); ok && col.Type.TypeTag == types.Float64 { //是Int 同时列属性为float
					statement.Values[index] = value.Float{Val: float64(item.Val)} //将其转为Float值
				} else {
					return nil, nil, nil, nil, errors.New(fmt.Sprintf("column %s need a type %s, but your input Value is %s", col.Name, catalog.ColumnType2StringName(col.Type.TypeTag), statement.Values[index].String()))
				}
			}
			columnPositions = append(columnPositions, col.ColumnPos)
			startBytePos = append(startBytePos, col.StartBytesPos)
			if col.Unique {
				//是unique的话 要检查是否重复，如果有索引
				uniquecolumns = append(uniquecolumns, catalog.UniquesColumn{ColumnName: col.Name, Value: statement.Values[index]})
			}

		}
		//再检测有没有not null
		for colName, col := range table.ColumnsMap {
			if !col.NotNull {
				continue
			}
			f := false
			for _, inputName := range statement.ColumnNames {
				if colName == inputName {
					f = true
					break
				}
			}
			if !f {
				return nil, nil, nil, nil, errors.New(fmt.Sprintf("column %s is a not null type,please input a Value for it ", colName))
			}
		}
	}

	return table, columnPositions, startBytePos, uniquecolumns, nil
}
func SelectCheck(statement types.SelectStatement) (error, string, *catalog.TableCatalog) {
	//先检查table在不在，获取其catalog
	var ok bool
	table, err := mapping.SelectGetTableCatalog(statement.TableNames[0])
	if err != nil {
		return errors.New("don't have a table named " + statement.TableNames[0]), "", nil
	}
	var indexcolumn string
	err, indexcolumn = whereOptCheck(statement.Where, table)
	if err != nil {
		return err, "", nil
	}
	if statement.Fields.SelectAll {
		return nil, indexcolumn, table
	}
	//检测一下select 的field 属性是不是合法
	for _, item := range statement.Fields.ColumnNames {
		if _, ok = table.ColumnsMap[item]; !ok {
			return errors.New("don't have a column named " + item), "", nil
		}
	}
	return nil, indexcolumn, table

}
func DeleteCheck(statement types.DeleteStatement) (error, string, *catalog.TableCatalog) {
	//先检查table在不在，获取其catalog

	table, err := mapping.DeleteGetTableCatalog(statement.TableName)
	if err != nil {
		return errors.New("don't have a table named " + statement.TableName), "", nil
	}
	var indexcolumn string
	//检查where表达式的列是否都存在，并返回最优索引
	err, indexcolumn = whereOptCheck(statement.Where, table)
	if err != nil {
		return err, "", nil
	}

	return nil, indexcolumn, table

}

//whereOptCheck 用来查找有没有方便走的索引
//1.检测属性列合法 2.选择一个属性走索引 最好是等值的，都不是等值的，就找数据类型简单的
func whereOptCheck(where *types.Where, table *catalog.TableCatalog) (error, string) {
	if where == nil {
		return nil, ""
	}
	columnNames := where.Expr.GetTargetCols()
	indexList := table.Indexs
	var ok bool
	//检测where里属性列是否合法
	for _, item := range columnNames {
		if _, ok = table.ColumnsMap[item]; !ok {
			return errors.New("dont have a column named " + item), ""
		}

	}
	var bestExpr *types.ComparisonExprLSRV = nil //之后可以加入索引查询优化
	//索引查询优化：等值索引优先，都是不等值索引选数据类型最简单的
	for _, indexItem := range indexList {
		if b, exprIndex := where.Expr.GetIndexExpr(indexItem.Keys[0].Name); b {
			if bestExpr == nil && exprIndex != nil { //刚开始时候
				bestExpr = exprIndex
			} else if bestExpr != nil && exprIndex != nil && bestExpr.Operator != value.Equal && exprIndex.Operator == value.Equal { //有等值索引优先使用
				bestExpr = exprIndex
			} else if bestExpr != nil && exprIndex != nil { //都是比较索引或者都是等值索引
				bestType := table.ColumnsMap[bestExpr.Left].Type.TypeTag
				nowType := table.ColumnsMap[indexItem.Keys[0].Name].Type.TypeTag
				switch bestType {
				case types.Int64:
					bestExpr = bestExpr
				case types.Float64:
					if nowType == types.Float64 || nowType == types.Bytes {
						bestExpr = bestExpr
					} else {
						bestExpr = exprIndex
					}
				case types.Bytes:
					if nowType == types.Float64 || nowType == types.Int64 {
						bestExpr = exprIndex
					}
				}
			}
		}
	}
	if bestExpr == nil {
		return nil, ""
	}
	return nil, bestExpr.Left
}
