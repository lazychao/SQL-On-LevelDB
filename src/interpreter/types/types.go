package types

import (
	"SQL-On-LevelDB/src/interpreter/value"
	"fmt"
)

// NOTE aliases to refer from parser.
const (
	True  = true
	False = false
)

//OnDelete is used for on delete behave
type OnDelete = int

const (
	NoAction OnDelete = iota
	Cascade
)

//KeyOrder order for key
type KeyOrder = int

const (
	Asc KeyOrder = iota
	Desc
)

//ScalarColumnTypeTag is the type
//标量
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

type OperationType = int

//创建数据库，使用数据库，创建表，创建索引，删除表/索引，删除数据库，插入，更新，删除行，select，
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
	Select
	ExecFile
)

//得到操作类型
type DStatements interface {
	GetOperationType() OperationType
	Show()
}

// DDStatements has parsed statements.
//type DDStatements struct {
//	CreateDatabases []CreateDatabaseStatement
//	CreateTables    []CreateTableStatement
//	CreateIndexes    []CreateIndexStatement
//	DropDatabses    []DropDatabaseStatement
//	DropTables      []DropTableStatement
//	DropIndexes     []DropIndexStatement
//}

// Column is a table column.
//一列有 列名，列类型，是否unique，是否非空，列的序号
type Column struct {
	Name      string
	Type      ColumnType
	Unique    bool
	NotNull   bool
	ColumnPos int //the created position when table is created, this value is fixed
}

//数据类型，数据长度，数据是否是array
type ColumnType struct {
	TypeTag ScalarColumnTypeTag
	Length  int
	IsArray bool
}

// Key is a table key.
type Key struct {
	Name     string
	KeyOrder KeyOrder
	//keyorder是增序降序
}

// Cluster is a Spanner table cluster.
type Cluster struct {
	TableName string
	OnDelete  OnDelete
}

// StoringClause is a storing clause info.
type StoringClause struct {
	ColumnNames []string
}

// Interleave is a interlive.
type Interleave struct {
	TableName string
}

// CreateDatabaseStatement is a 'CREATE DATABASE' statement info.
type CreateDatabaseStatement struct {
	DatabaseId string
}

func (c CreateDatabaseStatement) GetOperationType() OperationType {
	return CreateDatabase
}
func (c CreateDatabaseStatement) Show() {

}

// UseDatabaseStatement is a 'Use DATABASE' statement info.
type UseDatabaseStatement struct {
	DatabaseId string
}

func (c UseDatabaseStatement) GetOperationType() OperationType {
	return UseDatabase
}
func (c UseDatabaseStatement) Show() {

}

// CreateTableStatement is a 'CREATE TABLE' statement info.
//创建表，需要表名，  主键，各个列（用map存，可以直接用string来索引，而不是用数组切片）
type CreateTableStatement struct {
	TableName   string
	ColumnsMap  map[string]Column
	PrimaryKeys []Key
	Cluster     Cluster
}

func (c CreateTableStatement) GetOperationType() OperationType {
	return CreateTable
}
func (c CreateTableStatement) Show() {

}

// CreateIndexStatement is a 'CREATE INDEX' statement info.
type CreateIndexStatement struct {
	IndexName     string
	Unique        bool
	TableName     string
	Keys          []Key
	StoringClause StoringClause
	Interleaves   []Interleave
}

func (c CreateIndexStatement) GetOperationType() OperationType {
	return CreateIndex
}
func (c CreateIndexStatement) Show() {

}

// DropDatabaseStatement is a 'DROP TABLE' statement info.
type DropDatabaseStatement struct {
	DatabaseId string
}

func (c DropDatabaseStatement) GetOperationType() OperationType {
	return DropDatabase
}
func (c DropDatabaseStatement) Show() {

}

// DropTableStatement is a 'DROP TABLE' statement info.
type DropTableStatement struct {
	TableName string
}

func (c DropTableStatement) GetOperationType() OperationType {
	return DropTable
}
func (c DropTableStatement) Show() {

}

// DropIndexStatement is a 'DROP INDEX' statement info.
type DropIndexStatement struct {
	TableName string
	IndexName string
}

func (c DropIndexStatement) GetOperationType() OperationType {
	return DropIndex
}
func (c DropIndexStatement) Show() {

}

// SelectStatement is a 'SELECT' statement info.
//Select需要 fields 表名 where orderBy limit
type SelectStatement struct {
	Fields FieldsName
	/*
		type FieldsName struct {
			SelectAll   bool
			ColumnNames []string
		}
	*/
	TableNames []string
	Where      *Where //maybe is nil!!!
	OrderBy    []Order
	Limit      Limit //maybe is nil!!!
}

func (s SelectStatement) GetOperationType() OperationType {
	return Select
}
func (s SelectStatement) Show() {
	fmt.Println("fieldName:", s.Fields.ColumnNames[0])
	fmt.Println("TableName:", s.TableNames[0])
}

type ExecFileStatement struct {
	FileName string
}

func (s ExecFileStatement) GetOperationType() OperationType {
	return ExecFile
}
func (s ExecFileStatement) Show() {

}

type Point struct {
	Val     value.Value
	Include bool
}
type Range struct {
	Begin *Point
	End   *Point
}
type (
	//Where is the type for where func which maybe nil!
	Where struct {
		Expr Expr
	}
	Expr interface {
		Evaluate(row []value.Value) (bool, error)
		GetTargetCols() []string
		Debug()
		GetTargetColsNum() int
		//GetIndexExpr input a index column name, and find whether have a name same as index
		GetIndexExpr(string) (bool, *ComparisonExprLSRV)
		//GetRange input a index column name,and compute the index data range
		GetRange(indexName string) (bool, *Range)
	}
	//ComparisonExprLSRV left string right value
	//比较有四种情况，列名 compareOp 值，列名 op 列名，值 op 值，值 op 列名
	ComparisonExprLSRV struct {
		Left     string
		Operator value.CompareType
		Right    value.Value
	}
	ComparisonExprLVRS struct {
		Left     value.Value
		Operator value.CompareType
		Right    string
	}
	ComparisonExprLVRV struct {
		Left     value.Value
		Operator value.CompareType
		Right    value.Value
	}
	ComparisonExprLSRS struct {
		Left     string
		Operator value.CompareType
		Right    string
	}
	AndExpr struct {
		Left, Right       Expr
		LeftNum, RightNum int
	}
	OrExpr struct {
		Left, Right       Expr
		LeftNum, RightNum int
	}
	NotExpr struct {
		Expr    Expr
		LeftNum int
	}
	Limit struct {
		Offset, Rowcount int
	}
	Order struct {
		Col       string
		Direction KeyOrder
	}
	FieldsName struct {
		SelectAll   bool
		ColumnNames []string
	}
	SetExpr struct {
		Left  string
		Right value.Value
	}
)

func (e *ComparisonExprLSRV) Evaluate(row []value.Value) (bool, error) {
	val := row[0]
	if _, ok := val.(value.Null); ok { //left string's value is NULL
		if _, iok := e.Right.(value.Null); iok { //right is also NULL
			if e.Operator == value.Equal {
				return true, nil
			}
			return false, nil
		} else {
			if e.Operator == value.NotEqual {
				return true, nil
			}
			return false, nil
		}
	}
	if _, ok := e.Right.(value.Null); ok { //left not NULL
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return val.SafeCompare(e.Right, e.Operator)
}
func (e *ComparisonExprLSRV) GetTargetCols() []string {
	return []string{e.Left}
}
func (e *ComparisonExprLSRV) GetTargetColsNum() int {
	return 1
}
func (e *ComparisonExprLSRV) Debug() {
	fmt.Println(e.Left, e.Operator, e.Right.String())
}

//传入一个列，得到对应的索引表达式LSRV
func (e *ComparisonExprLSRV) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	if e.Left == indexName && e.Operator != value.NotEqual {
		return true, &ComparisonExprLSRV{Left: e.Left, Operator: e.Operator, Right: e.Right}
	}
	return false, nil
}
func (e *ComparisonExprLSRV) GetRange(indexName string) (bool, *Range) {
	if e.Left == indexName {
		switch e.Operator {
		case value.Equal:
			begin := Point{Val: e.Right, Include: true}
			end := Point{Val: e.Right, Include: true}
			return true, &Range{Begin: &begin, End: &end}
		case value.Less:
			begin := Point{Val: value.Null{Length: 0}, Include: false}
			end := Point{Val: e.Right, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		case value.LessEqual:
			begin := Point{Val: value.Null{Length: 0}, Include: false}
			end := Point{Val: e.Right, Include: true}
			return true, &Range{Begin: &begin, End: &end}
		case value.Great:
			begin := Point{Val: e.Right, Include: false}
			end := Point{Val: value.Null{Length: 0}, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		case value.GreatEqual:
			begin := Point{Val: e.Right, Include: true}
			end := Point{Val: value.Null{Length: 0}, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		}

	}
	return false, nil
}

func (e *ComparisonExprLVRS) Evaluate(row []value.Value) (bool, error) {
	val := row[0]
	if _, ok := val.(value.Null); ok {
		if _, iok := e.Left.(value.Null); iok {
			if e.Operator == value.Equal {
				return true, nil
			}
			return false, nil
		} else {
			if e.Operator == value.NotEqual {
				return true, nil
			}
			return false, nil
		}
	}
	if _, ok := e.Left.(value.Null); ok {
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return e.Left.SafeCompare(val, e.Operator)
}
func (e *ComparisonExprLVRS) GetTargetCols() []string {
	return []string{e.Right}
}
func (e *ComparisonExprLVRS) GetTargetColsNum() int {
	return 1
}
func (e *ComparisonExprLVRS) Debug() {
	fmt.Println(e.Left.String(), e.Operator, e.Right)
}
func (e *ComparisonExprLVRS) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	if e.Right == indexName && e.Operator != value.NotEqual {
		return true, &ComparisonExprLSRV{Left: e.Right, Operator: e.Operator, Right: e.Left}
	}
	return false, nil
}
func (e *ComparisonExprLVRS) GetRange(indexName string) (bool, *Range) {
	if e.Right == indexName {
		switch e.Operator {
		case value.Equal:
			begin := Point{Val: e.Left, Include: true}
			end := Point{Val: e.Left, Include: true}
			return true, &Range{Begin: &begin, End: &end}
		case value.Great:
			begin := Point{Val: value.Null{Length: 0}, Include: false}
			end := Point{Val: e.Left, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		case value.GreatEqual:
			begin := Point{Val: value.Null{Length: 0}, Include: false}
			end := Point{Val: e.Left, Include: true}
			return true, &Range{Begin: &begin, End: &end}
		case value.Less:
			begin := Point{Val: e.Left, Include: false}
			end := Point{Val: value.Null{Length: 0}, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		case value.LessEqual:
			begin := Point{Val: e.Left, Include: true}
			end := Point{Val: value.Null{Length: 0}, Include: false}
			return true, &Range{Begin: &begin, End: &end}
		}

	}
	return false, nil
}
func (e *ComparisonExprLVRV) Evaluate(row []value.Value) (bool, error) {
	return e.Left.SafeCompare(e.Right, e.Operator)
}
func (e *ComparisonExprLVRV) GetTargetCols() []string {
	return []string{}
}
func (e *ComparisonExprLVRV) GetTargetColsNum() int {
	return 0
}
func (e *ComparisonExprLVRV) Debug() {
	fmt.Println(e.Left.String(), e.Operator, e.Right.String())
}
func (e *ComparisonExprLVRV) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	return false, nil
}
func (e *ComparisonExprLVRV) GetRange(indexName string) (bool, *Range) {

	return false, nil
}
func (e *ComparisonExprLSRS) Evaluate(row []value.Value) (bool, error) {
	vall := row[0]
	valr := row[1]
	if _, ok := vall.(value.Null); ok { //left is NULL
		if _, iok := valr.(value.Null); iok { //right is also NULL
			if e.Operator == value.Equal {
				return true, nil
			} //
			return false, nil
		} else {
			if e.Operator == value.NotEqual {
				return true, nil
			}
			return false, nil
		}
	}
	if _, ok := valr.(value.Null); ok {
		if e.Operator == value.NotEqual {
			return true, nil
		}
		return false, nil
	}
	return vall.SafeCompare(valr, e.Operator)
}
func (e *ComparisonExprLSRS) GetTargetCols() []string {
	return []string{e.Left, e.Right}
}
func (e *ComparisonExprLSRS) GetTargetColsNum() int {
	return 2
}
func (e *ComparisonExprLSRS) Debug() {
	fmt.Println(e.Left, e.Operator, e.Right)
}
func (e *ComparisonExprLSRS) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	return false, nil
}
func (e *ComparisonExprLSRS) GetRange(indexName string) (bool, *Range) {
	return false, nil
}
func (e *AndExpr) Evaluate(row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(row[0:e.LeftNum])
	if err != nil {
		return false, err
	}
	rightOk, err := e.Right.Evaluate(row[e.LeftNum : e.LeftNum+e.RightNum])
	if err != nil {
		return false, err
	}
	if leftOk && rightOk {
		return true, nil
	}
	return false, nil
}

func (e *AndExpr) GetTargetCols() []string {
	return append(e.Left.GetTargetCols(), e.Right.GetTargetCols()...) //maybe with duplicate
}
func (e *AndExpr) GetTargetColsNum() int {
	return e.LeftNum + e.RightNum
}
func (e *AndExpr) Debug() {
	e.Left.Debug()
	fmt.Println(" and ")
	e.Right.Debug()
}

//也是传入一个列名，得到一个对应的索引表达式LSRV，但其实可能有多个。。
//也可以写个函数，把这个列的所有表达式都找出来，然后计算区间
func (e *AndExpr) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	b, c := e.Left.GetIndexExpr(indexName)
	if b {
		b1, c1 := e.Right.GetIndexExpr(indexName)
		if b1 && c1 != nil && c1.Operator == value.Equal {
			return true, c1
		}
		return b, c
	}
	return e.Right.GetIndexExpr(indexName)
}
func (e *AndExpr) GetRange(indexName string) (bool, *Range) {
	f1, rangee1 := e.Left.GetRange(indexName)
	f2, rangee2 := e.Right.GetRange(indexName)
	if f1 && f2 {
		rangee := AndRange(rangee1, rangee2)
		if rangee == nil {
			//区间为空
			return true, nil
		}
		return true, rangee
	} else if !f1 && !f2 {
		//should not enter this branch in final phase
		return false, nil
	} else if !f1 && f2 {
		return true, rangee2
	} else {
		return true, rangee1
	}
}
func (e *OrExpr) Evaluate(row []value.Value) (bool, error) {
	leftOk, err := e.Left.Evaluate(row[0:e.LeftNum])
	if err != nil {
		return false, err
	}
	if leftOk {
		return true, nil
	}
	rightOk, err := e.Right.Evaluate(row[e.LeftNum : e.LeftNum+e.RightNum])
	if err != nil {
		return false, err
	}
	return rightOk, nil
}
func (e *OrExpr) GetTargetCols() []string {
	return append(e.Left.GetTargetCols(), e.Right.GetTargetCols()...)
}
func (e *OrExpr) GetTargetColsNum() int {
	return e.LeftNum + e.RightNum
}
func (e *OrExpr) Debug() {
	e.Left.Debug()
	fmt.Println(" or ")
	e.Right.Debug()

}

//GetIndexExpr 注意 如果是or表达式 直接返回false，因此没法走单索引
func (e *OrExpr) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	return false, nil
}
func (e *OrExpr) GetRange(indexName string) (bool, *Range) {
	//shouldnot enter this function
	return false, nil
}
func (e *NotExpr) Evaluate(row []value.Value) (bool, error) {
	ok, err := e.Expr.Evaluate(row)
	if err != nil {
		return false, err
	}
	return !ok, nil
}
func (e *NotExpr) GetTargetCols() []string {
	return e.Expr.GetTargetCols()
}
func (e *NotExpr) GetTargetColsNum() int {
	return e.LeftNum
}
func (e *NotExpr) Debug() {
	e.Expr.Debug()
	fmt.Println("not ")
}
func (e *NotExpr) GetIndexExpr(indexName string) (bool, *ComparisonExprLSRV) {
	return e.Expr.GetIndexExpr(indexName)
}
func (e *NotExpr) GetRange(indexName string) (bool, *Range) {
	//翻转区间
	return false, nil
}

type InsertStament struct {
	TableName   string
	ColumnNames []string
	Values      []value.Value
}

func (c InsertStament) GetOperationType() OperationType {
	return Insert
}
func (c InsertStament) Show() {

}

type UpdateStament struct {
	TableName string
	SetExpr   []SetExpr
	Where     *Where //maybe is nil!!!
}

func (c UpdateStament) GetOperationType() OperationType {
	return Update
}
func (c UpdateStament) Show() {

}

type DeleteStatement struct {
	TableName string
	Where     *Where //maybe is nil!!!
}

func (c DeleteStatement) GetOperationType() OperationType {
	return Delete
}
func (c DeleteStatement) Show() {
}
func AndRange(rangee1 *Range, rangee2 *Range) *Range {
	var left *Point
	var right *Point
	//确定左端点
	if _, ok := rangee1.Begin.Val.(value.Null); ok {
		left = rangee2.Begin
	} else if _, ok := rangee2.Begin.Val.(value.Null); ok {
		left = rangee1.Begin
	} else {
		if f, _ := rangee1.Begin.Val.Compare(rangee2.Begin.Val, value.Great); f {
			left = rangee1.Begin
		} else if f, _ := rangee2.Begin.Val.Compare(rangee1.Begin.Val, value.Great); f {
			left = rangee2.Begin
		} else {
			if rangee2.Begin.Include && rangee1.Begin.Include {
				left = rangee1.Begin
			} else if rangee2.Begin.Include && !rangee1.Begin.Include {
				left = rangee1.Begin
			} else if rangee1.Begin.Include && !rangee2.Begin.Include {
				left = rangee2.Begin
			} else {
				left = rangee1.Begin
			}
		}
	}

	//确定右端点
	if _, ok := rangee1.End.Val.(value.Null); ok {
		right = rangee2.End
	} else if _, ok := rangee2.End.Val.(value.Null); ok {
		right = rangee1.End
	} else {
		if f, _ := rangee1.End.Val.Compare(rangee2.End.Val, value.Less); f {
			right = rangee1.End
		} else if f, _ := rangee2.End.Val.Compare(rangee1.End.Val, value.Less); f {
			right = rangee2.End
		} else {
			//相同值，比较
			if rangee2.End.Include && rangee1.End.Include {
				right = rangee1.End
			} else if rangee2.End.Include && !rangee1.End.Include {
				right = rangee1.End
			} else if rangee1.End.Include && !rangee2.End.Include {
				right = rangee2.End
			} else {
				right = rangee1.End
			}
		}
	}
	//右端点比左端点小
	if f, _ := right.Val.Compare(left.Val, value.Less); f {
		return nil
	} else if f, _ := right.Val.Compare(left.Val, value.Equal); f {
		if !(right.Include && left.Include) {
			return nil
		}
	}
	return &Range{Begin: left, End: right}
}
