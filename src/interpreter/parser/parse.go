package parser

import (
	"SQL-On-LevelDB/src/interpreter/lexer"
	"SQL-On-LevelDB/src/interpreter/types"
	"fmt"
	"io"
)

// Parse returns parsed Spanner DDL statements.
func Parse(r io.Reader, channel chan<- types.DStatements) (err error) {
	err = nil
	defer func() {
		//捕获panic，这个函数会直接退出，但是调用这个函数的(main)依旧正常运行
		if p := recover(); p != nil {
			err = fmt.Errorf("internal error: %v", p)
			//fmt.Println(err)
		}

	}()
	impl := lexer.NewLexerImpl(r, &keywordTokenizer{})
	l := newLexerWrapper(impl, channel)
	yyParse(l)
	if l.err != nil {
		err = l.err
	}
	return err
}
