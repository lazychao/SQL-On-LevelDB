package parser

import (
	"SQL-On-LevelDB/src/interpreter/lexer"
	"SQL-On-LevelDB/src/interpreter/types"
	"log"
)

type lexerWrapper struct {
	impl        *lexer.LexerImpl
	channelSend chan<- types.DStatements
	lastLiteral string
	err         error
}

func newLexerWrapper(li *lexer.LexerImpl, channel chan<- types.DStatements) *lexerWrapper {
	return &lexerWrapper{
		impl:        li,
		channelSend: channel,
	}
}

// yySymType 是.y文件自动生成的  就是union ,,yySymType是一个结构体 有各种终结符和非终结符
func (l *lexerWrapper) Lex(lval *yySymType) int { //和词法分析器的Lex不一样,,,参数是一个指针
	r, err := l.impl.Lex(lval.LastToken) //执行的是词法分析器的Lex，读取一个终结符
	if err != nil {
		log.Fatal(err)
	}
	l.lastLiteral = r.Literal
	tokVal := r.Token
	//更新上一个token信息
	lval.str = r.Literal
	lval.LastToken = tokVal
	return tokVal
}

func (l *lexerWrapper) Error(errStr string) {
	l.err = wrapParseError(l.lastLiteral, errStr)
}
