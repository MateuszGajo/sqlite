package main

import (
	"fmt"
	"strings"
)

type Tokenizer struct {
	input string
	index int
}

func (t *Tokenizer) eof() bool {
	return t.index >= len(t.input)
}

func (t *Tokenizer) peek() byte {
	if t.eof() {
		return '$'
	}
	return t.input[t.index]
}

func (t *Tokenizer) next() byte {
	t.index++
	return t.peek()
}

func (t *Tokenizer) skipWhiteSpaces() {
	for !t.eof() && (t.peek() == ' ' || t.peek() == '\n' || t.peek() == '\t') {
		t.next()
	}
}

type TokenType string

type Token struct {
	tokenType TokenType
	value     string
}

const (
	spaceToken      TokenType = "SpaceToken"
	selectToken     TokenType = "SelectToken"
	tableToken      TokenType = "tableToken"
	createToken     TokenType = "CreateToken"
	fromToken       TokenType = "FromToken"
	countToken      TokenType = "CountToken"
	identifierToken TokenType = "IdentifierToken"
	lParenToken     TokenType = "lParenToken"
	rParenToken     TokenType = "rParenToken"
	starToken       TokenType = "starToken"
	commaToken      TokenType = "commaToken"
	eofToken        TokenType = "eofToken"
)

var clauseKeywords = map[string]Token{
	"SELECT": Token{tokenType: selectToken},
	"CREATE": Token{tokenType: createToken},
	"TABLE":  Token{tokenType: tableToken},
	"FROM":   Token{tokenType: fromToken},
	"COUNT":  Token{tokenType: countToken, value: "count"},
}

var aggregateTokens = []TokenType{
	countToken,
}

func (t TokenType) isAggregate() bool {
	for _, item := range aggregateTokens {
		if item == t {
			return true
		}
	}
	return false
}

func (t *Tokenizer) parseChars() Token {
	char := t.peek()
	stringOutput := ""
	for (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
		stringOutput += string(char)
		char = t.next()
	}

	token := strings.ToUpper(stringOutput)

	if val, ok := clauseKeywords[token]; ok {
		return val
	}

	return Token{tokenType: identifierToken, value: stringOutput}
}

// "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)"
// Toknizer shoudl skip useless whitespaces
func (t *Tokenizer) tokenizer() []Token {
	tokens := []Token{}
	for !t.eof() {
		switch t.peek() {
		case '(':
			tokens = append(tokens, Token{tokenType: lParenToken})
			t.next()
			t.skipWhiteSpaces()
		case ')':
			tokens = append(tokens, Token{tokenType: rParenToken})
			t.next()
		case '*':
			tokens = append(tokens, Token{tokenType: starToken})
			t.next()
		case ',':
			tokens = append(tokens, Token{tokenType: commaToken})
			t.next()
		case ' ', '\n', '\t':
			t.skipWhiteSpaces()
			if t.peek() == ')' {
				continue
			}
			tokens = append(tokens, Token{tokenType: spaceToken})
		default:
			tokens = append(tokens, t.parseChars())
		}
	}
	tokens = append(tokens, Token{tokenType: eofToken})
	return tokens
}

// Grammar
// sqlStatement ->  SelectStatement | CreateStatement(to do)
// selectStatement -> SelectClause FromClause
// createStatement -> Create Table tableName
// createStatementArgs -> (" MultiString1 ")"
// MultiString1-> string comma Multistring1
// SelectClause -> Select space Aggregate
// FromClause -> From space TableName
// Aggregate -> AgreegateType AggregateArg
// AggregateType -> Count
// AggregateArg -> "(" MultiString ")"
// MultiString -> "*" | string (comma string)*
// TableName -> string

// SELECT COUNT(*) FROM apples

type Parser struct {
	tokens []Token
	index  int
}

func (p *Parser) eof() bool {
	return p.index >= len(p.tokens)
}

func (p *Parser) peek() Token {
	return p.tokens[p.index]
}

func (p *Parser) next() Token {
	if !p.eof() {
		p.index++
	}

	return p.peek()
}

func (p *Parser) expectNext(t TokenType) (Token, error) {
	tok := p.next()

	if tok.tokenType != t {
		return tok, fmt.Errorf("expected token: %v, got: %v", t, tok)
	}

	return tok, nil
}

func (p *Parser) skipWhiteSpaces() {
	if p.peek().tokenType != eofToken && (p.peek().tokenType == spaceToken) {
		p.next()
	}
}

func parseSqlStatement(input string) ASTNode {
	tokenizer := Tokenizer{
		input: input,
	}
	tokens := tokenizer.tokenizer()

	parser := Parser{
		tokens: tokens,
	}

	var astNode ASTNode
	var err error
	switch parser.peek().tokenType {
	case selectToken:
		astNode, err = parser.selectCause()
	case createToken:
		astNode, err = parser.createCause()
	default:
		panic("Unknown statement type: " + parser.peek().tokenType)
	}

	if err != nil {
		panic(err)
	}

	return astNode

}

type ASTNode interface{}

type fieldNode struct {
	name string
	args []string
}

type Agregate string

const (
	countAggregate Agregate = "count"
)

type SelectStatementFieldNode struct {
	field string
}
type SelectStatementAggregateNode struct {
	name    Agregate
	rawName string
	SelectStatementFieldNode
}

type SelectStatementNode interface{}

type SelectStatement struct {
	fields []SelectStatementNode
	from   string
}

type CreateTableStatement struct {
	tableName string
	columns   []CreateTableColumn
}

type Constrain string

const (
	notNull       Constrain = "NotNull"
	primaryKey    Constrain = "PrimaryKey"
	autoIncrement Constrain = "AutoIncrement"
)

// var sqlConstrainTokens = map[Constrain][][2]string{
// 	notNullConstrain:    {{"NOT", "NULL"}},
// 	primaryKeyConstrain: {{"PRIMARY", "KEY"}},
// 	autoIncrement:       {{"AUTOINCREMENT"}},
// }

type CreateTableColumn struct {
	name       string
	columnType string
	constrains []Constrain
}

func (p *Parser) selectCause() (SelectStatement, error) {
	_, err := p.expectNext(spaceToken)
	if err != nil {
		return SelectStatement{}, err
	}

	fields, err := p.selectStatemntFieldOrAggregate()
	if err != nil {
		return SelectStatement{}, err
	}

	// _, err = p.expectNext(spaceToken)
	// if err != nil {
	// 	return SelectStatement{}, err
	// }

	from, err := p.fromClause()
	if err != nil {
		return SelectStatement{}, err
	}

	return SelectStatement{
		fields: fields,
		from:   from,
	}, nil
}

func (p *Parser) selectStatemntFieldOrAggregate() ([]SelectStatementNode, error) {
	// aggregate statement are much complicated than this, we should also check group by statement but at the analyzer phase

	nodes := []SelectStatementNode{}
	var node SelectStatementNode
	var err error
	for {
		token := p.next()

		switch {
		case token.tokenType.isAggregate():
			node, err = p.selectStatementGetAggregateField()
		case token.tokenType == identifierToken:
			node = p.selectStatementGetFields()
		default:
			return nil, fmt.Errorf("expected select statement to have following field or aggregate instead got: %v", token.tokenType)
		}
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
		p.next()

		if p.peek().tokenType == commaToken {
			p.next()
			continue
		} else if p.peek().tokenType != spaceToken {
			return nil, fmt.Errorf("expected select statement to space got: %v", token.tokenType)
		}
		break
	}

	return nodes, nil
}

func (p *Parser) selectStatementGetFields() SelectStatementFieldNode {
	token := p.peek()

	if token.tokenType == starToken {
		return SelectStatementFieldNode{field: "*"}
	}

	return SelectStatementFieldNode{field: token.value}
}

func (p *Parser) selectStatementGetAggregateField() (SelectStatementAggregateNode, error) {
	aggregateType := p.peek()
	_, err := p.expectNext(lParenToken)
	if err != nil {
		return SelectStatementAggregateNode{}, err
	}

	p.next()

	node := p.selectStatementGetFields()

	_, err = p.expectNext(rParenToken)
	if err != nil {
		return SelectStatementAggregateNode{}, err
	}

	return SelectStatementAggregateNode{name: Agregate(aggregateType.value), SelectStatementFieldNode: node, rawName: aggregateType.value + "(" + node.field + ")"}, nil

}

func (p *Parser) createCause() (ASTNode, error) {
	_, err := p.expectNext(spaceToken)
	if err != nil {
		return SelectStatement{}, err
	}

	token := p.next()

	switch token.tokenType {
	case tableToken:
		return p.createTableClause()
	default:
		return nil, fmt.Errorf("unsported keyword: %v", string(token.tokenType))
	}

}

func (p *Parser) createTableClause() (CreateTableStatement, error) {
	_, err := p.expectNext(spaceToken)
	if err != nil {
		return CreateTableStatement{}, err
	}

	tableName := p.next()

	_, err = p.expectNext(spaceToken)
	if err != nil {
		return CreateTableStatement{}, err
	}

	columns, err := p.readCreateTableColumns()

	if err != nil {
		return CreateTableStatement{}, err
	}

	return CreateTableStatement{
		tableName: tableName.value,
		columns:   columns,
	}, nil

}

func (p *Parser) readCreateTableColumns() ([]CreateTableColumn, error) {
	p.next()
	if p.peek().tokenType != lParenToken {
		return nil, fmt.Errorf("expect to start with left parentheses")
	}

	createTableColumns := []CreateTableColumn{}

	for p.peek().tokenType != rParenToken {
		p.next()
		p.skipWhiteSpaces()

		crateTableColumn := CreateTableColumn{constrains: []Constrain{}}
		crateTableColumn.name = p.peek().value

		p.next()

		crateTableColumn.columnType = p.next().value

		p.next()
		for p.peek().tokenType != commaToken && p.peek().tokenType != rParenToken {
			switch strings.ToUpper(p.next().value) {
			case "NOT":
				p.next()
				switch strings.ToUpper(p.next().value) {
				case "NULL":
					crateTableColumn.constrains = append(crateTableColumn.constrains, notNull)
				default:
					return nil, fmt.Errorf("Not such constrain like not %v", p.peek().value)
				}
			case "PRIMARY":
				p.next()
				switch strings.ToUpper(p.next().value) {
				case "KEY":
					crateTableColumn.constrains = append(crateTableColumn.constrains, primaryKey)
				default:
					return nil, fmt.Errorf("Not such constrain like not %v", p.peek().value)
				}
			case "AUTOINCREMENT":
				crateTableColumn.constrains = append(crateTableColumn.constrains, autoIncrement)
			default:
				return nil, fmt.Errorf("No such constrain like: %+v", p.peek().value)
			}
			p.next()

		}
		createTableColumns = append(createTableColumns, crateTableColumn)

	}

	return createTableColumns, nil
}

func (p *Parser) fromClause() (string, error) {
	token, err := p.expectNext(fromToken)
	if err != nil {
		return "", fmt.Errorf("expected from token got: %v", token)
	}

	token, err = p.expectNext(spaceToken)
	if err != nil {
		return "", fmt.Errorf("expected space token got: %v", token)
	}

	token, err = p.expectNext(identifierToken)
	if err != nil {
		return "", fmt.Errorf("expected identifier token got: %v", token)
	}

	return token.value, nil
}
