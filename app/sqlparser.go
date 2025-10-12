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
	for !t.eof() && (t.peek() == ' ' || t.peek() == '\n') {
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
		case ' ', '\n':
			t.skipWhiteSpaces()
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

type AggregateNode struct {
	name string
	args []string
}

type SelectStatement struct {
	aggregate AggregateNode
	from      string
}

type CreateTableStatement struct {
	tableName string
	args      [][]string
}

func (p *Parser) selectCause() (SelectStatement, error) {
	_, err := p.expectNext(spaceToken)
	if err != nil {
		return SelectStatement{}, err
	}

	aggregate, err := p.Aggregate()
	if err != nil {
		return SelectStatement{}, err
	}

	from, err := p.fromClause()
	if err != nil {
		return SelectStatement{}, err
	}

	return SelectStatement{
		aggregate: aggregate,
		from:      from,
	}, nil
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

	args, err := p.readArgsWithCommasn()

	if err != nil {
		return CreateTableStatement{}, err
	}

	return CreateTableStatement{
		tableName: tableName.value,
		args:      args,
	}, nil

}

func (p *Parser) readArgsWithCommasn() ([][]string, error) {
	token := p.next()
	if token.tokenType != lParenToken {
		return nil, fmt.Errorf("Expect to start with left parentheses")
	}
	token = p.next()
	// i := 0;
	args1 := [][]string{}

main:
	for {
		p.skipWhiteSpaces()
		args := []string{}
		for token.tokenType != commaToken {
			args = append(args, token.value)
			p.next()
			p.skipWhiteSpaces()
			token = p.peek()
			if token.tokenType == rParenToken {
				args1 = append(args1, args)
				break main
			}
			if token.tokenType == commaToken {
				p.next()
				p.skipWhiteSpaces()
				token = p.peek()
				break
			}

		}
		args1 = append(args1, args)

	}

	return args1, nil
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

func (p *Parser) Aggregate() (AggregateNode, error) {
	name, err := p.expectNext(countToken)
	if err != nil {
		return AggregateNode{}, err
	}
	_, err = p.expectNext(lParenToken)
	if err != nil {
		return AggregateNode{}, err
	}

	tok := p.next()

	if tok.tokenType != starToken && tok.tokenType != identifierToken {
		return AggregateNode{}, fmt.Errorf("expected star token or identifier, got: %v", tok)
	}

	if tok.tokenType == starToken {
		_, err := p.expectNext(rParenToken)
		if err != nil {
			return AggregateNode{}, err
		}

		_, err = p.expectNext(spaceToken)
		if err != nil {
			return AggregateNode{}, err
		}

		return AggregateNode{
			name: string(name.value),
			args: []string{"*"},
		}, nil
	}

	token := p.next()
	args := []string{}

	for token.tokenType != rParenToken {
		args = append(args, token.value)
		_, err = p.expectNext(commaToken)
		if err != nil {
			return AggregateNode{}, err
		}
		token, err = p.expectNext(spaceToken)
		if err != nil {
			return AggregateNode{}, err
		}

		if token.tokenType == eofToken {
			return AggregateNode{}, fmt.Errorf("unexpected eof token when parsing aggregate")
		}
	}

	_, err = p.expectNext(spaceToken)
	if err != nil {
		return AggregateNode{}, err
	}

	return AggregateNode{
		name: string(name.value),
		args: args,
	}, nil

}
