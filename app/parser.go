package main

import (
	"fmt"
	"strings"
)

type Tokenizer struct {
	input string
	index int
}

func (t Tokenizer) eof() bool {
	return t.index >= len(t.input)
}

func (t Tokenizer) peek() byte {
	if t.eof() {
		return '$'
	}
	return t.input[t.index]
}

func (t Tokenizer) next() byte {
	t.index++
	return t.peek()
}

func (t Tokenizer) skipWhiteSpaces() {
	for !t.eof() && t.input[t.index] == ' ' {
		t.index++
	}
}

type TokenType string

const (
	spaceToken      TokenType = "SpaceToken"
	selectToken     TokenType = "SelectToken"
	fromToken       TokenType = "FromToken"
	countToken      TokenType = "CountToken"
	identifierToken TokenType = "IdentifierToken"
	lParenToken     TokenType = "lParenToken"
	rParenToken     TokenType = "rParenToken"
	starToken       TokenType = "starToken"
	commaToken      TokenType = "commaToken"
	eofToken        TokenType = "eofToken"
)

var clauseKeywords = map[string]TokenType{
	"SELECT": selectToken,
	"FROM":   fromToken,
	"COUNT":  countToken,
}

func (t Tokenizer) parseChars() TokenType {
	char := t.peek()
	stringOutput := char
	for (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
		char := t.next()
		stringOutput += char
	}

	token := strings.ToUpper(string(stringOutput))

	if val, ok := clauseKeywords[token]; ok {
		return val
	}

	return identifierToken
}

// Toknizer shoudl skip useless whitespaces
func (t Tokenizer) tokenizer() []TokenType {
	tokens := []TokenType{}
	for !t.eof() {
		switch t.peek() {
		case '(':
			tokens = append(tokens, lParenToken)
		case ')':
			tokens = append(tokens, rParenToken)
		case '*':
			tokens = append(tokens, starToken)
		case ',':
			tokens = append(tokens, commaToken)
		case ' ':
			t.skipWhiteSpaces()
			tokens = append(tokens, spaceToken)
		default:
			tokens = append(tokens, t.parseChars())
		}
	}
	tokens = append(tokens, eofToken)
	return tokens
}

// Grammar
// sqlStatement ->  SelectStatement | UpdateStatement(to do)
// selectStatement -> SelectClause FromClause
// SelectClause -> Select space Aggregate
// FromClause -> From space TableName
// Aggregate -> AgreegateType AggregateArg
// AggregateType -> Count
// AggregateArg -> "(" MultiString ")"
// MultiString -> "*" | string (comma string)*
// TableName -> string

// SELECT COUNT(*) FROM apples

type Parser struct {
	tokens []TokenType
	index  int
}

func (p Parser) eof() bool {
	return p.index >= len(p.tokens)
}

func (p Parser) peek() TokenType {
	return p.tokens[p.index]
}

func (p Parser) next() TokenType {
	if !p.eof() {
		p.index++
	}

	return p.peek()
}

func (p Parser) expectNext(t TokenType) (TokenType, error) {
	tok := p.next()

	if tok != t {
		return tok, fmt.Errorf("Expected token: %v, got: %v", t, tok)
	}

	return tok, nil
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
	switch parser.next() {
	case selectToken:
		astNode, err = parser.selectCause()
	default:
		panic("Unknown statement type: " + parser.peek())
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
	from      []string
}

func (p Parser) selectCause() (SelectStatement, error) {
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

func (p Parser) fromClause() ([]string, error) {
	token, err := p.expectNext(fromToken)
	if err != nil {
		return nil, fmt.Errorf("expected from token got: %v", token)
	}

	token, err = p.expectNext(spaceToken)
	if err != nil {
		return nil, fmt.Errorf("expected space token got: %v", token)
	}

	token, err = p.expectNext(identifierToken)
	if err != nil {
		return nil, fmt.Errorf("expected identifier token got: %v", token)
	}

	return []string{string(token)}, nil
}

func (p Parser) Aggregate() (AggregateNode, error) {
	name, err := p.expectNext(identifierToken)
	if err != nil {
		return AggregateNode{}, err
	}
	_, err = p.expectNext(lParenToken)
	if err != nil {
		return AggregateNode{}, err
	}

	tok := p.next()

	if tok != starToken && tok != identifierToken {
		return AggregateNode{}, fmt.Errorf("Expected star token or identifier, got: %v", tok)
	}

	if tok == starToken {
		_, err := p.expectNext(rParenToken)
		if err != nil {
			return AggregateNode{}, err
		}

		return AggregateNode{
			name: string(name),
			args: []string{"*"},
		}, nil
	}

	token := p.next()
	args := []string{}

	for token != rParenToken {
		args = append(args, string(token))
		token, err = p.expectNext(commaToken)
		if err != nil {
			return AggregateNode{}, err
		}
		token, err = p.expectNext(spaceToken)
		if err != nil {
			return AggregateNode{}, err
		}

		if token == eofToken {
			return AggregateNode{}, fmt.Errorf("Unexpected eof token when parsing aggregate")
		}
	}

	return AggregateNode{
		name: string(name),
		args: args,
	}, nil

}
