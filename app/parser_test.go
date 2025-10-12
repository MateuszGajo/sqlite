package main

import (
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	ast := parseSqlStatement("SELECT COUNT(*) FROM apples")

	selectStatement, ok := ast.(SelectStatement)

	if !ok {
		t.Errorf("Exepected type to be select statement")
	}

	if selectStatement.from != "apples" {
		t.Errorf("Expect from tables to be apples, got: %v", selectStatement.from)
	}

	if selectStatement.aggregate.name != "count" {
		t.Errorf("Expect aggregate to be count got: %v", selectStatement.aggregate.name)
	}

	if selectStatement.aggregate.args[0] != "*" {
		t.Errorf("Expect aggregate args to be * got: %+v", selectStatement.aggregate.args)
	}
}

func TestParser2(t *testing.T) {
	ast := parseSqlStatement(`CREATE TABLE apples
(
        id integer primary key autoincrement,
        name text,
        color text
)`)
	fmt.Println(ast)
	t.Errorf("d")

	// selectStatement, ok := ast.(SelectStatement)

	// if !ok {
	// 	t.Errorf("Exepected type to be select statement")
	// }

	// if selectStatement.from != "apples" {
	// 	t.Errorf("Expect from tables to be apples, got: %v", selectStatement.from)
	// }

	// if selectStatement.aggregate.name != "count" {
	// 	t.Errorf("Expect aggregate to be count got: %v", selectStatement.aggregate.name)
	// }

	// if selectStatement.aggregate.args[0] != "*" {
	// 	t.Errorf("Expect aggregate args to be * got: %+v", selectStatement.aggregate.args)
	// }
}
