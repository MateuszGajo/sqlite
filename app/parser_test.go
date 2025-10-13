package main

import (
	"reflect"
	"testing"
)

func TestSelectStatementWithCountStarAggregate(t *testing.T) {
	ast := parseSqlStatement("SELECT COUNT(*) FROM apples")

	selectStatement, ok := ast.(SelectStatement)

	if !ok {
		t.Errorf("Exepected type to be select statement")
	}

	if selectStatement.from != "apples" {
		t.Errorf("Expect from tables to be apples, got: %v", selectStatement.from)
	}

	if len(selectStatement.fields) != 1 {
		t.Errorf("Expect to find only 1 field instead we got: %v", len(selectStatement.fields))
	}

	field, ok := selectStatement.fields[0].(SelectStatementAggregateNode)

	if !ok {
		t.Errorf("expected field to be aggregate node, val: %v", field)
	}

	if field.name != countAggregate {
		t.Errorf("Expect aggregate to be count got: %v", field.name)
	}

	if field.field != "*" {
		t.Errorf("Expect aggregate args to be * got: %+v", field.field)
	}
}

func TestSelectStatementWithField(t *testing.T) {
	ast := parseSqlStatement("SELECT aa, bb FROM apples")

	selectStatement, ok := ast.(SelectStatement)

	if !ok {
		t.Errorf("Exepected type to be select statement")
	}

	if selectStatement.from != "apples" {
		t.Errorf("Expect from tables to be apples, got: %v", selectStatement.from)
	}

	if len(selectStatement.fields) != 2 {
		t.Errorf("Expect to find only 2 field instead we got: %v", len(selectStatement.fields))
	}

	firstField, ok := selectStatement.fields[0].(SelectStatementFieldNode)

	if !ok {
		t.Errorf("expected field to be simple field, val: %v", firstField)
	}

	if firstField.field != "aa" {
		t.Errorf("Expect first field to be aa got: %+v", firstField.field)
	}

	secondField, ok := selectStatement.fields[1].(SelectStatementFieldNode)

	if !ok {
		t.Errorf("expected field to be simple field, val: %v", secondField)
	}

	if secondField.field != "bb" {
		t.Errorf("Expect second field to be bb got: %+v", secondField.field)
	}
}

func TestSelectStatementWithFieldAndAggregate(t *testing.T) {
	// TODO: we need to have group by statement validated by analyzer
	ast := parseSqlStatement("SELECT aa, count(*) FROM apples")

	selectStatement, ok := ast.(SelectStatement)

	if !ok {
		t.Errorf("Exepected type to be select statement")
	}

	if selectStatement.from != "apples" {
		t.Errorf("Expect from tables to be apples, got: %v", selectStatement.from)
	}

	if len(selectStatement.fields) != 2 {
		t.Errorf("Expect to find only 2 field instead we got: %v", len(selectStatement.fields))
	}

	firstField, ok := selectStatement.fields[0].(SelectStatementFieldNode)

	if !ok {
		t.Errorf("expected field to be simple field, val: %v", firstField)
	}

	if firstField.field != "aa" {
		t.Errorf("Expect first field to be aa got: %+v", firstField.field)
	}

	secondField, ok := selectStatement.fields[1].(SelectStatementAggregateNode)

	if !ok {
		t.Errorf("expected field to be aggregate node, val: %v", secondField)
	}

	if secondField.name != countAggregate {
		t.Errorf("Expect aggregate to be count got: %v", secondField.name)
	}

	if secondField.field != "*" {
		t.Errorf("Expect aggregate args to be * got: %+v", secondField.field)
	}
}

func TestCreateTableStatement(t *testing.T) {
	ast := parseSqlStatement(`CREATE TABLE apples
(
        id integer primary key autoincrement,
        name text,
        color text
)`)

	createTableStatement, ok := ast.(CreateTableStatement)

	if !ok {
		t.Errorf("Exepected type create table statement")
	}

	if createTableStatement.tableName != "apples" {
		t.Errorf("Expect table name to be apples, got: %v", createTableStatement.tableName)
	}

	if len(createTableStatement.columns) != 3 {
		t.Errorf("expect 3 columns got :%v", len(createTableStatement.columns))
	}

	if createTableStatement.columns[0].columnType != "integer" {
		t.Errorf("Exepect first column to be integer type got: %v", createTableStatement.columns[0].columnType)
	}

	if createTableStatement.columns[0].name != "id" {
		t.Errorf("Exepect first column to be name id: %v", createTableStatement.columns[0].name)
	}

	if !reflect.DeepEqual(createTableStatement.columns[0].constrains, []Constrain{primaryKey, autoIncrement}) {
		t.Errorf("expect first column to have constrain primarykey and autoincrement, got: %+v", createTableStatement.columns[0].constrains)
	}

	if createTableStatement.columns[1].columnType != "text" {
		t.Errorf("Exepect second column to be text type got: %v", createTableStatement.columns[1].columnType)
	}

	if createTableStatement.columns[1].name != "name" {
		t.Errorf("Exepect second column to be name name: %v", createTableStatement.columns[1].name)
	}

	if len(createTableStatement.columns[1].constrains) != 0 {
		t.Errorf("Exepect second column to have 0 constrains: %v", len(createTableStatement.columns[1].constrains))
	}

	if createTableStatement.columns[2].columnType != "text" {
		t.Errorf("Exepect second column to be text type got: %v", createTableStatement.columns[2].columnType)
	}

	if createTableStatement.columns[2].name != "color" {
		t.Errorf("Exepect second column to be color name: %v", createTableStatement.columns[2].name)
	}

	if len(createTableStatement.columns[2].constrains) != 0 {
		t.Errorf("Exepect second column to have 0 constrains: %v", len(createTableStatement.columns[2].constrains))
	}
}
