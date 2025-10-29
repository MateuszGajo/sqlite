package main

import (
	"fmt"
	"os"
	"reflect"
)

type SqliteServer struct {
	header DbHeader
	reader Reader
}

func (s SqliteServer) handleDbInfo() {
	page := s.reader.read(0)
	page = page[100:]

	btreeHeader := parseBtreeHeader(page[:12])

	fmt.Printf("database page size: %v\n", s.header.dbSizeInBytes)
	fmt.Printf("number of tables: %v\n", btreeHeader.numberOfCells)
}

func (s SqliteServer) handleTablesInfo() {
	schemas := s.reader.getSchemas()

	for i, schema := range schemas {
		fmt.Printf("%s", schema.tableName)
		if i < len(schemas)-1 {
			fmt.Printf(" ")
		}
	}
}

func (s SqliteServer) handleSelectStatement(statement SelectStatement) error {

	nodes := []any{}

	for _, val := range statement.fields {
		nodes = append(nodes, val)
	}

	planner := CreatePlanner()
	executionPlan := planner.preparePlan(nodes, statement.from, statement.where)

	extutor := NewExecutor(s.reader)
	executeCols, err := extutor.execute(executionPlan)

	if err != nil {
		return err
	}

	showResultSet(nodes, executeCols)

	return nil

}

func (s SqliteServer) handleSqlStatement(sqlStatement string) {
	parsedSql := parseSqlStatement(sqlStatement)

	switch val := parsedSql.(type) {
	case SelectStatement:
		s.handleSelectStatement(val)
	default:
		panic(fmt.Sprintf("not defined sqlType: %s", reflect.TypeOf(val)))
	}
}

func (s SqliteServer) handle(command string) {
	switch command {
	case ".dbinfo":
		s.handleDbInfo()
	case ".tables":
		s.handleTablesInfo()
	default:
		s.handleSqlStatement(command)
	}
}

// Usage: ./your_program.sh sample.db .dbinfo
// sample.db "SELECT COUNT(*) FROM apples"
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	// databaseFilePath := "sample.db"
	// command := "SELECT name, color FROM apples"
	// command := "SELECT name, color FROM apples where color='Red'"
	// command := "SELECT count(*) FROM apples"
	// command := "select pear, apple, raspberry from banana"

	reader := NewReader(databaseFilePath)

	server := SqliteServer{
		header: parseDatabaseHeader(reader.readHeader()),
		reader: reader,
	}

	server.handle(command)

}
