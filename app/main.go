package main

import (
	"fmt"
	"reflect"
)

type SqliteServer struct {
	header DbHeader
	reader Reader
}

func (s SqliteServer) getSchemas() []DbSchema {
	schemas := getSchemas(s.reader.read(0))

	reverse(schemas)

	return schemas
}

func (s SqliteServer) handleDbInfo() {
	page := s.reader.read(0)
	page = page[100:]

	btreeHeader := parseBtreeHeader(page[:12])

	fmt.Printf("database page size: %v\n", s.header.dbSizeInBytes)
	fmt.Printf("number of tables: %v\n", btreeHeader.numberOfCells)
}

func (s SqliteServer) handleTablesInfo() {
	schemas := s.getSchemas()

	for i, schema := range schemas {
		fmt.Printf("%s", schema.tableName)
		if i < len(schemas)-1 {
			fmt.Printf(" ")
		}
	}
}

func (s SqliteServer) handleSelectStatement(statement SelectStatement) error {
	schemas := s.getSchemas()
	var schemaa *DbSchema
	for _, schema := range schemas {
		if schema.tableName == statement.from {
			schemaa = &schema
		}
	}
	if schemaa == nil {
		panic("could find page")
		return fmt.Errorf("Couldnt find root page for table :%v", statement.from)
	}

	page := s.reader.read(int(schemaa.rootPage))

	pageParsed := parsePage(page, int(schemaa.rootPage))

	fmt.Println(pageParsed)
	// sql := parseSqlStatement(schemaa.sqlText)
	fmt.Println(string(schemaa.sqlText))

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
	// databaseFilePath := os.Args[1]
	// command := os.Args[2]
	databaseFilePath := "sample.db"
	command := "SELECT COUNT(*) FROM apples"

	reader := NewReader(databaseFilePath)

	server := SqliteServer{
		header: parseDatabaseHeader(reader.readHeader()),
		reader: reader,
	}

	server.handle(command)

}
