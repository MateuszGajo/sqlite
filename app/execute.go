package main

import "fmt"

type Executor struct {
	reader Reader
}

func NewExecutor(reader Reader) Executor {
	return Executor{
		reader: reader,
	}
}

func (e Executor) execute(plannerNode ExecutionPlan) ([]map[string]ExecuteColumn, error) {
	if len(plannerNode.columns) > 0 {
		return e.getDataa(plannerNode)
	}

	columnsRowData := []map[string]ExecuteColumn{}
	column := make(map[string]ExecuteColumn)
	for _, item := range plannerNode.aggFunc {
		switch item.funcAgg {
		case "count":
			// this is special case, we can read btree header to count it
			if item.arg == "*" {
				schema, err := e.reader.getSchemaByTablename(plannerNode.tablename)

				if err != nil {
					return nil, err
				}

				pages := e.reader.seqRead(int(schema.rootPage))

				numberOfCells := 0
				for _, page := range pages {
					// count headers
					numberOfCells += int(page.btreeHeader.numberOfCells)
				}
				column[item.rawName] = ExecuteColumn{
					name:    item.rawName,
					colType: "integer",
					data:    numberOfCells,
				}
				columnsRowData = append(columnsRowData, column)
			}
		default:
			panic("should never enter here")
		}
	}

	if len(columnsRowData) == 0 {
		panic("nothing to return")
	}

	return columnsRowData, nil

}

type ExecuteColumn struct {
	colType string
	name    string
	data    any
}

func (e Executor) getDataa(plannerNode ExecutionPlan) ([]map[string]ExecuteColumn, error) {

	schema, err := e.reader.getSchemaByTablename(plannerNode.tablename)

	if err != nil {
		return nil, err
	}

	pages := e.reader.seqRead(int(schema.rootPage))

	sql := parseSqlStatement(schema.sqlText)

	createTableSql, ok := sql.(CreateTableStatement)

	columns := make(map[int]PlannerColumn)

mainLoop:
	for i, schemaItem := range createTableSql.columns {
		for _, column := range plannerNode.columns {
			if schemaItem.name == column.name {
				columns[i] = PlannerColumn{
					name:    column.name,
					colType: schemaItem.columnType,
					where:   "",
				}
				continue mainLoop
			}
		}
	}

	if !ok {
		// for simplicity allow only create table, will be extended later
		return nil, fmt.Errorf("reading schema, expected create table statement")
	}

	columnsRowData := []map[string]ExecuteColumn{}
	for _, page := range pages {
	cellLoop:
		for _, cell := range page.cells {
			columnData := make(map[string]ExecuteColumn)
			for i, record := range cell.record {
				column, ok := columns[i]

				if !ok {
					continue
				}

				if column.where != "" {
					panic("implement it later")
					// i think it should skip cell loop and that should cover it
					continue cellLoop
				}

				columnData[column.name] = ExecuteColumn{
					colType: column.colType,
					data:    record,
					name:    column.name,
				}
			}
			columnsRowData = append(columnsRowData, columnData)

			// this is only for groupby statements, it shouldnt execture when there is no groupby
			for _, agg := range plannerNode.aggFunc {
				switch agg.funcAgg {
				case "COUNT":
				case "AVG":
				}
			}
		}
	}

	return columnsRowData, nil
}
