package main

import (
	"fmt"
	"reflect"
)

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
		return e.exectureColumnSearch(plannerNode)
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

func (e Executor) getColumnHashMap(createTableSql CreateTableStatement, plannerNode ExecutionPlan) map[int]PlannerColumn {
	columns := make(map[int]PlannerColumn)

mainLoop:
	for i, schemaItem := range createTableSql.columns {
		for _, column := range plannerNode.columns {
			if schemaItem.name == column.name {
				columns[i] = PlannerColumn{
					name:    column.name,
					colType: schemaItem.columnType,
				}
				continue mainLoop
			}
		}
	}

	return columns
}

func (e Executor) getWhereHashMap(createTableSql CreateTableStatement, whereCondition []WhereCondition) map[int]WhereCondition {
	columns := make(map[int]WhereCondition)

mainLoop:
	for i, schemaItem := range createTableSql.columns {
		for _, whereCon := range whereCondition {
			if schemaItem.name == whereCon.field {
				columns[i] = whereCon
				continue mainLoop
			}
		}
	}

	return columns
}

func (e Executor) exectureColumnSearch(plannerNode ExecutionPlan) ([]map[string]ExecuteColumn, error) {

	schema, err := e.reader.getSchemaByTablename(plannerNode.tablename)

	if err != nil {
		return nil, err
	}

	pages := e.reader.seqRead(int(schema.rootPage))

	sql := parseSqlStatement(schema.sqlText)

	createTableSql, ok := sql.(CreateTableStatement)

	columns := e.getColumnHashMap(createTableSql, plannerNode)

	if !ok {
		// for simplicity allow only create table, will be extended later
		return nil, fmt.Errorf("reading schema, expected create table statement")
	}

	whereHashTable := e.getWhereHashMap(createTableSql, plannerNode.where)

	columnsRowData := e.getRawData(pages, columns, whereHashTable, plannerNode.aggFunc)

	return columnsRowData, nil
}

func (e Executor) getRawData(pages []Page, columns map[int]PlannerColumn, whereHashTable map[int]WhereCondition, aggFunc []AggFunc) []map[string]ExecuteColumn {
	columnsRowData := []map[string]ExecuteColumn{}
	for _, page := range pages {
	cellLoop:
		for _, cell := range page.cells {
			columnData := make(map[string]ExecuteColumn)
			for i, record := range cell.record {
				column, colOk := columns[i]

				whereCon, whereOk := whereHashTable[i]
				if whereOk {
					switch whereCon.operator {
					case "=":
						switch v := record.(type) {
						case []byte:
							if string(v) != whereCon.comparisonVal {
								continue cellLoop
							}
						default:
							panic(fmt.Sprintf("record type in where clause not supported, type: %v", reflect.TypeOf(v)))
						}
					default:
						panic(fmt.Sprintf("not supported operator in where statement %v", whereCon.operator))
					}
				}
				if !colOk {
					continue
				}

				columnData[column.name] = ExecuteColumn{
					colType: column.colType,
					data:    record,
					name:    column.name,
				}
			}
			columnsRowData = append(columnsRowData, columnData)

			// this is only for groupby statements, it shouldnt execture when there is no groupby
			for _, agg := range aggFunc {
				switch agg.funcAgg {
				case "COUNT":
				case "AVG":
				}
			}
		}
	}

	return columnsRowData
}
