package main

type Planner struct {
}

type AggFunc struct {
	funcAgg string
	rawName string
	arg     string
}

type PlannerColumn struct {
	name    string
	colType string
}

type ExecutionPlan struct {
	columns   []PlannerColumn
	aggFunc   []AggFunc
	tablename string
	where     []WhereCondition
}

func CreatePlanner() Planner {
	return Planner{}
}

func (p Planner) preparePlan(nodes []any, tablename string, where []WhereCondition) ExecutionPlan {
	columns := []PlannerColumn{}
	aggregates := []AggFunc{}

	for _, node := range nodes {
		field := ""
		switch v := node.(type) {
		case SelectStatementAggregateNode:
			field = v.SelectStatementFieldNode.field
			item := AggFunc{
				funcAgg: string(v.name),
				arg:     v.SelectStatementFieldNode.field,
				rawName: v.rawName,
			}
			aggregates = append(aggregates, item)
		case SelectStatementFieldNode:
			field = v.field
		default:
			panic("not supported")
		}

		if field != "*" {
			columns = append(columns, PlannerColumn{name: field})
		}
	}

	return ExecutionPlan{
		columns:   columns,
		aggFunc:   aggregates,
		tablename: tablename,
		where:     where,
	}
}
