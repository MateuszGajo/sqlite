package main

import (
	"fmt"
	"strconv"
)

func showResultSet(nodes []any, executColumns []map[string]ExecuteColumn) {
	colOrder := []string{}
	for _, node := range nodes {
		switch v := node.(type) {
		case SelectStatementAggregateNode:
			colOrder = append(colOrder, v.rawName)
		case SelectStatementFieldNode:
			colOrder = append(colOrder, v.field)
		default:
			panic("not supported")
		}
	}

	for _, item := range executColumns {
		resData := ""
		for _, col := range colOrder {
			b := item[col]
			switch b.colType {
			case "text":
				v := b.data.([]uint8)
				resData += string(v) + "|"
			case "integer":
				v := b.data.(int)
				resData += strconv.Itoa(v) + "|"
			default:
				panic("unknown type")
			}
		}

		if len(resData) > 0 {
			resData = resData[:len(resData)-1]
		}

		fmt.Println(resData)
	}
}
