package main

import (
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"testing"
)

func TestExecutor(t *testing.T) {
	reader := NewReader("sample.db")
	executor := NewExecutor(reader)

	executionPlan := ExecutionPlan{
		columns: []PlannerColumn{
			{name: "name", colType: "text"},
		},
		aggFunc:   []AggFunc{},
		tablename: "apples",
		where:     []WhereCondition{{field: "color", operator: "=", comparisonVal: "Red"}},
	}

	data, err := executor.execute(executionPlan)

	if err != nil {
		t.Error(err)
	}

	if len(data) != 1 {
		t.Errorf("Expected to see one item, got: %v", len(data))
	}

	item := data[0]["name"]

	byteVal, ok := item.data.([]byte)

	if !ok {
		t.Errorf("Exepected item to by array of bytes, got: %v", reflect.TypeOf(item.data))
	}

	if string(byteVal) != "Fuji" {
		t.Errorf("Expect name of item to be Fuji got: %v", string(byteVal))
	}
}

func BenchmarkExecute_WithProfiling(b *testing.B) {
	f, err := os.Create("cpu.prof")
	if err != nil {
		b.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	b.Run("Execute", func(b *testing.B) {
		reader := NewReader("sample.db")
		executor := NewExecutor(reader)
		executionPlan := ExecutionPlan{
			columns: []PlannerColumn{
				{name: "name", colType: "text"},
			},
			aggFunc:   []AggFunc{},
			tablename: "apples",
			where:     []WhereCondition{},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := executor.execute(executionPlan)
			if err != nil {
				b.Fatalf("execute failed: %v", err)
			}
		}
	})

	mf, err := os.Create("mem.prof")
	if err != nil {
		b.Fatal("could not create memory profile: ", err)
	}
	defer mf.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(mf); err != nil {
		b.Fatal("could not write memory profile: ", err)
	}
}
