package main

import "testing"

func TestVarint(t *testing.T) {
	data := []byte{128, 1, 4}
	val, rest := parseVarint(data)

	if val != 1 {
		t.Errorf("wrong val, got: %v", val)
	}

	if len(rest) != 1 && rest[0] != 4 {
		t.Errorf("expected to return one byte '4', got: %v", rest[0])
	}
}

func TestVarint2(t *testing.T) {
	data := []byte{0x81, 0x47}
	val, _ := parseVarint(data)
	// val, _ := parseVarint2(data, 0)

	if val != 199 {
		t.Errorf("wrong val, got: %v", val)
	}

}
