package utils

import "testing"

func TestIntersect(t *testing.T) {
	intersection, aNotB, bNotA := Intersect([]string{"a", "b"}, []string{"b", "c"})
	if len(intersection) != 1 || intersection[0] != "b" {
		t.Fatalf("intersection %v", intersection)
	}
	if len(aNotB) != 1 || aNotB[0] != "a" {
		t.Fatalf("aNotB %v", aNotB)
	}
	if len(bNotA) != 1 || bNotA[0] != "c" {
		t.Fatalf("bNotA %v", bNotA)
	}
}
