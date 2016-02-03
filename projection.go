package csv

// A StringProjection is a function which produces a slice of strings from a Record.
type StringProjection func(r Record) []string
