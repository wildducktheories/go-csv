package csv

// A RecordComparator is a function that returns true if the left record is 'less' than right record according
// to some total order.
type RecordComparator func(l, r Record) bool
