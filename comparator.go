package csv

// A RecordComparator is a function that returns true if the left Record is 'less' than the right Record
// according to some total order.
type RecordComparator func(l, r Record) bool

// Constructs a single RecordComparator from a slice of RecordComparators
func AsRecordComparator(comparators []RecordComparator) RecordComparator {
	return func(l, r Record) bool {
		for _, c := range comparators {
			if c(l, r) {
				return true
			} else if c(r, l) {
				return false
			}
		}
		return false
	}

}

// A StringComparator is a function that returns true if the left string is 'less' then the right string
// according to some total order.
type StringComparator func(l, r string) bool

// A StringSliceComparator is a function that returns true if the left slice is 'less' than the right slice
// according to some total order.
type StringSliceComparator func(l, r []string) bool

func AsStringSliceComparator(comparators []StringComparator) StringSliceComparator {
	return func(l, r []string) bool {
		for i, c := range comparators {
			if c(l[i], r[i]) {
				return true
			} else if c(r[i], l[i]) {
				return false
			}
		}
		return false
	}
}
