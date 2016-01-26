package csv

import (
	"fmt"
	"github.com/wildducktheories/go-csv/utils"
	"sort"
)

// An adapter that converts a slice of CSV records into an instance of sort.Interface using the
// specified comparators, in order, to compare records.
type Sortable struct {
	Data        []Record
	Comparators []func(i, j int) bool
}

// An implementation of sort.Interface.Len()
func (b *Sortable) Len() int {
	return len(b.Data)
}

// An implementation of sort.Interface.Swap()
func (b *Sortable) Swap(i, j int) {
	b.Data[i], b.Data[j] = b.Data[j], b.Data[i]
}

// An implementation of sort.Interface.Less()
func (b *Sortable) Less(i, j int) bool {
	for _, c := range b.Comparators {
		if c(i, j) {
			return true
		} else if c(j, i) {
			return false
		}
	}
	return false
}

// Converts the receiver into a SortProcess with the specified keys.
func (b *Sortable) AsSortProcess(keys []string) *SortProcess {
	return &SortProcess{
		Keys: keys,
		AsSort: func(data []Record) sort.Interface {
			b.Data = data
			return b
		},
	}
}

// Answer a comparator for the field named k, using the string comparator specified by less.
func (b *Sortable) Comparator(k string, less func(l, r string) bool) func(i, j int) bool {
	return func(i, j int) bool {
		return less(b.Data[i].Get(k), b.Data[j].Get(k))
	}
}

// Answers true if l is less than r, according to a lexical comparison
func LessStrings(l, r string) bool {
	return l < r
}

// Answers true if the numeric value of l is less than r according to a numerical
// comparison (if l and r are both parseable as floats) or according to a lexical
// comparison otherwise.
func LessNumericStrings(l, r string) bool {
	var lf, rf float64
	if _, err := fmt.Sscanf(l, "%f", &lf); err != nil {
		return LessStrings(l, r)
	} else if _, err := fmt.Sscanf(r, "%f", &rf); err != nil {
		return LessStrings(l, r)
	} else {
		return lf < rf
	}
}

// Specifies the keys to be used by a CSV sort. Columns to be used as sort keys are specified with Keys, in order of precedence. Numeric
// is the list of keys which for which a numeric comparison should be used.
type SortKeys struct {
	Keys    []string
	Numeric []string
}

// Answer a Sort for the specified slice of CSV records, using the comparators derived from the
// keys specified by the receiver.
func (p *SortKeys) AsSort(data []Record) sort.Interface {
	return p.AsSortable(data)
}

// Answer a Sortable whose comparators have been initialized with string or numerical string
// comparators according the specification of the receiver.
func (p *SortKeys) AsSortable(data []Record) *Sortable {
	bk := &Sortable{
		Data:        data,
		Comparators: make([]func(i, j int) bool, len(p.Keys), len(p.Keys)),
	}
	for i, k := range p.Keys {
		bk.Comparators[i] = bk.Comparator(k, LessStrings)
		for _, n := range p.Numeric {
			if n == k {
				bk.Comparators[i] = bk.Comparator(k, LessNumericStrings)
				break
			}
		}
	}
	return bk
}

// Derive a SortProcess from the receiver.
func (p *SortKeys) AsSortProcess() *SortProcess {
	return &SortProcess{
		AsSort: p.AsSort,
		Keys:   p.Keys,
	}
}

// A process, which given a CSV reader, sorts a stream of Records using the sort
// specified by the result of the AsSort function. The stream is checked to verify
// that it has the specified keys.
type SortProcess struct {
	AsSort func(data []Record) sort.Interface
	Keys   []string
}

// Run the sort process specified by the receiver against the specified CSV reader,
// writing the results to a Writer constructed from the specified builder.
// Termination of the sort process is signalled by writing nil or at most one error
// into the specified error channel.
// It is an error to apply the receiving process to a reader whose Header is not
// a strict superset of the receiver's Keys.
func (p *SortProcess) Run(reader Reader, builder WriterBuilder, errCh chan<- error) {

	errCh <- func() error {
		defer reader.Close()

		keys := p.Keys

		// get the data header
		dataHeader := reader.Header()

		_, x, _ := utils.Intersect(keys, dataHeader)

		if len(x) != 0 {
			return fmt.Errorf("invalid keys: %v", x)
		}

		if all, err := ReadAll(reader); err != nil {
			return err
		} else {

			writer := builder(dataHeader)
			defer writer.Close(err)

			sort.Sort(p.AsSort(all))

			for _, e := range all {
				if err := writer.Write(e); err != nil {
					return err
				}
			}
		}
		return nil
	}()
}
