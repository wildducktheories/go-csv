package csv

import (
	"fmt"
	"github.com/wildducktheories/go-csv/utils"
	"sort"
)

// An adapter that converts a slice of CSV records into an instance of sort.Interface using the
// specified comparators, in order, to compare records.
type Sortable struct {
	Keys        []string
	Data        []Record
	Comparators []SortComparator
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

// Derives a SortProcess from the receiver. Note that it isn't safe
// to run multiple processes derived from the same Sortable at the same
// time.
func (b *Sortable) AsSortProcess() *SortProcess {
	return &SortProcess{
		Keys: b.Keys,
		AsSort: func(data []Record) sort.Interface {
			b.Data = data
			return b
		},
	}
}

// Answer a comparator for the field named k, using the string comparator specified by less.
func (b *Sortable) Comparator(k string, less StringComparator) SortComparator {
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

// Specifies the keys to be used by a CSV sort.
type SortKeys struct {
	Keys     []string // list of columns to use for sorting
	Numeric  []string // list of columns for which a numerical string comparison is used
	Reversed []string // list of columns for which the comparison is reversed
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
		Keys:        p.Keys,
		Data:        data,
		Comparators: make([]SortComparator, len(p.Keys), len(p.Keys)),
	}
	for x, c := range p.AsRecordComparators() {
		c := c
		bk.Comparators[x] = func(i, j int) bool {
			return c(bk.Data[i], bk.Data[j])
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

// Derive a StringProjection from the sort keys.
func (p *SortKeys) AsStringProjection() StringProjection {
	return func(r Record) []string {
		result := make([]string, len(p.Keys))
		for i, k := range p.Keys {
			result[i] = r.Get(k)
		}
		return result
	}
}

// Answers a comparator that can compare two slices.
func (p *SortKeys) AsStringSliceComparator() StringSliceComparator {
	numeric := utils.NewIndex(p.Numeric)
	reverseIndex := utils.NewIndex(p.Reversed)
	comparators := make([]StringComparator, len(p.Keys))
	for i, k := range p.Keys {
		if numeric.Contains(k) {
			comparators[i] = LessNumericStrings
		} else {
			comparators[i] = LessStrings
		}
		if reverseIndex.Contains(k) {
			f := comparators[i]
			comparators[i] = func(l, r string) bool {
				return !f(l, r)
			}
		}
	}
	return AsStringSliceComparator(comparators)
}

// Answers a slice of comparators that can compare two records.
func (p *SortKeys) AsRecordComparators() []RecordComparator {
	numeric := utils.NewIndex(p.Numeric)
	reverseIndex := utils.NewIndex(p.Reversed)
	comparators := make([]RecordComparator, len(p.Keys))
	for i, k := range p.Keys {
		k := k
		if numeric.Contains(k) {
			comparators[i] = func(l, r Record) bool {
				return LessNumericStrings(l.Get(k), r.Get(k))
			}
		} else {
			comparators[i] = func(l, r Record) bool {
				return LessStrings(l.Get(k), r.Get(k))
			}
		}
		if reverseIndex.Contains(k) {
			f := comparators[i]
			comparators[i] = func(l, r Record) bool {
				return !f(l, r)
			}
		}
	}
	return comparators
}

// Answers a comparator that can compare two records.
func (p *SortKeys) AsRecordComparator() RecordComparator {
	return AsRecordComparator(p.AsRecordComparators())
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

	errCh <- func() (err error) {
		defer reader.Close()

		keys := p.Keys

		// get the data header
		dataHeader := reader.Header()
		writer := builder(dataHeader)
		defer writer.Close(err)

		_, x, _ := utils.Intersect(keys, dataHeader)
		if len(x) != 0 {
			return fmt.Errorf("invalid keys: %v", x)
		}

		if all, err := ReadAll(reader); err != nil {
			return err
		} else {

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
