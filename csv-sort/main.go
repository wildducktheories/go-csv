// Given a header-prefixed input stream of CSV records, sort the stream according to the lexical order
// of the columns specified by --key.
//
// To use a numerical comparison rather than a lexical comparison, list the columns for which a numerical
// comparison is to be performed with --numeric. The columns specified with --numeric must be
// a strict subset of the columns specified by --key.
//
// The results of performing a numerical sort with columns that do not contain strictly numerical values
// is deterministic, but not well defined.
//
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"
)

type process struct {
	keys    []string
	numeric []string
}

func configure(args []string) (*process, error) {
	flags := flag.NewFlagSet("csv-sort", flag.ExitOnError)
	var key string
	var numericKey string

	flags.StringVar(&key, "key", "", "The fields to sort the input stream by.")
	flags.StringVar(&numericKey, "numeric", "", "The specified fields are numeric fields.")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	usage := func() {
		fmt.Printf("usage: csv-sort {options}\n")
		flags.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	keys, err := csv.Parse(key)
	if err != nil || len(keys) < 1 {
		usage()
		return nil, fmt.Errorf("--key must specify one or more columns")
	}

	numeric, err := csv.Parse(numericKey)
	if err != nil && len(numericKey) > 0 {
		usage()
		return nil, fmt.Errorf("--numeric must specify the list of numeric keys")
	}

	i, _, _ := utils.Intersect(keys, numeric)
	if len(i) < len(numeric) {
		return nil, fmt.Errorf("--numeric must be a strict subset of --key")
	}

	return &process{
		keys:    keys,
		numeric: numeric,
	}, nil

}

func lessStrings(l, r string) bool {
	return l < r
}

func lessNumbers(l, r string) bool {
	var lf, rf float64
	if _, err := fmt.Sscanf(l, "%f", &lf); err != nil {
		return lessStrings(l, r)
	} else if _, err := fmt.Sscanf(r, "%f", &rf); err != nil {
		return lessStrings(l, r)
	} else {
		return lf < rf
	}
}

type ByKeys struct {
	data        []csv.Record
	comparators []func(i, j int) bool
}

func (b *ByKeys) Len() int {
	return len(b.data)
}

func (b *ByKeys) Swap(i, j int) {
	b.data[i], b.data[j] = b.data[j], b.data[i]
}

func (b *ByKeys) Less(i, j int) bool {
	for _, c := range b.comparators {
		if c(i, j) {
			return true
		} else if c(j, i) {
			return false
		}
	}
	return false
}

func (b *ByKeys) comparator(k string, f func(l, r string) bool) func(i, j int) bool {
	return func(i, j int) bool {
		return f(b.data[i].Get(k), b.data[j].Get(k))
	}
}

// Answer a sortable object in which all the comparators have been defined as either
// lessStrings or lessNumbers.
func NewByKeys(data []csv.Record, keys []string, numeric []string) *ByKeys {
	bk := &ByKeys{
		data:        data,
		comparators: make([]func(i, j int) bool, len(keys), len(keys)),
	}
	for i, k := range keys {
		bk.comparators[i] = bk.comparator(k, lessStrings)
		for _, n := range numeric {
			if n == k {
				bk.comparators[i] = bk.comparator(k, lessNumbers)
				break
			}
		}
	}
	return bk
}

func (p *process) run(reader csv.Reader, builder csv.WriterBuilder, errCh chan<- error) {

	errCh <- func() error {
		defer reader.Close()

		keys := p.keys
		numeric := p.numeric

		// get the data header
		dataHeader := reader.Header()

		_, x, _ := utils.Intersect(keys, dataHeader)

		if len(x) != 0 {
			return fmt.Errorf("invalid keys: %v", x)
		}

		if all, err := csv.ReadAll(reader); err != nil {
			return err
		} else {

			writer := builder(dataHeader)
			defer writer.Close(err)

			sort.Sort(NewByKeys(all, keys, numeric))

			for _, e := range all {
				if err := writer.Write(e); err != nil {
					return err
				}
			}
		}
		return nil
	}()
}

func main() {
	var p *process
	var err error
	var errCh = make(chan error, 1)

	if p, err = configure(os.Args[1:]); err == nil {
		p.run(csv.WithIoReader(os.Stdin), csv.WithIoWriter(os.Stdout), errCh)
		err = <-errCh
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
