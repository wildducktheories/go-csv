// Given a header-prefixed input stream of CSV records select the fields that match the specified key (--key).
// If --permute-only is is specified, all the fields of the input stream are preserved, but the output stream
// is permuted so that the key fields occupy the left-most fields of the output stream. The remaining fields
// are preserved in their original order.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"
)

type process struct {
	keys        []string
	permuteOnly bool
}

func configure(args []string) (*process, error) {
	flags := flag.NewFlagSet("csv-select", flag.ExitOnError)
	var key string
	var permuteOnly bool

	flags.StringVar(&key, "key", "", "The fields to copy into the output stream")
	flags.BoolVar(&permuteOnly, "permute-only", false, "Preserve all the fields of the input, but put the specified keys first")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	usage := func() {
		fmt.Printf("usage: csv-select {options}\n")
		flags.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	keys, err := csv.Parse(key)
	if err != nil || len(keys) < 1 {
		usage()
		return nil, fmt.Errorf("--key must specify one or more columns")
	}

	return &process{
		keys:        keys,
		permuteOnly: permuteOnly,
	}, nil

}

func (p *process) run(reader csv.Reader, builder csv.WriterBuilder) (err error) {
	defer reader.Close()

	keys := p.keys
	permuteOnly := p.permuteOnly

	// get the data header
	dataHeader := reader.Header()

	_, _, b := utils.Intersect(keys, dataHeader)
	if len(b) > 0 && permuteOnly {
		extend := make([]string, len(keys)+len(b))
		copy(extend, keys)
		copy(extend[len(keys):], b)
		keys = extend
	}

	// create a new output stream
	writer := builder(keys)
	defer writer.Close(err)
	for data := range reader.C() {
		outputData := writer.Blank()
		outputData.PutAll(data)
		if err := writer.Write(outputData); err != nil {
			return err
		}
	}
	return reader.Error()
}

func main() {
	var p *process
	var err error

	if p, err = configure(os.Args[1:]); err == nil {
		err = p.run(csv.WithIoReader(os.Stdin), csv.WithIoWriter(os.Stdout))
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
