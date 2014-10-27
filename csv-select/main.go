// Given a header-prefixed input stream of CSV records select the fields that match the specified key (--key).
// If --permute-only is is specified, all the fields of the input stream are preserved, but the output stream
// is permuted so that the key fields occupy the left-most fields of the outout stream. The remaining fields
// are preserved in their original order.
package main

import (
	rawCsv "encoding/csv"

	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"
)

func body() error {
	var key string
	var permuteOnly bool

	flag.StringVar(&key, "key", "", "The fields to copy into the output stream")
	flag.BoolVar(&permuteOnly, "permute-only", false, "Preserve all the fields of the input, but put the specified keys first")
	flag.Parse()

	usage := func() {
		fmt.Printf("usage: select {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	keys, err := csv.Parse(key)
	if err != nil || len(keys) < 1 {
		usage()
		return fmt.Errorf("--key must specify one or more columns")
	}

	// open the reader
	reader, err := csv.WithIoReader(os.Stdin)
	if err != nil && err.Error() != "EOF" {
		return fmt.Errorf("cannot parse header from input stream: %v", err)
	}

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
	writer := rawCsv.NewWriter(os.Stdout)
	writer.Write(keys)
	for {
		data, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		outputData := make([]string, len(keys))
		for i, h := range keys {
			outputData[i] = data.Get(h)
		}
		writer.Write(outputData)
	}
	writer.Flush()

	return nil
}

func main() {
	err := body()
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
