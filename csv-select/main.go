// Given a header-prefixed input stream of CSV records select the fields that match the specified key (--key)
package main

import (
	rawCsv "encoding/csv"
	"github.com/wildducktheories/go-csv"

	"flag"
	"fmt"
	"os"
	"strings"
)

func body() error {
	var key string

	flag.StringVar(&key, "key", "", "The fields to copy into the output stream")
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

	// check that the key exist in the dataHeader
	reader, err := csv.WithIoReader(os.Stdin)
	if err != nil {
		return fmt.Errorf("cannot parse header from input stream: %v", err)
	}

	// create a stream from the header
	dataHeader := reader.Header()
	formattedDataHeader := csv.Format(dataHeader)
	headerStream, err := csv.WithIoReader(strings.NewReader(formattedDataHeader + "\n" + formattedDataHeader))
	if err != nil {
		return fmt.Errorf("failed to reparse header: %v", err)
	}
	headerRec, err := headerStream.Read()

	// check that every key in the partial-key is also in the data header
	for _, h := range keys {
		if err != nil {
			return err
		}
		if headerRec.Get(h) != h {
			return fmt.Errorf("'%s' is not a field of the input stream", h)
		}
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
