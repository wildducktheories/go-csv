// Given a header-prefixed input stream of CSV records and the specification of a partial key formed from one or more of the fields, generate an augmented,
// header-prefixed, stream of CSV records such that the augmented key of each output record is unique.
//
// For example, given the following input with the partial key Date,Amount
//
//    Date,Amount,Description
//    2014/12/31,100.0,Payment
//    2014/12/31,100.0,Payment
//    2014/12/31,85.0,Payment
//
// Generate an additional column, Sequence, such that the augmented key Date,Amount,Sequence is unique
// for all input records.
//
//    Date,Amount,Description,Sequence
//    2014/12/31,100.0,Payment,
//    2014/12/31,100.0,Payment,1
//    2014/12/31,85.0,Payment,
//
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
	var partialKey, additionalKey string

	flag.StringVar(&partialKey, "partial-key", "", "The columns of the partial key.")
	flag.StringVar(&additionalKey, "additional-key", "", "The column name for the additional key.")
	flag.Parse()

	usage := func() {
		fmt.Printf("usage: uniqify {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	partialKeys, err := csv.Parse(partialKey)
	if err != nil || len(partialKeys) < 1 {
		usage()
		return fmt.Errorf("--partial-key must specify one or more columns")
	}

	if additionalKey == "" {
		usage()
		return fmt.Errorf("--additional-key must specify the name of new column")
	}

	// check that the partial keys exist in the dataHeader
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
	for _, h := range partialKeys {
		if err != nil {
			return err
		}
		if headerRec.Get(h) != h {
			return fmt.Errorf("'%s' is not a field of the input stream", h)
		}
	}

	if headerRec.Get(additionalKey) != "" {
		return fmt.Errorf("'%s' already exists in the header", additionalKey)
	}

	// create a new output stream
	augmentedHeader := make([]string, len(dataHeader)+1)
	copy(augmentedHeader, dataHeader)
	augmentedHeader[len(dataHeader)] = additionalKey

	keys := make(map[string]int)

	writer := rawCsv.NewWriter(os.Stdout)
	writer.Write(augmentedHeader)
	for {
		data, err := reader.Read()
		augmentedData := make([]string, len(dataHeader)+1)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		key := make([]string, len(partialKeys))
		for i, h := range partialKeys {
			key[i] = data.Get(h)
		}
		formattedKey := csv.Format(key)
		additionalKeyValue, ok := keys[formattedKey]
		if !ok {
			keys[formattedKey] = 0
		} else {
			additionalKeyValue++
			keys[formattedKey] = additionalKeyValue
		}
		copy(augmentedData, data.AsSlice())
		if additionalKeyValue > 0 {
			augmentedData[len(dataHeader)] = fmt.Sprintf("%d", additionalKeyValue)
		}
		writer.Write(augmentedData)
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
