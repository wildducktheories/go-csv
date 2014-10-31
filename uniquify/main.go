// Given a header-prefixed input stream of CSV records and the specification of a partial key (--partial-key)
// formed from one or more of the fields, generate an augmented, header-prefixed, stream of CSV records
// such that the augmented key of each output record is unique. The field used to ensure uniqueness is
// specified by the --additional-key option.
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
	"github.com/wildducktheories/go-csv/utils"

	"flag"
	"fmt"
	"os"
)

func body() error {
	var partialKey, additionalKey string

	flag.StringVar(&partialKey, "partial-key", "", "The fields of the partial key.")
	flag.StringVar(&additionalKey, "additional-key", "", "The field name for the additional key.")
	flag.Parse()

	var line = 0
	var failed = true

	defer func() {
		if failed {
			fmt.Fprintf(os.Stderr, "failed at line: %d\n", line+1)
		}
	}()

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
	line = 1

	// create a stream from the header
	dataHeader := reader.Header()

	i, a, _ := utils.Intersect(partialKeys, dataHeader)
	if len(a) > 0 {
		return fmt.Errorf("%s does not exist in the data header", csv.Format(a))
	}

	i, a, _ = utils.Intersect([]string{additionalKey}, dataHeader)
	if len(i) != 0 {
		return fmt.Errorf("%s already exists in data header", i[0])
	}

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
		line++
		key := make([]string, len(partialKeys))
		for i, h := range partialKeys {
			key[i] = data.Get(h)
		}
		formattedKey := csv.Format(key)
		additionalKeyValue, ok := keys[formattedKey]
		if !ok {
			additionalKeyValue = 0
		} else {
			additionalKeyValue++
		}
		keys[formattedKey] = additionalKeyValue
		copy(augmentedData, data.AsSlice())
		if additionalKeyValue > 0 {
			augmentedData[len(dataHeader)] = fmt.Sprintf("%d", additionalKeyValue)
		}
		writer.Write(augmentedData)
	}
	writer.Flush()
	failed = false
	return nil
}

func main() {
	err := body()
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
