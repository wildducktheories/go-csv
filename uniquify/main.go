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
	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"

	"flag"
	"fmt"
	"os"
)

type process struct {
	partialKeys   []string
	additionalKey string
}

func configure(args []string) (*process, error) {
	var partialKey, additionalKey string

	flags := flag.NewFlagSet("uniquify", flag.ContinueOnError)

	flags.StringVar(&partialKey, "partial-key", "", "The fields of the partial key.")
	flags.StringVar(&additionalKey, "additional-key", "", "The field name for the additional key.")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	usage := func() {
		fmt.Printf("usage: uniqify {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	partialKeys, err := csv.Parse(partialKey)
	if err != nil || len(partialKeys) < 1 {
		usage()
		return nil, fmt.Errorf("--partial-key must specify one or more columns")
	}

	if additionalKey == "" {
		usage()
		return nil, fmt.Errorf("--additional-key must specify the name of new column")
	}

	return &process{
		partialKeys:   partialKeys,
		additionalKey: additionalKey,
	}, nil
}

func (p *process) run(reader csv.Reader, builder csv.WriterBuilder, errCh chan<- error) { // check that the partial keys exist in the dataHeader
	defer reader.Close()
	var line = 0
	var failed = true
	var err error

	defer func() {
		if failed {
			if err == nil {
				err = fmt.Errorf("failed at line: %d", line+1)
			} else {
				err = fmt.Errorf("failed at line: %d: %s", line+1, err)
			}
		}
		errCh <- err
	}()

	partialKeys := p.partialKeys
	additionalKey := p.additionalKey

	line = 1

	// create a stream from the header
	dataHeader := reader.Header()

	i, a, _ := utils.Intersect(partialKeys, dataHeader)
	if len(a) > 0 {
		err = fmt.Errorf("%s does not exist in the data header", csv.Format(a))
		return
	}

	i, a, _ = utils.Intersect([]string{additionalKey}, dataHeader)
	if len(i) != 0 {
		err = fmt.Errorf("%s already exists in data header", i[0])
		return
	}

	augmentedHeader := make([]string, len(dataHeader)+1)
	copy(augmentedHeader, dataHeader)
	augmentedHeader[len(dataHeader)] = additionalKey

	keys := make(map[string]int)

	writer := builder(augmentedHeader)
	defer writer.Close(err)

	for data := range reader.C() {
		line++
		augmentedData := writer.Blank()
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
		augmentedData.PutAll(data)
		if additionalKeyValue > 0 {
			augmentedData.Put(additionalKey, fmt.Sprintf("%d", additionalKeyValue))
		}
		writer.Write(augmentedData)
	}
	failed = false
	err = reader.Error()
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
