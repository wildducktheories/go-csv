package main

import (
	"github.com/wildducktheories/go-csv"

	"flag"
	"fmt"
	"os"
)

func configure(args []string) (*csv.UniquifyProcess, error) {
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

	return &csv.UniquifyProcess{
		PartialKeys:   partialKeys,
		AdditionalKey: additionalKey,
	}, nil
}

func main() {
	var p *csv.UniquifyProcess
	var err error
	var errCh = make(chan error, 1)

	if p, err = configure(os.Args[1:]); err == nil {
		p.Run(csv.WithIoReader(os.Stdin), csv.WithIoWriter(os.Stdout), errCh)
		err = <-errCh
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
