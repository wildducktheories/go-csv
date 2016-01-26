package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"
)

func configure(args []string) (*csv.SortProcess, error) {
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

	return (&csv.SortKeys{Keys: keys, Numeric: numeric}).AsSortProcess(), nil
}

func main() {
	var p *csv.SortProcess
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
