package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
)

func configure(args []string) (*csv.SelectProcess, error) {
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

	return &csv.SelectProcess{
		Keys:        keys,
		PermuteOnly: permuteOnly,
	}, nil

}

func main() {
	var p *csv.SelectProcess
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
