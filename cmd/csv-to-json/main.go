package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
)

func configure(args []string) (*csv.CsvToJsonProcess, error) {
	var baseObject string
	var stringsOnly bool
	flags := flag.NewFlagSet("csv-to-json", flag.ExitOnError)

	flags.BoolVar(&stringsOnly, "strings", false, "Don't attempt to convert strings to other JSON types.")
	flags.StringVar(&baseObject, "base-object-key", "", "Write the other columns into the base JSON object found in the specified column.")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	return &csv.CsvToJsonProcess{
		BaseObject:  baseObject,
		StringsOnly: stringsOnly,
	}, nil
}

func main() {
	var p *csv.CsvToJsonProcess
	var err error

	if p, err = configure(os.Args[1:]); err == nil {
		errCh := make(chan error, 1)
		p.Run(csv.WithIoReader(os.Stdin), json.NewEncoder(os.Stdout), errCh)
		err = <-errCh
	}
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
