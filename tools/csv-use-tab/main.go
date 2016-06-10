package main

import (
	encoding "encoding/csv"
	"flag"
	"fmt"
	"os"

	"github.com/wildducktheories/go-csv"
)

const (
	TAB = rune(0x09)
)

func configure(args []string) (*csv.UseTabProcess, error) {
	flags := flag.NewFlagSet("csv-use-tab", flag.ExitOnError)

	var onRead bool

	flags.BoolVar(&onRead, "on-read", false, "Whether to use tab on read. Defaults to false.")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	return &csv.UseTabProcess{
		OnRead: onRead,
	}, nil

}

func main() {
	var p *csv.UseTabProcess
	var err error
	var errCh = make(chan error, 1)

	if p, err = configure(os.Args[1:]); err == nil {
		var csvReader csv.Reader
		var csvWriter csv.WriterBuilder
		if p.OnRead {
			tmp := encoding.NewReader(os.Stdin)
			tmp.Comma = TAB
			csvReader = csv.WithCsvReader(tmp, os.Stdin)
			csvWriter = csv.WithIoWriter(os.Stdout)
		} else {
			tmp := encoding.NewWriter(os.Stdout)
			tmp.Comma = TAB
			csvWriter = csv.WithCsvWriter(tmp, os.Stdout)
			csvReader = csv.WithIoReader(os.Stdin)
		}
		p.Run(csvReader, csvWriter, errCh)
		err = <-errCh
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
