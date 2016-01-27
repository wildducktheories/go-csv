package main

import (
	"github.com/wildducktheories/go-csv"

	"flag"
	"fmt"
	"os"
)

func configure(args []string) (*csv.SurrogateKeysProcess, error) {
	var naturalKey, surrogateKey string
	var err error

	flags := flag.NewFlagSet("surrogate-keys", flag.ContinueOnError)

	flags.StringVar(&naturalKey, "natural-key", "", "The fields of the natural key")
	flags.StringVar(&surrogateKey, "surrogate-key", "", "The field name for the surrogate key.")

	if err = flags.Parse(args); err != nil {
		return nil, err
	}

	usage := func() {
		fmt.Printf("usage: surrogate-keys {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	naturalKeys, err := csv.Parse(naturalKey)
	if err != nil || len(naturalKey) < 1 {
		usage()
		return nil, fmt.Errorf("--natural-key must specify one or more columns")
	}

	if surrogateKey == "" {
		usage()
		return nil, fmt.Errorf("--surrogate-key must specify the name of a new column")
	}

	return &csv.SurrogateKeysProcess{
		NaturalKeys:  naturalKeys,
		SurrogateKey: surrogateKey,
	}, nil
}

func main() {
	var p *csv.SurrogateKeysProcess
	var err error

	errCh := make(chan error, 1)
	if p, err = configure(os.Args[1:]); err == nil {
		p.Run(csv.WithIoReader(os.Stdin), csv.WithIoWriter(os.Stdout), errCh)
		err = <-errCh
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
