// Given a header-prefixed input stream of CSV records and the specification of a natural key (--natural-key) generate an augmented, header-prefixed, output stream of CSV
// records which contains a surrogate key (--surrogate-key) that is derived from the MD5 sum of the natural key.
//
// The surrogate key is constructed by calculating the MD5 hash of the string representation of a CSV record that contains the fields of the natural key
// of each record.
//
// For example, given the following input CSV stream which has a natural key of Date,Amount,Sequence
//    	Date,Amount,Description,Sequence
//    	2014/12/31,100.0,Payment
//    	2014/12/31,100.0,Payment,1
//    	2014/12/31,85.0,Payment
//
// generate an additional column, KeyMD5, containing a surrogate key that represents the natural key.
//
// 		Date,Amount,Description,Sequence,KeyMD5
// 		2014/12/31,100.0,Payment,"",bead7c34cf0828efb8a240e262e7afea
// 		2014/12/31,100.0,Payment,1,cc8ab528163236eb1aa4004202ee1935
// 		2014/12/31,85.0,Payment,"",8f4d3a8a05031256a4fa4cf1fadd757b
//
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
