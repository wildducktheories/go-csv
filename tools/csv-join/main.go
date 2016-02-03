package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"
)

func configure(args []string) (*csv.Join, []string, error) {
	flags := flag.NewFlagSet("csv-join", flag.ExitOnError)
	var joinKey string
	var numericKey string

	flags.StringVar(&joinKey, "join-key", "", "The columns of the join key")
	flags.StringVar(&numericKey, "numeric", "", "The specified columns are treated as numeric strings.")
	if err := flags.Parse(args); err != nil {
		return nil, nil, err
	}

	usage := func() {
		fmt.Printf("usage: csv-join {options}\n")
		flags.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	joinKeys, err := csv.Parse(joinKey)
	if err != nil || len(joinKeys) < 1 {
		usage()
		return nil, nil, fmt.Errorf("--join-key must specify one or more columns.")
	}
	leftKeys := make([]string, len(joinKeys))
	rightKeys := make([]string, len(joinKeys))
	for i, k := range joinKeys {
		split := strings.Split(k, "=")
		if len(split) == 1 {
			split = append(split, split[0])
		}
		if len(split) != 2 {
			return nil, nil, fmt.Errorf("each join key must be of the form left=right")
		}
		leftKeys[i] = split[0]
		rightKeys[i] = split[1]
	}

	numeric, err := csv.Parse(numericKey)
	if err != nil && len(numericKey) > 0 {
		usage()
		return nil, nil, fmt.Errorf("--numeric must specify the list of numeric keys.")
	}

	if i, _, _ := utils.Intersect(joinKeys, numeric); len(i) < len(numeric) {
		return nil, nil, fmt.Errorf("--numeric must be a strict subset of left hand side --join-key")
	}

	fn := flags.Args()
	if len(fn) < 2 {
		return nil, nil, fmt.Errorf("expected at least 2 file arguments, found %d", len(fn))
	}

	return &csv.Join{
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		Numeric:   numeric,
	}, fn, nil
}

func openReader(n string) (csv.Reader, error) {
	if n == "-" {
		return csv.WithIoReader(os.Stdin), nil
	} else {
		if f, err := os.Open(n); err != nil {
			return nil, err
		} else {
			return csv.WithIoReader(f), nil
		}
	}
}

func main() {
	var j *csv.Join
	var err error
	var fn []string

	err = func() error {
		if j, fn, err = configure(os.Args[1:]); err == nil {
			readers := make([]csv.Reader, len(fn))
			for i, n := range fn {
				if readers[i], err = openReader(n); err != nil {
					return err
				}
			}
			procs := make([]csv.Process, len(readers)-1)
			for i, _ := range procs {
				procs[i] = j.WithRight(readers[i+1])
			}
			pipeline := csv.NewPipeLine(procs)
			var errCh = make(chan error, 1)
			pipeline.Run(readers[0], csv.WithIoWriter(os.Stdout), errCh)
			return <-errCh
		} else {
			return err
		}
	}()

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
