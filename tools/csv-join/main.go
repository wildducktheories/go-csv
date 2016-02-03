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

			// construct a sort process for the left most file

			leftSortProcess := (&csv.SortKeys{
				Numeric: j.Numeric,
				Keys:    j.LeftKeys,
			}).AsSortProcess()

			// map the numeric key to the keyspace of the rightmost files

			l2r := map[string]string{}
			for i, k := range j.LeftKeys {
				l2r[k] = j.RightKeys[i]
			}

			rightNumeric := make([]string, len(j.Numeric))
			for i, k := range j.Numeric {
				rightNumeric[i] = l2r[k]
			}

			// create a sort process for the right most files.

			rightSortProcess := (&csv.SortKeys{
				Numeric: rightNumeric,
				Keys:    j.RightKeys,
			}).AsSortProcess()

			// open one reader for each file
			readers := make([]csv.Reader, len(fn))
			for i, n := range fn {
				if readers[i], err = openReader(n); err != nil {
					return err
				}
			}

			// create one join process for each of the last n-1 readers
			procs := make([]csv.Process, len(readers)-1)
			for i, _ := range procs {
				procs[i] = j.WithRight(csv.WithProcess(readers[i+1], rightSortProcess))
			}

			// create a pipeline from the n-1 join processes
			pipeline := csv.NewPipeLine(procs)

			// run the join pipeline with the first reader
			var errCh = make(chan error, 1)
			pipeline.Run(csv.WithProcess(readers[0], leftSortProcess), csv.WithIoWriter(os.Stdout), errCh)
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
