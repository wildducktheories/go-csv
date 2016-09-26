package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/wildducktheories/go-csv"
)

func configure(args []string) (*csv.InfluxLineFormatProcess, error) {
	var measurement string
	var timestamp string
	var format string
	var location string
	var tags string
	var values string

	flags := flag.NewFlagSet("influx-line-format", flag.ExitOnError)

	flags.StringVar(&measurement, "measurement", "", "The name of the influx measurement.")
	flags.StringVar(&timestamp, "timestamp", "timestamp", "The name of the CSV timestamp field.")
	flags.StringVar(&format, "format", "2006-01-02 15:04:05", "The format of the CSV timestamp field. A go timestamp format or s|ms|ns.")
	flags.StringVar(&location, "location", "UTC", "The location in which the timestamp should be interpreted.")
	flags.StringVar(&tags, "tags", "", "The CSV columns to be used as tags.")
	flags.StringVar(&values, "values", "", "The CSV columns to be used as values.")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if _, err := time.LoadLocation(location); err != nil {
		return nil, err
	}

	if timestamp == "" {
		return nil, errors.New("--timestamp column must be specified")
	}

	if measurement == "" {
		return nil, errors.New("--measurement must be specified")
	}

	if valuesSlice, err := csv.Parse(values); err != nil {
		return nil, errors.New("--values must specify a set of values columns")
	} else if tagsSlice, err := csv.Parse(tags); err != nil {
		return nil, errors.New("--tags must specify a st of tag columns")
	} else {
		if len(valuesSlice) == 0 {
			return nil, errors.New("at least one values column must be specified")
		}
		if len(tagsSlice) == 0 {
			return nil, errors.New("at least one tag column must be specified")
		}
		return &csv.InfluxLineFormatProcess{
			Measurement: measurement,
			Timestamp:   timestamp,
			Format:      format,
			Location:    location,
			Tags:        tagsSlice,
			Values:      valuesSlice,
		}, nil
	}
}

func main() {
	var p *csv.InfluxLineFormatProcess
	var err error

	if p, err = configure(os.Args[1:]); err == nil {
		errCh := make(chan error, 1)
		p.Run(csv.WithIoReader(os.Stdin), os.Stdout, errCh)
		err = <-errCh
	}
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
