// Given a stream of JSON records, generate a stream of csv records.
//
// The columns of the csv stream are named by the --columns parameter. Each column is interprefed
// as a path into the corresponding input object. If the object at that path is a string, the
// string is copied into the specified column of the output stream. Otherwise, the json encoding
// of the object is copied into the specified column of the output stream.
//
// Each object in the input object which is mapped by a CSV column is logically deleted from
// the input object. If --base-object-key is specified, a JSON encoding of the remaining input object
// is written into the specified column of the CSV output stream.
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/wildducktheories/go-csv"
)

func readMap(m map[string]interface{}, p []string) interface{} {
	if len(p) == 1 {
		v := m[p[0]]
		delete(m, p[0])
		if v == nil {
			return ""
		} else {
			return v
		}
	} else if len(p) > 1 {
		var o interface{}
		var mo map[string]interface{}
		var ok bool
		if o, ok = m[p[0]]; ok {
			if mo, ok = o.(map[string]interface{}); ok {
				return readMap(mo, p[1:])
			}
		}
	}
	return ""
}

func asString(v interface{}) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	} else {
		if b, err := json.Marshal(v); err != nil {
			return "", fmt.Errorf("unable to marshal as string: %v", err)
		} else {
			return string(b), nil
		}
	}
}

func body() error {
	var baseObject string
	var columns string

	flag.StringVar(&baseObject, "base-object-key", "", "The column into which the remainder of each JSON object is read.")
	flag.StringVar(&columns, "columns", "", "The columns of the CSV file")
	flag.Parse()

	if columns == "" {
		return fmt.Errorf("fatal: a --columns parameter must be specified")
	}

	if header, err := csv.Parse(columns); err != nil {
		return fmt.Errorf("fatal: --columns could not be parsed as a CSV record")
	} else {

		paths := map[string][]string{}
		for _, k := range header {
			if _, ok := paths[k]; ok {
				return fmt.Errorf("fatal: --columns contains a duplicate header: %s", k)
			}
			paths[k] = strings.Split(k, ".")
		}

		if baseObject != "" {
			if _, ok := paths[baseObject]; !ok {
				header = append(header, baseObject)
			}
		}

		// open the decoder
		decoder := json.NewDecoder(os.Stdin)

		if encoder, err := csv.WithIoWriter(header, os.Stdout); err != nil {
			return err
		} else {
			defer encoder.Flush()

			line := 0

			for {
				line++
				m := map[string]interface{}{}
				if err := decoder.Decode(&m); err != nil {
					if err == io.EOF {
						break
					}
					fmt.Fprintf(os.Stderr, "warning: %d: unable to decode object: %s\n", line, err)
					return err
				}
				r := encoder.Blank()
				for _, k := range header {
					v := readMap(m, paths[k])
					if s, err := asString(v); err != nil {
						fmt.Fprintf(os.Stderr, "warning: %d: unable to encode object: %s\n", line, err)
						continue
					} else {
						r.Put(k, s)
					}
				}

				if baseObject != "" {
					if b, err := json.Marshal(m); err != nil {
						fmt.Fprintf(os.Stderr, "warning: %d: unable to marshal object: %s\n", line, err)
						continue
					} else {
						r.Put(baseObject, string(b))
					}
				}

				if err := encoder.Write(r); err != nil {
					return fmt.Errorf("fatal: %d: failed to write: %s", line, err)
				}
			}
		}
	}
	return nil
}

func main() {
	err := body()
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
