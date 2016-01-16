// Given a stream of CSV records, generate a stream of JSON records, one per line. The headers
// are treated as paths into the resulting JSON object, so a CSV file containing the header
// foo.bar,baz and the data 1, 2 will be converted into a JSON object like {"foo": {"bar": 1}, "baz": 2}
//
// If column values can be successfully unmarshalled as JSON numbers, booleans, objects or arrays then
// the value will be encoded as the corresponding JSON object, otherwise it will be encoded as a string.
// Use --strings to force all column values to be encoded as JSON strings.
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/wildducktheories/go-csv"
)

func writeToMap(m map[string]interface{}, p []string, v interface{}) {
	if len(p) == 1 {
		m[p[0]] = v
	} else if len(p) > 1 {
		var o interface{}
		var mo map[string]interface{}
		var ok bool
		if o, ok = m[p[0]]; !ok {
			mo = map[string]interface{}{}
		} else {
			if mo, ok = o.(map[string]interface{}); !ok {
				mo = map[string]interface{}{}
			}
		}
		m[p[0]] = mo
		writeToMap(mo, p[1:], v)
	}

}

func body() error {
	var baseObject string
	var stringsOnly bool

	flag.BoolVar(&stringsOnly, "strings", false, "Don't attempt to convert strings to other JSON types.")
	flag.StringVar(&baseObject, "base-object-key", "", "Write the other columns into the base JSON object found in the specified column.")
	flag.Parse()

	// open the reader
	reader, err := csv.WithIoReader(os.Stdin)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot parse header from input stream: %v", err)
	}

	encoder := json.NewEncoder(os.Stdout)

	paths := map[string][]string{}
	for _, k := range reader.Header() {
		paths[k] = strings.Split(k, ".")
	}

	if baseObject != "" {
		if _, ok := paths[baseObject]; !ok {
			return fmt.Errorf("fatal: '%s' is not a valid key in the input stream", baseObject)
		}
	}

	// see: http://stackoverflow.com/questions/13340717/json-numbers-regular-expression
	numberMatcher := regexp.MustCompile("^ *-?(?:0|[1-9]\\d*)(?:\\.\\d+)?(?:[eE][+-]?\\d+)? *$")

	for {
		data, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		dataMap := data.AsMap()
		objectMap := map[string]interface{}{}

		if baseObject != "" {
			if base, ok := dataMap[baseObject]; ok {
				if err := json.Unmarshal([]byte(base), &objectMap); err != nil {
					fmt.Fprintf(os.Stderr, "warning: failed to parse base object: %s: %s\n", base, err)
				}
			}
		}

		for k, v := range dataMap {
			var f float64
			var ov interface{}

			ov = v

			if baseObject != "" && k == baseObject {
				continue
			} else if v == "" {
				continue
			} else if stringsOnly {
				ov = v
			} else if v == "null" {
				continue
			} else if v == "true" || v == "TRUE" {
				ov = true
			} else if v == "false" || v == "FALSE" {
				ov = false
			} else if v[0] == '{' {
				j := map[string]interface{}{}
				if err := json.Unmarshal([]byte(v), &j); err == nil {
					fmt.Fprintf(os.Stderr, "v, j = %s, %+v\n", v, j)
					ov = j
				}
			} else if v[0] == '[' {
				aj := make([]interface{}, 0)
				if err := json.Unmarshal([]byte(v), &aj); err == nil {
					ov = aj
				}
			} else if numberMatcher.MatchString(v) {
				if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
					ov = f
				}
			}
			writeToMap(objectMap, paths[k], ov)
		}
		encoder.Encode(objectMap)
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
