package csv

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"os"
	"strconv"
	"time"
)

// InfluxLineFormatProcess is a process which converts a CSV file into
// influx line format.
type OpenTSDBImportFormatProcess struct {
	Measurement string   // the name of the measurement
	Timestamp   string   // the name of the timestamp column
	Format      string   // the format of the timestamp column (for format see documentation of go time.Parse())
	Location    string   // the location in which the timestamp is interpreted (per go time.LoadLocation())
	Tags        []string // the columns to be used as tags
	Values      []string // the columns to be used as values.
}

// Run exhausts the reader, writing one record in influx line format per CSV input record.
func (p *OpenTSDBImportFormatProcess) Run(reader Reader, out io.Writer, errCh chan<- error) {
	errCh <- func() (err error) {
		defer reader.Close()

		sort.Strings(p.Tags)
		sort.Strings(p.Values)
		// see: http://stackoverflow.com/questions/13340717/json-numbers-regular-expression
		numberMatcher := regexp.MustCompile("^ *-?(?:0|[1-9]\\d*)(?:\\.\\d+)?(?:[eE][+-]?\\d+)? *$")

		if location, err := time.LoadLocation(p.Location); err != nil {
			return err
		} else {

			if len(p.Values) > 1 {
				return fmt.Errorf("opentsdb does not support multiple values")
			}

			count := 1
			for data := range reader.C() {
				count++

				stringTs := data.Get(p.Timestamp)

				parse := func(s string) (time.Time, error) {
					if p.Format == "ns" {
						if ns, err := strconv.ParseInt(s, 10, 64); err != nil {
							return time.Unix(0, 0), err
						} else {
							return time.Unix(0, ns), nil
						}
					} else {
						return time.ParseInLocation(p.Format, stringTs, location)
					}
				}				

				if ts, err := parse(stringTs); err != nil {
					fmt.Fprintf(os.Stderr, "error parsing timestamp: %v\n", err)
					continue
				} else {

					// <metric> <timestamp> <value> <tagk=tagv> [<tagkN=tagvN>]

					buffer := make([]byte, 0)
					buffer = append(buffer, p.Measurement...)

					a := ts.UnixNano()
					b := int64(0)

					if (a / int64(time.Second))*int64(time.Second) == a {
						b = int64(a/int64(time.Second))
					} else {
						b = a/int64(time.Millisecond)
					}

					buffer = append(buffer, " "...)
					buffer = append(buffer, strconv.FormatInt(b, 10)...)
					buffer = append(buffer, " "...)

					for _, f := range p.Values {
						v := data.Get(f)
						if v == "" {
							continue
						}

						if numberMatcher.MatchString(v) || v == "true" || v == "false" {
							buffer = append(buffer, v...)
						} else {
							buffer = append(buffer, strconv.Quote(v)...)
						}
					}

					tagCount := 0
					for _, t := range p.Tags {
						v := data.Get(t)
						if v == "" {
							continue
						}
						if tagCount > 0 {
							buffer = append(buffer, ","...)
						} else {
							buffer = append(buffer, " "...)
						}
						tagCount++
						buffer = append(buffer, t...)
						buffer = append(buffer, "="...)
						buffer = append(buffer, escapeTag([]byte(v))...)
					}

					buffer = append(buffer, "\n"...)


					if _, err := out.Write(buffer); err != nil {
						return err
					}
				}

			}
			return reader.Error()
		}
	}()
}
