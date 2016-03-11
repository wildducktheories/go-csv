package csv

import (
	"io"
	"sort"
	"strconv"
	"time"
)

// InfluxLineFormatProcess is a process which converts a CSV file into
// influx line format.
type InfluxLineFormatProcess struct {
	Measurement string   // the name of the measurement
	Timestamp   string   // the name of the timestamp column
	Format      string   // the format of the timestamp column (for format see documentation of go time.Parse())
	Location    string   // the location in which the timestamp is interpreted (per go time.LoadLocation())
	Tags        []string // the columns to be used as tags
	Values      []string // the columns to be used as values.
}

// Run exhausts the reader, writing one record in influx line format per CSV input record.
func (p *InfluxLineFormatProcess) Run(reader Reader, out io.Writer, errCh chan<- error) {
	errCh <- func() (err error) {
		defer reader.Close()

		sort.Strings(p.Tags)
		sort.Strings(p.Values)
		// see: http://stackoverflow.com/questions/13340717/json-numbers-regular-expression
		// numberMatcher := regexp.MustCompile("^ *-?(?:0|[1-9]\\d*)(?:\\.\\d+)?(?:[eE][+-]?\\d+)? *$")

		if location, err := time.LoadLocation(p.Location); err != nil {
			return err
		} else {

			maxLen := len(p.Measurement)
			for data := range reader.C() {

				stringTs := data.Get(p.Timestamp)
				if ts, err := time.ParseInLocation(p.Format, stringTs, location); err != nil {
					return err
				} else {

					buffer := make([]byte, 0, maxLen)
					buffer = append(buffer, p.Measurement...)
					for _, t := range p.Tags {
						v := data.Get(t)
						if v == "" {
							continue
						}
						buffer = append(buffer, ","...)
						buffer = append(buffer, t...)
						buffer = append(buffer, "="...)
						buffer = append(buffer, v...)
					}

					buffer = append(buffer, " "...)
					first := true
					appended := 0
					for _, f := range p.Values {
						v := data.Get(f)
						if v == "" {
							continue
						}

						appended++
						if !first {
							buffer = append(buffer, ","...)
						} else {
							first = false
						}
						buffer = append(buffer, f...)
						buffer = append(buffer, "="...)
						buffer = append(buffer, v...)
					}
					if appended == 0 {
						continue
					}

					if len(buffer) > maxLen {
						maxLen = len(buffer)
					}

					buffer = append(buffer, " "...)
					buffer = append(buffer, strconv.FormatInt(ts.UnixNano(), 10)...)
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
