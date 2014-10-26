package csv

import (
	"bytes"
	"encoding/csv"
	"strings"
)

//Parse a string representing one or more encoded CSV record and returns the first such record.
func Parse(record string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(record))
	result, err := reader.Read()
	if err != nil {
		return nil, err
	}
	return result, nil
}

//Format the specified slice as a CSV record using the default CSV encoding conventions.
func Format(record []string) string {
	buffer := bytes.NewBufferString("")
	writer := csv.NewWriter(buffer)
	writer.Write(record)
	writer.Flush()
	return buffer.String()
}
