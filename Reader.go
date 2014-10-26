package csv

import (
	"encoding/csv"
	"io"
	"os"
)

// Reader provides a reader of CSV streams whose first record is a header describing each field. Fields of each data
// record are keyed by the value of the corresponding field in the header record.
type Reader interface {
	// Answers the header.
	Header() []string
	// Reads the next record from the stream.
	Read() (Record, error)
}

type reader struct {
	builder func([]string) Record
	header  []string
	csv     *csv.Reader
}

// ReadAll reads all the records from the specified reader and only returns a non-nil error
// if an error, other than EOF, occurs during the reading process.
func ReadAll(reader Reader) ([]Record, error) {
	all := make([]Record, 0, 1)
	for {
		if record, err := reader.Read(); err == nil {
			if len(all) == cap(all) {
				extension := make([]Record, len(all), cap(all)*2)
				copy(extension, all)
				all = extension
			}
			all = all[0 : len(all)+1]
			all[len(all)-1] = record
		} else {
			if err.Error() == "EOF" {
				err = nil
			}
			return all, err
		}
	}
}

// WithIoReader creates a csv Reader from the specified io Reader.
func WithIoReader(io io.Reader) (Reader, error) {
	csvReader := csv.NewReader(io)
	csvReader.FieldsPerRecord = -1
	return WithCsvReader(csvReader)
}

// WithCsvReader creates a csv reader from the specified encoding/csv Reader.
func WithCsvReader(io *csv.Reader) (Reader, error) {
	header, err := io.Read()
	if err == nil {
		return &reader{
			builder: NewRecordBuilder(header),
			header:  header,
			csv:     io,
		}, nil
	}
	return nil, err
}

// OpenForRead opens the specified file and calls NewReader on the resulting File.
func OpenForRead(name string) (Reader, error) {
	file, err := os.Open(name)
	if err == nil {
		reader, err := WithIoReader(file)
		return reader, err
	}
	return nil, err
}

func (reader *reader) Header() []string {
	return reader.header
}

func (reader *reader) Read() (Record, error) {
	fields, err := reader.csv.Read()
	if err == nil {
		return reader.builder(fields), nil
	}
	return nil, err
}
