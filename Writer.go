package csv

import (
	encoding "encoding/csv"
	"io"
)

type Writer interface {
	Header() []string     // Answer the header of the stream.
	Blank() Record        // Provide a blank record compatible with the stream.
	Write(r Record) error // Write a single record into the underying stream.
	Flush() error         // Perform a flush into the underlying stream.
}

type writer struct {
	header  []string
	builder RecordBuilder
	encoder *encoding.Writer
}

// Answer a Writer for the CSV stream constrained by specified header, using the specified encoding writer
func WithCsvWriter(header []string, w *encoding.Writer) (Writer, error) {
	result := &writer{
		header:  header,
		builder: NewRecordBuilder(header),
		encoder: w,
	}
	result.encoder.Write(header)
	return result, result.encoder.Error()
}

// Answer a Writer for the CSV stream constrained by the specified header, using the specified io writer.
func WithIoWriter(header []string, w io.Writer) (Writer, error) {
	return WithCsvWriter(header, encoding.NewWriter(w))
}

// Answer the header that constrains the output stream
func (w *writer) Header() []string {
	return w.header
}

// Answer a blank record for the output stream
func (w *writer) Blank() Record {
	return w.builder(make([]string, len(w.header), len(w.header)))
}

// Write a record into the underlying stream.
func (w *writer) Write(r Record) error {
	h := r.Header()
	var d []string
	if len(h) > 0 && len(w.header) == len(h) && &h[0] == &w.header[0] {
		// optimisation to avoid copying or iterating over slice in default case
		d = r.AsSlice()
	} else {
		// fallback in case where the stream and the record have a different header
		d := make([]string, len(w.header), len(w.header))
		for i, k := range w.header {
			d[i] = r.Get(k)
		}
	}
	return w.encoder.Write(d)
}

// Flush into the underlying stream.
func (w *writer) Flush() error {
	w.encoder.Flush()
	return w.encoder.Error()
}
