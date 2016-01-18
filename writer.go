package csv

import (
	encoding "encoding/csv"
	"io"
)

type Writer interface {
	Header() []string     // Answer the header of the stream.
	Blank() Record        // Provide a blank record compatible with the stream.
	Write(r Record) error // Write a single record into the underying stream.
	Close(err error) error
}

// A constructor for a writer.
type WriterBuilder func([]string) Writer

type writer struct {
	header  []string
	builder RecordBuilder
	encoder *encoding.Writer
	closer  io.Closer
	err     error
}

// Answer a Writer for the CSV stream constrained by specified header, using the specified encoding writer
func WithCsvWriter(w *encoding.Writer, c io.Closer) WriterBuilder {
	return func(header []string) Writer {
		result := &writer{
			header:  header,
			builder: NewRecordBuilder(header),
			encoder: w,
			closer:  c,
		}
		result.err = result.encoder.Write(header)
		return result
	}
}

// Answer a Writer for the CSV stream constrained by the specified header, using the specified io writer.
func WithIoWriter(w io.WriteCloser) WriterBuilder {
	return WithCsvWriter(encoding.NewWriter(w), w)
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
	if w.err != nil {
		return w.err
	}
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

// Close the stream and propagate an error
func (w *writer) Close(err error) error {
	if w.closer != nil {
		return w.closer.Close()
	} else {
		return nil
	}
}
