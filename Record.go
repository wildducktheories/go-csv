package csv

import (
	"github.com/wildducktheories/go-csv/utils"
)

//Record provides keyed access to the fields of data records where each field
//of a data record is keyed by the value of the corresponding field in the header record.
type Record interface {
	// Return the header of the record.
	Header() []string
	// Gets the value of the field specified by the key. Returns the empty string
	// if the field does not exist in the record.
	Get(key string) string
	// Puts the value into the field specified by the key.
	Put(key string, value string)
	// Return the contents of the record as a map.
	AsMap() map[string]string
	// Return the contents of the record as a slice.
	AsSlice() []string
}

type record struct {
	header []string
	index  map[string]int
	fields []string
}

// NewRecordBuilder returns a function that can be used to create new Records
// for a CSV stream with the specified header.
//
// This can be used with raw encoding/csv streams in cases where a CSV stream contains
// more than one record type.
func NewRecordBuilder(header []string) func(fields []string) Record {
	index := utils.Index(header)
	return func(fields []string) Record {
		tmp := make([]string, len(fields), len(header))
		copy(tmp, fields)
		return &record{
			header: header,
			index:  index,
			fields: tmp,
		}
	}
}

func (r *record) Header() []string {
	return r.header
}

// Answer the value of the field indexed by the column containing the specified header value.
func (r *record) Get(key string) string {
	x, ok := r.index[key]
	if ok && x < len(r.fields) {
		return r.fields[x]
	}
	return ""
}

// Puts the specified value into the record at the index determined by the key value.
func (r *record) Put(key string, value string) {
	x, ok := r.index[key]
	if ok && x < cap(r.fields) {
		if x > len(r.fields) {
			r.fields = r.fields[0:x]
		}
		r.fields[x] = value
	}
}

// Return a map containing the contents of the record.
func (r *record) AsMap() map[string]string {
	result := make(map[string]string)
	for i, h := range r.header {
		if i < len(r.fields) {
			result[h] = r.fields[i]
		} else {
			result[h] = ""
		}
	}
	return result
}

// Return the record values as a slice.
func (r *record) AsSlice() []string {
	result := make([]string, len(r.fields), len(r.header))
	copy(result, r.fields)
	return result
}
