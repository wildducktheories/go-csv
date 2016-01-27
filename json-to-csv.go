package csv

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Given a stream of JSON records, generate a stream of csv records.
//
// The columns of the output CSV stream are named by the Header parameter. Each column is interprefed
// as a path into the JSON input object. If the object at that path is a string, the
// string is copied into the specified column of the output stream. Otherwise, the json encoding
// of the object is copied into the specified column of the output stream.
//
// Each object in the input object which is mapped by a CSV column is logically deleted from
// the input object. If --base-object-key is specified, a JSON encoding of the remaining input object
// is written into the specified column of the CSV output stream.
//
type JsonToCsvProcess struct {
	BaseObject string
	Header     []string
}

func (proc *JsonToCsvProcess) readMap(m map[string]interface{}, p []string) interface{} {
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
				return proc.readMap(mo, p[1:])
			}
		}
	}
	return ""
}

func (p *JsonToCsvProcess) asString(v interface{}) (string, error) {
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

func (p *JsonToCsvProcess) Run(decoder *json.Decoder, builder WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		header := p.Header
		baseObject := p.BaseObject

		line := 0
		paths := map[string][]string{}
		for _, k := range header {
			paths[k] = strings.Split(k, ".")
		}
		if baseObject != "" {
			if _, ok := paths[baseObject]; !ok {
				header = append(header, baseObject)
			}
		}

		// open the decoder
		encoder := builder(header)
		defer encoder.Close(err)

		for {
			line++
			m := map[string]interface{}{}
			if err := decoder.Decode(&m); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("warning: %d: unable to decode object: %s\n", line, err)
			}
			r := encoder.Blank()
			for _, k := range header {
				v := p.readMap(m, paths[k])
				if s, err := p.asString(v); err != nil {
					return fmt.Errorf("warning: %d: unable to encode object: %s\n", line, err)
				} else {
					r.Put(k, s)
				}
			}

			if baseObject != "" {
				if b, err := json.Marshal(m); err != nil {
					return fmt.Errorf("warning: %d: unable to marshal object: %s\n", line, err)
				} else {
					r.Put(baseObject, string(b))
				}
			}

			if err := encoder.Write(r); err != nil {
				return fmt.Errorf("fatal: %d: failed to write: %s", line, err)
			}
		}
		return nil
	}()
}
