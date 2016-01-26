package csv

import (
	"github.com/wildducktheories/go-csv/utils"
)

// Given a header-prefixed input stream of CSV records select the fields that match the specified key (Key).
// If PermuteOnly is is specified, all the fields of the input stream are preserved, but the output stream
// is permuted so that the key fields occupy the left-most fields of the output stream. The remaining fields
// are preserved in their original order.
type SelectProcess struct {
	Keys        []string
	PermuteOnly bool
}

func (p *SelectProcess) Run(reader Reader, builder WriterBuilder, errCh chan<- error) {
	defer reader.Close()
	var err error

	keys := p.Keys
	permuteOnly := p.PermuteOnly

	// get the data header
	dataHeader := reader.Header()

	_, _, b := utils.Intersect(keys, dataHeader)
	if len(b) > 0 && permuteOnly {
		extend := make([]string, len(keys)+len(b))
		copy(extend, keys)
		copy(extend[len(keys):], b)
		keys = extend
	}

	// create a new output stream
	writer := builder(keys)
	defer writer.Close(err)
	for data := range reader.C() {
		outputData := writer.Blank()
		outputData.PutAll(data)
		if err := writer.Write(outputData); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- reader.Error()
}
