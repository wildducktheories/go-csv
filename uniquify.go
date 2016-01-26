package csv

import (
	"fmt"
	"github.com/wildducktheories/go-csv/utils"
)

// Given a header-prefixed input stream of CSV records and the specification of a partial key (PartialKey)
// formed from one or more of the fields, generate an augmented, header-prefixed, stream of CSV records
// such that the augmented key of each output record is unique. The field used to ensure uniqueness is
// specified by the AdditionalKey option.
//
// For example, given the following input with the partial key Date,Amount
//
//    Date,Amount,Description
//    2014/12/31,100.0,Payment
//    2014/12/31,100.0,Payment
//    2014/12/31,85.0,Payment
//
// Generate an additional column, Sequence, such that the augmented key Date,Amount,Sequence is unique
// for all input records.
//
//    Date,Amount,Description,Sequence
//    2014/12/31,100.0,Payment,
//    2014/12/31,100.0,Payment,1
//    2014/12/31,85.0,Payment,
type UniquifyProcess struct {
	PartialKeys   []string
	AdditionalKey string
}

func (p *UniquifyProcess) Run(reader Reader, builder WriterBuilder, errCh chan<- error) { // check that the partial keys exist in the dataHeader
	defer reader.Close()
	var line = 0
	var failed = true
	var err error

	defer func() {
		if failed {
			if err == nil {
				err = fmt.Errorf("failed at line: %d", line+1)
			} else {
				err = fmt.Errorf("failed at line: %d: %s", line+1, err)
			}
		}
		errCh <- err
	}()

	partialKeys := p.PartialKeys
	additionalKey := p.AdditionalKey

	line = 1

	// create a stream from the header
	dataHeader := reader.Header()

	i, a, _ := utils.Intersect(partialKeys, dataHeader)
	if len(a) > 0 {
		err = fmt.Errorf("%s does not exist in the data header", Format(a))
		return
	}

	i, a, _ = utils.Intersect([]string{additionalKey}, dataHeader)
	if len(i) != 0 {
		err = fmt.Errorf("%s already exists in data header", i[0])
		return
	}

	augmentedHeader := make([]string, len(dataHeader)+1)
	copy(augmentedHeader, dataHeader)
	augmentedHeader[len(dataHeader)] = additionalKey

	keys := make(map[string]int)

	writer := builder(augmentedHeader)
	defer writer.Close(err)

	for data := range reader.C() {
		line++
		augmentedData := writer.Blank()
		key := make([]string, len(partialKeys))
		for i, h := range partialKeys {
			key[i] = data.Get(h)
		}
		formattedKey := Format(key)
		additionalKeyValue, ok := keys[formattedKey]
		if !ok {
			additionalKeyValue = 0
		} else {
			additionalKeyValue++
		}
		keys[formattedKey] = additionalKeyValue
		augmentedData.PutAll(data)
		if additionalKeyValue > 0 {
			augmentedData.Put(additionalKey, fmt.Sprintf("%d", additionalKeyValue))
		}
		writer.Write(augmentedData)
	}
	failed = false
	err = reader.Error()
}
