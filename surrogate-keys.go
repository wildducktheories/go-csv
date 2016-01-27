package csv

import (
	"github.com/wildducktheories/go-csv/utils"

	"crypto/md5"
	"fmt"
)

// Given a header-prefixed input stream of CSV records and the specification of a natural key (NaturalKeys)
// generate an augmented, header-prefixed, output stream of CSV records which contains a surrogate
// key (SurrogateKey) that is derived from the MD5 sum of the natural key.
//
// The surrogate key is constructed by calculating the MD5 hash of the string representation of a CSV record that contains the fields of the natural key
// of each record.
//
// For example, given the following input CSV stream which has a natural key of Date,Amount,Sequence
//    	Date,Amount,Description,Sequence
//    	2014/12/31,100.0,Payment
//    	2014/12/31,100.0,Payment,1
//    	2014/12/31,85.0,Payment
//
// generate an additional column, KeyMD5, containing a surrogate key that represents the natural key.
//
// 		Date,Amount,Description,Sequence,KeyMD5
// 		2014/12/31,100.0,Payment,"",bead7c34cf0828efb8a240e262e7afea
// 		2014/12/31,100.0,Payment,1,cc8ab528163236eb1aa4004202ee1935
// 		2014/12/31,85.0,Payment,"",8f4d3a8a05031256a4fa4cf1fadd757b
//
type SurrogateKeysProcess struct {
	NaturalKeys  []string
	SurrogateKey string
}

func (p *SurrogateKeysProcess) Run(reader Reader, builder WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer reader.Close()

		naturalKeys := p.NaturalKeys
		surrogateKey := p.SurrogateKey

		// create a stream from the header
		dataHeader := reader.Header()

		i, a, _ := utils.Intersect(naturalKeys, dataHeader)
		if len(a) > 0 {
			return fmt.Errorf("%s does not exist in the data header", Format(a))
		}

		i, a, _ = utils.Intersect([]string{surrogateKey}, dataHeader)
		if len(i) != 0 {
			return fmt.Errorf("%s already exists in data header", i[0])
		}

		// create a new output stream
		augmentedHeader := make([]string, len(dataHeader)+1)
		copy(augmentedHeader, dataHeader)
		augmentedHeader[len(dataHeader)] = surrogateKey

		writer := builder(augmentedHeader)
		defer writer.Close(err)
		for data := range reader.C() {
			augmentedData := writer.Blank()
			key := make([]string, len(naturalKeys))
			for i, h := range naturalKeys {
				key[i] = data.Get(h)
			}
			formattedKey := Format(key)

			hash := fmt.Sprintf("%x", md5.Sum([]byte(formattedKey)))
			augmentedData.PutAll(data)
			augmentedData.Put(surrogateKey, hash)
			if err := writer.Write(augmentedData); err != nil {
				return err
			}
		}
		return reader.Error()
	}()
}
