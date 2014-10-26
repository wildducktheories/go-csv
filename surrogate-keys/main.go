// Given a header-prefixed input stream of CSV records and the specification of a natural key (--natural-key) generate an augmented, header-prefixed, output stream of CSV
// records which contains a surrogate key (--surrogate-key) that is derived from the MD5 sum of the natural key.
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
// 		2014/12/31,100.0,Payment,"",d97e17fc4b32aa405da3598c82be4052
// 		2014/12/31,100.0,Payment,1,6c21a286811d8f7c90c19a68de4091c4
// 		2014/12/31,85.0,Payment,"",c2f3e05f25610fb52c8e543ea95393c0
//
package main

import (
	rawCsv "encoding/csv"
	"github.com/wildducktheories/go-csv"

	"crypto/md5"
	"flag"
	"fmt"
	"os"
	"strings"
)

func body() error {
	var naturalKey, surrogateKey string

	flag.StringVar(&naturalKey, "natural-key", "", "The columns of the natural key")
	flag.StringVar(&surrogateKey, "surrogate-key", "", "The column name for the surrogate key.")
	flag.Parse()

	usage := func() {
		fmt.Printf("usage: surrogate-keys {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	naturalKeys, err := csv.Parse(naturalKey)
	if err != nil || len(naturalKey) < 1 {
		usage()
		return fmt.Errorf("--natural-key must specify one or more columns")
	}

	if surrogateKey == "" {
		usage()
		return fmt.Errorf("--surrogate-key must specify the name of a new column")
	}

	// check that the partial keys exist in the dataHeader
	reader, err := csv.WithIoReader(os.Stdin)
	if err != nil {
		return fmt.Errorf("cannot parse header from input stream: %v", err)
	}

	// create a stream from the header
	dataHeader := reader.Header()
	formattedDataHeader := csv.Format(dataHeader)
	headerStream, err := csv.WithIoReader(strings.NewReader(formattedDataHeader + "\n" + formattedDataHeader))
	if err != nil {
		return fmt.Errorf("failed to reparse header: %v", err)
	}
	headerRec, err := headerStream.Read()

	// check that every key in the partial-key is also in the data header
	for _, h := range naturalKeys {
		if err != nil {
			return err
		}
		if headerRec.Get(h) != h {
			return fmt.Errorf("'%s' is not a field of the input stream", h)
		}
	}

	if headerRec.Get(surrogateKey) != "" {
		return fmt.Errorf("'%s' already exists in the header", surrogateKey)
	}

	// create a new output stream
	augmentedHeader := make([]string, len(dataHeader)+1)
	copy(augmentedHeader, dataHeader)
	augmentedHeader[len(dataHeader)] = surrogateKey

	writer := rawCsv.NewWriter(os.Stdout)
	writer.Write(augmentedHeader)
	for {
		data, err := reader.Read()
		augmentedData := make([]string, len(dataHeader)+1)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		key := make([]string, len(naturalKeys))
		for i, h := range naturalKeys {
			key[i] = data.Get(h)
		}
		formattedKey := csv.Format(key)

		hash := fmt.Sprintf("%x", md5.Sum([]byte(formattedKey)))
		copy(augmentedData, data.AsSlice())
		augmentedData[len(dataHeader)] = hash
		writer.Write(augmentedData)
	}
	writer.Flush()

	return nil
}

func main() {
	err := body()
	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
