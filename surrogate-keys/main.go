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
// 		2014/12/31,100.0,Payment,"",bead7c34cf0828efb8a240e262e7afea
// 		2014/12/31,100.0,Payment,1,cc8ab528163236eb1aa4004202ee1935
// 		2014/12/31,85.0,Payment,"",8f4d3a8a05031256a4fa4cf1fadd757b
//
package main

import (
	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/go-csv/utils"

	"crypto/md5"
	"flag"
	"fmt"
	"os"
)

type process struct {
	naturalKeys  []string
	surrogateKey string
}

func configure(args []string) (*process, error) {
	var naturalKey, surrogateKey string
	var err error

	flags := flag.NewFlagSet("surrogate-keys", flag.ContinueOnError)

	flags.StringVar(&naturalKey, "natural-key", "", "The fields of the natural key")
	flags.StringVar(&surrogateKey, "surrogate-key", "", "The field name for the surrogate key.")

	if err = flags.Parse(args); err != nil {
		return nil, err
	}

	usage := func() {
		fmt.Printf("usage: surrogate-keys {options}\n")
		flag.PrintDefaults()
	}

	// Use  a CSV parser to extract the partial keys from the parameter
	naturalKeys, err := csv.Parse(naturalKey)
	if err != nil || len(naturalKey) < 1 {
		usage()
		return nil, fmt.Errorf("--natural-key must specify one or more columns")
	}

	if surrogateKey == "" {
		usage()
		return nil, fmt.Errorf("--surrogate-key must specify the name of a new column")
	}

	return &process{
		naturalKeys:  naturalKeys,
		surrogateKey: surrogateKey,
	}, nil
}

func (p *process) run(reader csv.Reader, builder csv.WriterBuilder, errCh chan<- error) {
	defer reader.Close()

	var err error

	naturalKeys := p.naturalKeys
	surrogateKey := p.surrogateKey

	// create a stream from the header
	dataHeader := reader.Header()

	i, a, _ := utils.Intersect(naturalKeys, dataHeader)
	if len(a) > 0 {
		errCh <- fmt.Errorf("%s does not exist in the data header", csv.Format(a))
	}

	i, a, _ = utils.Intersect([]string{surrogateKey}, dataHeader)
	if len(i) != 0 {
		errCh <- fmt.Errorf("%s already exists in data header", i[0])
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
		formattedKey := csv.Format(key)

		hash := fmt.Sprintf("%x", md5.Sum([]byte(formattedKey)))
		augmentedData.PutAll(data)
		augmentedData.Put(surrogateKey, hash)
		if err := writer.Write(augmentedData); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- reader.Error()
	return
}

func main() {
	var p *process
	var err error

	errCh := make(chan error, 1)
	if p, err = configure(os.Args[1:]); err == nil {
		p.run(csv.WithIoReader(os.Stdin), csv.WithIoWriter(os.Stdout), errCh)
		err = <-errCh
	}

	if err != nil {
		fmt.Printf("fatal: %v\n", err)
		os.Exit(1)
	}
}
