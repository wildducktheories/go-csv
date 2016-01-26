package csv

// A Process is a function that can be run asynchronously that consumes a stream of CSV
// records provided by reader and writes them into a writer of CSV records as
// constructed by the specified builder. It signals its successful completion by
// writing a nil into the specified error channel. An unsuccessful completion
// is signaled by writing at most one error into the specified error channel.
type Process interface {
	Run(reader Reader, builder WriterBuilder, errCh chan<- error)
}
