package csv

// Merely copies records from input to output - delimiting munging
// is done by the tool.
type UseTabProcess struct {
	OnRead bool
}

func (p *UseTabProcess) Run(reader Reader, builder WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer reader.Close()

		// create a new output stream
		writer := builder(reader.Header())
		defer writer.Close(err)

		for data := range reader.C() {
			if err = writer.Write(data); err != nil {
				return err
			}
		}

		return reader.Error()
	}()
}
