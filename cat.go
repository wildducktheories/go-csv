package csv

// A process that copies the reader to the writer.
type CatProcess struct {
}

func (p *CatProcess) Run(r Reader, b WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		w := b(r.Header())
		defer w.Close(err)
		for rec := range r.C() {
			if e := w.Write(rec); e != nil {
				return e
			}
		}
		return r.Error()
	}()
}
