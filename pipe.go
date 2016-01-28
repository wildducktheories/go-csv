package csv

// Implements a unidirectional channel that can connect a reader process to a writer process.
type Pipe interface {
	Builder() WriterBuilder // Builds a Writer for the write end of the pipe
	Reader() Reader         // Returns the Reader for the read end of the pipe
}

type pipe struct {
	header []string
	ch     chan Record
	init   chan interface{}
	err    error
}

type pipeWriter struct {
	pipe    *pipe
	builder RecordBuilder
}

// Answer a new Pipe whose Builder and Reader can be used to connect two chained
// processes.
func NewPipe() Pipe {
	return &pipe{
		ch:   make(chan Record),
		err:  nil,
		init: make(chan interface{}),
	}
}

func (p *pipe) Reader() Reader {
	return p
}

func (p *pipe) C() <-chan Record {
	<-p.init
	return p.ch
}

func (p *pipe) Close() {
}

func (p *pipe) Header() []string {
	<-p.init
	return p.header
}

func (p *pipe) Error() error {
	<-p.init
	return p.err
}

func (p *pipe) Builder() WriterBuilder {
	return func(header []string) Writer {
		p.header = header
		close(p.init)
		return &pipeWriter{pipe: p, builder: NewRecordBuilder(header)}
	}
}

func (p *pipeWriter) Blank() Record {
	return p.builder(make([]string, len(p.pipe.header)))
}

func (p *pipeWriter) Close(err error) error {
	p.pipe.err = err
	close(p.pipe.ch)
	return nil
}

func (p *pipeWriter) Error() error {
	return p.pipe.err
}

func (p *pipeWriter) Header() []string {
	return p.pipe.header
}

func (p *pipeWriter) Write(r Record) error {
	p.pipe.ch <- r
	return nil
}

// A pipeline of processes.
type pipeline struct {
	stages []Process
}

// Join a sequence of processes by connecting them with pipes, returning a new process that
// represents the entire pipeline.
func NewPipeLine(p []Process) Process {
	if p == nil || len(p) == 0 {
		p = []Process{&CatProcess{}}
	}
	return &pipeline{
		stages: p,
	}
}

// Run the pipeline by connecting each stage with pipes and then running each stage
// as a goroutine.
func (p *pipeline) Run(r Reader, b WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {

		errors := make(chan error, len(p.stages))
		for _, c := range p.stages[:len(p.stages)-1] {
			p := NewPipe()
			go c.Run(r, p.Builder(), errors)
			r = p.Reader()
		}
		go p.stages[len(p.stages)-1].Run(r, b, errors)

		running := len(p.stages)

		for running > 0 {
			e := <-errors
			running--
			if err == nil {
				err = e
			}
		}

		return err
	}()
}
