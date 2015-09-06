package iconv

import "io"

type Writer struct {
	destination       io.Writer
	converter         *Converter
	buffer            []byte
	readPos, writePos int
	err               error
}

func NewWriter(destination io.Writer, fromEncoding string, toEncoding string) (*Writer, error) {
	// create a converter
	converter, err := NewConverter(fromEncoding, toEncoding)

	if err == nil {
		return NewWriterFromConverter(destination, converter), err
	}

	// return the error
	return nil, err
}

func NewWriterFromConverter(destination io.Writer, converter *Converter) (writer *Writer) {
	writer = new(Writer)

	// copy elements
	writer.destination = destination
	writer.converter = converter

	// create 8K buffers
	writer.buffer = make([]byte, 8*1024)

	return writer
}

func (this *Writer) emptyBuffer() {
	// write new data out of buffer
	bytesWritten, err := this.destination.Write(this.buffer[this.readPos:this.writePos])

	// update read position
	this.readPos += bytesWritten

	// slide existing data to beginning
	if this.readPos > 0 {
		// copy current bytes - is this guaranteed safe?
		copy(this.buffer, this.buffer[this.readPos:this.writePos])

		// adjust positions
		this.writePos -= this.readPos
		this.readPos = 0
	}

	// track any reader error / EOF
	if err != nil {
		this.err = err
	}
}

// implement the io.Writer interface
func (this *Writer) Write(p []byte) (n int, err error) {
	// write data into our internal buffer
	bytesRead, bytesWritten, err := this.converter.Convert(p, this.buffer[this.writePos:])

	// update bytes written for return
	n += bytesRead
	this.writePos += bytesWritten

	// checks for when we have a full buffer
	for this.writePos > 0 {
		// if we have an error, just return it
		if this.err != nil {
			return
		}

		// else empty the buffer
		this.emptyBuffer()
	}

	return n, err
}
