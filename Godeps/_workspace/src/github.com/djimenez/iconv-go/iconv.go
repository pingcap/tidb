/*
Wraps the iconv API present on most systems, which allows for conversion
of bytes from one encoding to another. This package additionally provides
some convenient interface implementations like a Reader and Writer.
*/
package iconv

// All in one Convert method, rather than requiring the construction of an iconv.Converter
func Convert(input []byte, output []byte, fromEncoding string, toEncoding string) (bytesRead int, bytesWritten int, err error) {
	// create a temporary converter
	converter, err := NewConverter(fromEncoding, toEncoding)

	if err == nil {
		// call converter's Convert
		bytesRead, bytesWritten, err = converter.Convert(input, output)

		if err == nil {
			var shiftBytesWritten int

			// call Convert with a nil input to generate any end shift sequences
			_, shiftBytesWritten, err = converter.Convert(nil, output[bytesWritten:])

			// add shift bytes to total bytes
			bytesWritten += shiftBytesWritten
		}

		// close the converter
		converter.Close()
	}

	return
}

// All in one ConvertString method, rather than requiring the construction of an iconv.Converter
func ConvertString(input string, fromEncoding string, toEncoding string) (output string, err error) {
	// create a temporary converter
	converter, err := NewConverter(fromEncoding, toEncoding)

	if err == nil {
		// convert the string
		output, err = converter.ConvertString(input)

		// close the converter
		converter.Close()
	}

	return
}
