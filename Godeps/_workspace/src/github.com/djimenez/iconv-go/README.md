# Install

The main method of installation is through "go get" (provided in $GOROOT/bin)

	go get github.com/djimenez/iconv-go
	
This both downloads from github and installs the package into your $GOPATH. To just
recompile the package after you've already "get"ed (e.g. for a new go version), 
use "go install" instead.

	go install github.com/djimenez/iconv-go

See documentation for "go get" and "go install" for more information.

__PLEASE NOTE__ that this package requires the use of cgo, since it is only a wrapper around iconv - either provided by libiconv or glibc on your system. Attempts to build without cgo enabled will fail.

# Usage

To use the package, you'll need the appropriate import statement:

	import (
		iconv "github.com/djimenez/iconv-go"
	)

## Converting string Values 

Converting a string can be done with two methods. First, there's
iconv.ConvertString(input, fromEncoding, toEncoding string)

	output,_ := iconv.ConvertString("Hello World!", "utf-8", "windows-1252")

Alternatively, you can create a converter and use its ConvertString method.
Reuse of a Converter instance is recommended when doing many string conversions
between the same encodings.

	converter := iconv.NewConverter("utf-8", "windows-1252")
	output,_ := converter.ConvertString("Hello World!")
	
	// converter can then be closed explicitly
	// this will also happen when garbage collected
	converter.Close()

ConvertString may return errors for the following reasons:

 * EINVAL - when either the from or to encoding is not supported by iconv
 * EILSEQ - when the input string contains an invalid byte sequence for the
   given from encoding

## Converting []byte Values

Converting a []byte can similarly be done with two methods. First, there's
iconv.Convert(input, output []byte, fromEncoding, toEncoding string). You'll
immediately notice this requires you to give it both the input and output
buffer. Ideally, the output buffer should be sized so that it can hold all
converted bytes from input, but if it cannot, then Convert will put as many
bytes as it can into the buffer without creating an invalid sequence. For
example, if iconv only has a single byte left in the output buffer but needs 2
or more for the complete character in a multibyte encoding it will stop writing
to the buffer and return with an iconv.E2BIG error.

	in := []byte("Hello World!")
	out := make([]byte, len(input))
	
	bytesRead, bytesWritten, err := iconv.Convert(in, out, "utf-8", "latin1")

Just like with ConvertString, there is also a Convert method on Converter that
can be used.

	...
	converter := iconv.NewConverter("utf-8", "windows-1252")
	
	bytesRead, bytesWritten, error := converter.Convert(input, output)
	
Convert may return errors for the following reasons:

 * EINVAL - when either the from or to encoding is not supported by iconv
 * EILSEQ - when the input string contains an invalid byte sequence for the
   given from encoding
 * E2BIG - when the output buffer is not big enough to hold the full
   conversion of input
   
   Note on E2BIG: this is a common error value especially when converting to a
   multibyte encoding and should not be considered fatal. Partial conversion
   has probably occurred be sure to check bytesRead and bytesWritten.

### Note on Shift Based Encodings

When using iconv.Convert convenience method it will automatically try to append
to your output buffer with a nil input so that any end shift sequences are
appropiately written. Using a Converter.Convert method however will not
automatically do this since it can be used to process a full stream in chunks.
So you'll need to remember to pass a nil input buffer at the end yourself, just
like you would with direct iconv usage.

## Converting an \*io.Reader

The iconv.Reader allows any other \*io.Reader to be wrapped and have its bytes
transcoded as they are read. 

	// We're wrapping stdin for simplicity, but a File or network reader could
	// be wrapped as well
	reader,_ := iconv.NewReader(os.Stdin, "utf-8", "windows-1252")

## Converting an \*io.Writer

The iconv.Writer allows any other \*io.Writer to be wrapped and have its bytes
transcoded as they are written. 

	// We're wrapping stdout for simplicity, but a File or network reader could
	// be wrapped as well
	writer,_ := iconv.NewWriter(os.Stdout, "utf-8", "windows-1252")
