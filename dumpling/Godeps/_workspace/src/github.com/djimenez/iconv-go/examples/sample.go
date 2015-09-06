package main

import (
	"encoding/hex"
	"fmt"
	iconv "github.com/djimenez/iconv-go"
	"io/ioutil"
	"os"
)

func main() {
	// read bytes from sample.utf8
	utf8Bytes, err := ioutil.ReadFile("sample.utf8")

	if err != nil {
		fmt.Println("Could not open 'sample.utf8': ", err)
	}

	// read bytes from sample.ebcdic-us
	ebcdicBytes, err := ioutil.ReadFile("sample.ebcdic-us")

	if err != nil {
		fmt.Println("Could not open 'sample.ebcdic-us': ", err)
	}

	// use iconv to check conversions both ways
	utf8String := string(utf8Bytes)
	ebcdicString := string(ebcdicBytes)

	// convert from utf-8 to ebcdic
	utf8ConvertedString, err := iconv.ConvertString(utf8String, "utf-8", "ebcdic-us")

	if err != nil || ebcdicString != utf8ConvertedString {
		// generate hex string
		ebcdicHexString := hex.EncodeToString(ebcdicBytes)
		utf8ConvertedHexString := hex.EncodeToString([]byte(utf8ConvertedString))

		fmt.Println("utf-8 was not properly converted to ebcdic-us by iconv.ConvertString, error: ", err)
		fmt.Println(ebcdicHexString, " - ", len(ebcdicString))
		fmt.Println(utf8ConvertedHexString, " - ", len(utf8ConvertedString))
	} else {
		fmt.Println("utf-8 was properly converted to ebcdic-us by iconv.ConvertString")
	}

	// convert from ebcdic to utf-8
	ebcdicConvertedString, err := iconv.ConvertString(ebcdicString, "ebcdic-us", "utf-8")

	if err != nil || utf8String != ebcdicConvertedString {
		// generate hex string
		utf8HexString := hex.EncodeToString(utf8Bytes)
		ebcdicConvertedHexString := hex.EncodeToString([]byte(ebcdicConvertedString))

		fmt.Println("ebcdic-us was not properly converted to utf-8 by iconv.ConvertString, error: ", err)
		fmt.Println(utf8HexString, " - ", len(utf8String))
		fmt.Println(ebcdicConvertedHexString, " - ", len(ebcdicConvertedString))
	} else {
		fmt.Println("ebcdic-us was properly converted to utf-8 by iconv.ConvertString")
	}

	testBuffer := make([]byte, len(ebcdicBytes)*2)

	// convert from ebdic bytes to utf-8 bytes
	bytesRead, bytesWritten, err := iconv.Convert(ebcdicBytes, testBuffer, "ebcdic-us", "utf-8")

	if err != nil || bytesRead != len(ebcdicBytes) || bytesWritten != len(utf8Bytes) {
		fmt.Println("ebcdic-us was not properly converted to utf-8 by iconv.Convert, error: ", err)
	} else {
		fmt.Println("ebcdic-us was properly converted to utf-8 by iconv.Convert")
	}

	// convert from utf-8 bytes to ebcdic bytes
	bytesRead, bytesWritten, err = iconv.Convert(utf8Bytes, testBuffer, "utf-8", "ebcdic-us")

	if err != nil || bytesRead != len(utf8Bytes) || bytesWritten != len(ebcdicBytes) {
		fmt.Println("utf-8 was not properly converted to ebcdic-us by iconv.Convert, error: ", err)
	} else {
		fmt.Println("utf-8 was properly converted to ebcdic-us by iconv.Convert")
	}

	// test iconv.Reader
	utf8File, _ := os.Open("sample.utf8")
	utf8Reader, _ := iconv.NewReader(utf8File, "utf-8", "ebcdic-us")
	bytesRead, err = utf8Reader.Read(testBuffer)

	if err != nil || bytesRead != len(ebcdicBytes) {
		fmt.Println("utf8 was not properly converted to ebcdic-us by iconv.Reader", err)
	} else {
		fmt.Println("utf8 was property converted to ebcdic-us by iconv.Reader")
	}

	ebcdicFile, _ := os.Open("sample.ebcdic-us")
	ebcdicReader, _ := iconv.NewReader(ebcdicFile, "ebcdic-us", "utf-8")
	bytesRead, err = ebcdicReader.Read(testBuffer)

	if err != nil || bytesRead != len(utf8Bytes) {
		fmt.Println("ebcdic-us was not properly converted to utf-8 by iconv.Reader: ", err)

		if bytesRead > 0 {
			fmt.Println(string(testBuffer[:bytesRead]))
			fmt.Println(hex.EncodeToString(testBuffer[:bytesRead]))
			fmt.Println(hex.EncodeToString(utf8Bytes))
		}
	} else {
		fmt.Println("ebcdic-us was properly converted to utf-8 by iconv.Reader")
	}
}
