[![Build Status](https://travis-ci.org/cloudfoundry-incubator/candiedyaml.svg)](https://travis-ci.org/cloudfoundry-incubator/candiedyaml)
[![GoDoc](https://godoc.org/github.com/cloudfoundry-incubator/candiedyaml?status.svg)](https://godoc.org/github.com/cloudfoundry-incubator/candiedyaml)


candiedyaml
===========

YAML for Go

A YAML 1.1 parser with support for YAML 1.2 features

Usage
-----

```go
package myApp

import (
  "github.com/cloudfoundry-incubator/candiedyaml"
  "fmt"
  "os"
)

func main() {
  file, err := os.Open("path/to/some/file.yml")
  if err != nil {
    println("File does not exist:", err.Error())
    os.Exit(1)
  }
  defer file.Close()

  document := new(interface{})
  decoder := candiedyaml.NewDecoder(file)
  err = decoder.Decode(document)
  
  if err != nil {
    println("Failed to decode document:", err.Error())
  }
  
  println("parsed yml into interface:", fmt.Sprintf("%#v", document))
  
  fileToWrite, err := os.Create("path/to/some/new/file.yml")
  if err != nil {
    println("Failed to open file for writing:", err.Error())
    os.Exit(1)
  }
  defer fileToWrite.Close()

  encoder := candiedyaml.NewEncoder(fileToWrite)
  err = encoder.Encode(document)

  if err != nil {
    println("Failed to encode document:", err.Error())
    os.Exit(1)
  }
  
  return
}
```
