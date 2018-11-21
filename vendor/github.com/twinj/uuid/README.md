Go UUID implementation
========================

[![license](http://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/myesui/uuid/master/LICENSE)
[![GoDoc](http://godoc.org/github.com/myesui/uuid?status.png)](http://godoc.org/github.com/myesui/uuid)
[![Build Status](https://ci.appveyor.com/api/projects/status/github/myesui/uuid?branch=master&svg=true)](https://ci.appveyor.com/project/myesui/uuid)
[![Build Status](https://travis-ci.org/myesui/uuid.png?branch=master)](https://travis-ci.org/myesui/uuid)
[![Coverage Status](https://coveralls.io/repos/github/myesui/uuid/badge.svg?branch=master)](https://coveralls.io/github/myesui/uuid?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/myesui/uuid)](https://goreportcard.com/report/github.com/myesui/uuid)

This package provides RFC 4122 and DCE 1.1 compliant UUIDs.
It will generate the following:

* Version 1: based on a Timestamp and MAC address as Node id
* Version 2: based on DCE Security - Experimental
* Version 3: based on MD5 hash
* Version 4: based on cryptographically secure random numbers
* Version 5: based on SHA-1 hash

Functions NewV1, NewV2, NewV3, NewV4, NewV5, New, NewHex and Parse() for
generating version 1, 2, 3, 4 and 5 Uuid's

# Requirements

Will generally support last 3 versions of Go.

 - 1.8
 - 1.7
 - 1.6

## Installation

Use the `go` tool:

	$ go get gopkg.in/myesui/uuid.v1
	

See [gopkg.in](http://labix.org/gopkg.in)

# Typical Usage

See [documentation and examples](http://godoc.org/github.com/myesui/uuid)
for more information.

## All UUIDs

    import "gopkg.in/myesui/uuid.v1"

    id, _ := uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

    if uuid.Equal(id, uuid.NameSpaceDNS) {
        fmt.Println("Alas these are equal")
    }

    if uuid.Compare(id, uuid.NameSpaceDNS) == 0 {
        fmt.Println("They are also equal")
    }

    if uuid.Compare(id, uuid.NameSpaceX500) == -1 {
        fmt.Println("id < uuid.NameSpaceX500")
    }

    if uuid.Compare(uuid.NameSpaceX50, id) == 1 {
        fmt.Println("uuid.NameSpaceX500 > id")
    }

    // Default Format is FormatCanonical
    fmt.Println(uuid.Formatter(id, uuid.FormatCanonicalCurly))

    uuid.SwitchFormat(uuid.FormatCanonicalBracket)

## Formatting UUIDs

    The default format is uuid.FormatCanonical xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx
    
    Any call to uuid.String() will produce this output.
    
    The format is twice as fast as the others at producing a string from the bytes.
    
    To change to another format permanently use:
   
    uuid.SwitchFormat(uuid.Format*) 
    uuid.SwitchFormatToUpper(uuid.Format*) 
    
    Once this has been called in an init function all UUID.String() calls will use the new format.
    
    Available formats:
    
    FormatHex              = xxxxxxxxxxxxxxxxxxxxxxxxxx
    FormatHexCurly         = {xxxxxxxxxxxxxxxxxxxxxxxxxx}
    FormatHexBracket       = (xxxxxxxxxxxxxxxxxxxxxxxxxx)
    
    // This is the default format.
    FormatCanonical Format = xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx
    
    FormatCanonicalCurly   = {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx}
    FormatCanonicalBracket = (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx)
    FormatUrn              = urn:uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxx
    
    The uuid.Formatter function also exists and is only ever meant to be used for one time prints where a different format from the default or switched format is required. You can supply your own format as long as it fits the pattern.
    which contains %x for the five groups in an UUID. Eg: FormatCanonical = %x-%x-%x-%x-%x
    
    You can also stwict to a custom format
    
    Note: AT this time cutsom formats are not supported for TextMarshalling. If a custom format is deteced it will use the canonical format. Use a call to String() and save as a string instead. 

## Version 1 and 2 UUIDs

    import "gopkg.in/myesui/uuid.v1"

    id := uuid.NewV1()
    fmt.Println(id)
    fmt.Printf("version %s variant %x: %s\n", u1.Version(), u1.Variant(), id)

    id = uuid.NewV2(uuid.DomainUser)
    fmt.Println(id)
    fmt.Printf("version %s variant %x: %s\n", u1.Version(), u1.Variant(), id)

    ids := uuid.BulkV1(500)
    for _, v := range ids {
    	fmt.Println(v)
    }
    
    ids = make([]UUID, 100)
    ReadV1(ids)
    for _, v := range ids {
        fmt.Println(v)
    }

    // If you wish to register a saving mechanism to keep track of your UUIDs over restarts
    // It is recommeneded to add a Saver so as to reduce risk in UUID collisions
    saver := savers.FileSystemSaver.Init()

    // Must be called before any V1 or V2 UUIDs. Do not call other uuid.Register* if
    // registering a Saver
    uuid.RegisterSaver(saver)

## Version 3 and 5 UUIDs

    import "gopkg.in/myesui/uuid.v1"

    id := uuid.NewV3(uuid.NameSpaceURL, uuid.Name("www.example.com"))
    fmt.Println(id)
    fmt.Printf("version %s variant %x: %s\n", u1.Version(), u1.Variant(), id)

    id := uuid.NewV5(uuid.NameSpaceURL, uuid.Name("www.example.com"))
    fmt.Println(id)
    fmt.Printf("version %s variant %x: %s\n", u1.Version(), u1.Variant(), id)

    id = uuid.NewV5(uuid.NameSpaceURL, id)
    fmt.Println(id)
    fmt.Printf("version %s variant %x: %s\n", u1.Version(), u1.Variant(), id)

## Version 4 UUIDs

    import "gopkg.in/myesui/uuid.v1"

    // A V4 UUID will panic by default if the systems CPRNG fails - this can
    // be changed by registering your own generator
    u4 := uuid.NewV4()
    fmt.Println(id)
    fmt.Printf("version %d variant %x: %s\n", u4.Version(), u4.Variant(), u4)
    
    ids := uuid.BulkV4(500)
    for _, v := range ids {
        fmt.Println(v)
    }
    
    ids := make([]UUID, 100)
    ReadV4(ids)
    for _, v := range ids {
        fmt.Println(v)
    }

## Custom Generators

    import "gopkg.in/myesui/uuid.v1"

    // Improve resolution for V1 and 2 UUIDs
    // The resolution correlates to how many ids can be created before waiting
    // for the next unique timestamp. The default is a low 1024, this equates
    // to Ids that can be created in 100 nanoseconds. It is low to encourage
    // you to set it.
    uuid.RegisterGenerator(&GeneratorConfig{Resolution: 18465})

    // Provide your own node Id or MAC address
    uuid.RegisterGenerator(&GeneratorConfig{
        Id: func() uuid.Node{
            // My Node Id
            // If this returns nil a random one will be generated
        },
    })

    // Replace the default Timestamp spinner with your own.
    uuid.RegisterGenerator(&GeneratorConfig{
        Next: func()(uuid.Timestamp){
            // My own Timestamp function...
            // Resolution will become reduendant if you set this.
            // The package will increment the clock sequence if you produce equal Timestamps
        },
    })

    // Replace the default crypto/rand.Read CPRNG with your own.
    uuid.RegisterGenerator(&GeneratorConfig{
        Random: func([]byte)(int, error){
            // My CPRNG function...
        },
    })
    
    // type HandleRandomError func([]byte, int, error) error

    // Replace the default random number error handler for V4 UUIDs. This function is called
    // when there is an error in the crypto/rand CPRNG. The default error handler function reads 
    // from math.Rand as a fallback.
    // 
    // You can change that behaviour and handle the error by providing your own function.
    // 
    // Errors could be due to a lack of system entropy or some other serious issue. These issues are rare,
    // however, having the tools to handle such issues is important.
    // This approach was taken as each user of this package will want to handle this differently.
    // 
    // For example one user of the package might want to just panic instead. 
    //  Returning an error will cause a panic.
    
    uuid.RegisterGenerator(&GeneratorConfig{
        HandleError: func(id []byte, n int, err error)bool{
            return err
        },
    })
    
    // You can also just generate your own completely.
    myGenerator := NewGenerator(nil)
    
    id := myGenerator.NewV4()
    
    // You can replace the logger
    uuid.RegisterGenerator(&GeneratorConfig{
            Logger: log.New(someWriter, "my-prefix", myFlags),
        })
    

## Coverage

* go test -coverprofile cover.out github.com/myesui/uuid
* go test -coverprofile cover.out github.com/myesui/uuid/savers

* go tool cover -html=cover.out -o cover.html

## Contribution 

1. fork from the *master* branch to create your own fork
2. clone from *master* into $GOPATH/src/github.com/myesui/uuid
3. git remote add `username` https://github.com/username/uuid.git
4. push changes on your fork and track your remote
5. Remember to create a branch

To ensure you get the correct packages and subpackages install in a gopath which matches *go/src/github.com/myesui/uuid*

## Links

* [RFC 4122](http://www.ietf.org/rfc/rfc4122.txt)
* [DCE 1.1: Authentication and Security Services](http://pubs.opengroup.org/onlinepubs/9629399/apdxa.htm)

# Design considerations

* UUID is an interface which correlates to 

* V1 UUIDs are sequential. This can cause the Generator to work
more slowly compared to other implementations. It can however be manually tuned
to have performance that is on par. This is achieved by setting the Timestamp
Resolution. Benchmark tests have been provided to help determine the best
setting for your server

    Proper test coverage has determined thant the UUID timestamp spinner works
    correctly, across multiple clock resolutions. The generator produces
    timestamps that roll out sequentially and will only modify the clock
    sequence on very rare circumstances.

    It is highly recommended that you register a uuid.Saver if you use V1 or V2
    UUIDs as it will ensure a higher probability of uniqueness.

        Example V1 output:
        5fb1a280-30f0-11e6-9614-005056c00001
        5fb1a281-30f0-11e6-9614-005056c00001
        5fb1a282-30f0-11e6-9614-005056c00001
        5fb1a283-30f0-11e6-9614-005056c00001
        5fb1a284-30f0-11e6-9614-005056c00001
        5fb1a285-30f0-11e6-9614-005056c00001
        5fb1a286-30f0-11e6-9614-005056c00001
        5fb1a287-30f0-11e6-9614-005056c00001
        5fb1a288-30f0-11e6-9614-005056c00001
        5fb1a289-30f0-11e6-9614-005056c00001
        5fb1a28a-30f0-11e6-9614-005056c00001
        5fb1a28b-30f0-11e6-9614-005056c00001
        5fb1a28c-30f0-11e6-9614-005056c00001
        5fb1a28d-30f0-11e6-9614-005056c00001
        5fb1a28e-30f0-11e6-9614-005056c00001
        5fb1a28f-30f0-11e6-9614-005056c00001
        5fb1a290-30f0-11e6-9614-005056c00001

* The V1 UUID generator should be file system and server agnostic
    To achieve this there are:
        ** No Os locking threads or file system dependant storage 
        ** Provided the uuid.Saver interface so a package can implement its own solution if required
* The V4 UUID should allow a package to handle any error that can occur in the CPRNG. The default is to read from math.Rand``````.
* The package should be able to handle multiple instances of Generators so a package can produce UUIDs from multiple sources.

## Copyright

Copyright (C) 2017 myesui@github.com
See [LICENSE](https://github.com/myesui/uuid/tree/master/LICENSE) file for details.
